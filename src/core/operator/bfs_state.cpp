#include "duckpgq/core/operator/bfs_state.hpp"

#include <duckpgq/core/operator/event/shortest_path_event.hpp>
#include <duckpgq/core/utils/compressed_sparse_row.hpp>
#include <duckpgq/core/utils/duckpgq_barrier.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {

BFSState::BFSState(shared_ptr<DataChunk> pairs_, CSR *csr_, idx_t num_threads_,
                   string mode_, ClientContext &context_)
    : pairs(std::move(pairs_)), csr(csr_), v_size(csr_->vsize - 2),
      context(context_), num_threads(num_threads_), mode(std::move(mode_)),
      src_data(pairs->data[0]), dst_data(pairs->data[1]){
  LogicalType bfs_type = mode == "iterativelength"
                             ? LogicalType::BIGINT
                             : LogicalType::LIST(LogicalType::BIGINT);
  D_ASSERT(csr_ != nullptr);
  // Only have to initialize the current batch and state once.
  total_pairs_processed = 0; // Initialize the total pairs processed
  current_batch_path_list_len = 0;
  started_searches = 0; // reset
  active = 0;
  iter = 1;
  change = false;
  pf_results = make_shared_ptr<DataChunk>();
  pf_results->Initialize(context, {bfs_type});

  visit1 = vector<std::bitset<LANE_LIMIT>>(v_size);
  visit2 = vector<std::bitset<LANE_LIMIT>>(v_size);
  seen = vector<std::bitset<LANE_LIMIT>>(v_size);
  element_locks = vector<std::mutex>(v_size);
  parents_ve = std::vector<std::array<ve, LANE_LIMIT>>(
      v_size, std::array<ve, LANE_LIMIT>{});

  // Initialize source and destination vectors
  src_data.ToUnifiedFormat(pairs->size(), vdata_src);
  dst_data.ToUnifiedFormat(pairs->size(), vdata_dst);
  src = FlatVector::GetData<int64_t>(src_data);
  dst = FlatVector::GetData<int64_t>(dst_data);

  CreateTasks();
  scheduled_threads = std::min(num_threads, (idx_t)global_task_queue.size());
  barrier = make_uniq<Barrier>(scheduled_threads);
}

void BFSState::Clear() {
  iter = 1;
  active = 0;
  change = false;
  // empty visit vectors
  for (auto i = 0; i < v_size; i++) {
    visit1[i] = 0;
    visit2[i] = 0;
    seen[i] = 0; // reset
    if (mode == "shortestpath") {
      for (auto j = 0; j < LANE_LIMIT; j++) {
        parents_ve[i][j] = {-1, -1};
      }
    }
  }
}

void BFSState::CreateTasks() {
  // workerTasks[workerId] = [task1, task2, ...]
  // vector<vector<pair<idx_t, idx_t>>> worker_tasks(num_threads);
  // auto cur_worker = 0;
  int64_t *v = (int64_t *)csr->v;
  int64_t current_task_edges = 0;
  idx_t current_task_start = 0;
  for (idx_t v_idx = 0; v_idx < (idx_t)v_size; v_idx++) {
    auto number_of_edges = v[v_idx + 1] - v[v_idx];
    if (current_task_edges + number_of_edges > split_size) {
      global_task_queue.push_back({current_task_start, v_idx});
      current_task_start = v_idx;
      current_task_edges = 0; // reset
    }
    current_task_edges += number_of_edges;
  }

  // Final task if there are any remaining edges
  if (current_task_start < (idx_t)v_size) {
    global_task_queue.push_back({current_task_start, v_size});
  }
}


shared_ptr<std::pair<idx_t, idx_t>> BFSState::FetchTask() {
  std::unique_lock<std::mutex> lock(queue_mutex);  // Lock the mutex to access the queue

  // Avoid unnecessary waiting if no tasks are available
  if (current_task_index >= global_task_queue.size()) {
    // std::cout << "FetchTask: No more tasks available. Exiting." << std::endl;
    return nullptr;  // No more tasks
  }

  // Wait until a task is available or the queue is finalized
  queue_cv.wait(lock, [this]() {
    return current_task_index < global_task_queue.size();
  });

  // Fetch the next task and increment the task index
  if (current_task_index < global_task_queue.size()) {
    auto task = make_shared_ptr<std::pair<idx_t, idx_t>>(global_task_queue[current_task_index]);
    current_task_index++;

    return task;
  }
  return nullptr;
}

void BFSState::ResetTaskIndex() {
  std::lock_guard<std::mutex> lock(queue_mutex);  // Lock to reset index safely
  current_task_index = 0;  // Reset the task index for the next stage
  queue_cv.notify_all();  // Notify all threads that tasks are available
}

pair<idx_t, idx_t> BFSState::BoundaryCalculation(idx_t worker_id) const {
  idx_t block_size = ceil((double)v_size / num_threads);
  block_size = block_size == 0 ? 1 : block_size;
  idx_t left = block_size * worker_id;
  idx_t right = std::min(block_size * (worker_id + 1), (idx_t)v_size);
  return {left, right};
}

void BFSState::InitializeLanes() {
  auto &result_validity = FlatVector::Validity(pf_results->data[0]);
  std::bitset<LANE_LIMIT> seen_mask;
  seen_mask.set();

  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    lane_to_num[lane] = -1;
    while (started_searches < pairs->size()) {
      auto search_num = started_searches++;
      int64_t src_pos = vdata_src.sel->get_index(search_num);
      if (!vdata_src.validity.RowIsValid(src_pos)) {
        result_validity.SetInvalid(search_num);
      } else {
        visit1[src[src_pos]][lane] = true;
        // bfs_state->seen[bfs_state->src[src_pos]][lane] = true;
        lane_to_num[lane] = search_num; // active lane
        active++;
        seen_mask[lane] = false;
        break;
      }
    }
  }
  for (int64_t i = 0; i < v_size; i++) {
    seen[i] = seen_mask;
  }

}

void BFSState::ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op) {
  if (mode == "iterativelength") {
    throw NotImplementedException("Iterative length has not been implemented yet");
    // event.InsertEvent(
    // make_shared_ptr<ParallelIterativeEvent>(gstate, pipeline, *this));
  } else if (mode == "shortestpath") {
    event.InsertEvent(
      make_shared_ptr<ShortestPathEvent>(shared_from_this(), pipeline, *op));
  } else {
    throw NotImplementedException("Mode not supported");
  }
}


// void BFSState::InitializeBFS(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op_) {
//   path_finding_result = make_uniq<DataChunk>();
//   path_finding_result->Initialize(context, {LogicalType::LIST(LogicalType::BIGINT)});
//
//
//
//   // remaining pairs for current batch
//   while (started_searches < current_pairs_batch->size()) {
//     ScheduleBFSBatch(pipeline, event);
//     Clear();
//   }
//   if (started_searches != current_pairs_batch->size()) {
//     throw InternalException("Number of started searches does not match the number of pairs");
//   }
  // path_finding_result->SetCardinality(current_pairs_batch->size());
  // path_finding_result->Print();
  // current_pairs_batch->Fuse(*path_finding_result);
  // current_pairs_batch->Print();
  // results->Append(*current_pairs_batch);
  // total_pairs_processed += current_pairs_batch->size();
  // std::cout << "Total pairs processed: " << total_pairs_processed << std::endl;
  // if (total_pairs_processed < pairs->Count()) {
  //   InitializeBFS(pipeline, event, op);
  // }
// }

} // namespace core

} // namespace duckpgq