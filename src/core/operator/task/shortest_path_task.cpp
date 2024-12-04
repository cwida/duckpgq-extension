#include "duckpgq/core/operator/task/shortest_path_task.hpp"
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

ShortestPathTask::ShortestPathTask(shared_ptr<Event> event_p,
                                   ClientContext &context,
                                   shared_ptr<GlobalBFSState> &state, idx_t worker_id,
                                   const PhysicalOperator &op_p)
    : ExecutorTask(context, std::move(event_p), op_p), context(context),
      state(state), worker_id(worker_id) {
  left = right = UINT64_MAX; // NOLINT
}

TaskExecutionResult ShortestPathTask::ExecuteTask(TaskExecutionMode mode) {
  auto &barrier = state->barrier;

  do {
    IterativePath();

    // Synchronize after IterativePath
    barrier->Wait();
    if (worker_id == 0) {
      state->ResetTaskIndex();
    }
    barrier->Wait();

    if (worker_id == 0) {
      ReachDetect();
    }

    // std::cout << "Worker " << worker_id << ": Waiting at barrier before
    // ResetTaskIndex." << std::endl;
    barrier->Wait();
    if (worker_id == 0) {
      state->ResetTaskIndex();
      // std::cout << "Worker " << worker_id << ": ResetTaskIndex completed." <<
      // std::endl;
    }
    barrier->Wait();
    // std::cout << "Worker " << worker_id << ": Passed barrier after
    // ResetTaskIndex." << std::endl;
  } while (state->change);

  barrier->Wait();
  if (worker_id == 0) {
    // std::cout << "Worker " << worker_id << " started path construction" <<
    // std::endl;
    PathConstruction();
    std::cout << "Worker " << worker_id << " finished path construction" <<
    std::endl;
  }

  // Final synchronization before finishing
  barrier->Wait();
  std::cout << "Worker " << worker_id << " finishing task" << std::endl;
  event->FinishTask();
  return TaskExecutionResult::TASK_FINISHED;
}

bool ShortestPathTask::SetTaskRange() {
  auto task = state->FetchTask();
  if (task == nullptr) {
    return false;
  }
  left = task->first;
  right = task->second;
  return true;
}

void ShortestPathTask::IterativePath() {
  auto &seen = state->seen;
  auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
  auto &next = state->iter & 1 ? state->visit2 : state->visit1;
  auto &barrier = state->barrier;
  int64_t *v = (int64_t *)state->csr->v;
  vector<int64_t> &e = state->csr->e;
  auto &edge_ids = state->csr->edge_ids;
  auto &parents_ve = state->parents_ve;
  auto &change = state->change;

  // Attempt to get a task range
  bool has_tasks = SetTaskRange();
  for (auto i = left; i < right; i++) {
    next[i] = 0;
  }

  // Synchronize after clearing
  barrier->Wait();

  // Main processing loop
  while (has_tasks) {
    for (auto i = left; i < right; i++) {
      if (visit[i].any()) {
        for (auto offset = v[i]; offset < v[i + 1]; offset++) {
          auto n = e[offset];
          auto edge_id = edge_ids[offset];
          {
            std::lock_guard<std::mutex> lock(state->element_locks[n]);
            next[n] |= visit[i];
          }
          for (auto l = 0; l < LANE_LIMIT; l++) {
            if (parents_ve[n][l].GetV() == -1 && visit[i][l]) {
              parents_ve[n][l] = {static_cast<int64_t>(i), edge_id};
            }
          }
        }
      }
    }

    // Check for a new task range
    has_tasks = SetTaskRange();
  }

  // Synchronize at the end of the main processing
  barrier->Wait([&]() {
    state->ResetTaskIndex();
  });
  barrier->Wait();

  // Second processing stage (if needed)
  has_tasks = SetTaskRange();
  change = false;
  while (has_tasks) {
    for (auto i = left; i < right; i++) {
      if (next[i].any()) {
        next[i] &= ~seen[i];
        seen[i] |= next[i];
        change |= next[i].any();
      }
    }
    has_tasks = SetTaskRange();
  }

  // Synchronize again
  barrier->Wait([&]() {
    state->ResetTaskIndex();
  });
  barrier->Wait();
}

void ShortestPathTask::ReachDetect() {
  // detect lanes that finished
  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    int64_t search_num = state->lane_to_num[lane];
    if (search_num >= 0) { // active lane
      //! Check if dst for a source has been seen
      int64_t dst_pos = state->vdata_dst.sel->get_index(search_num);
      if (state->seen[state->dst[dst_pos]][lane]) {
        state->active--;
      }
    }
  }
  if (state->active == 0) {
    state->change = false;
  }
  // into the next iteration
  state->iter++;
}

void ShortestPathTask::PathConstruction() {
  auto path_finding_result = make_uniq<DataChunk>();
  path_finding_result->Initialize(context, {LogicalType::LIST(LogicalType::BIGINT)});
  auto result_data = FlatVector::GetData<list_entry_t>(path_finding_result->data[0]);
  auto &result_validity = FlatVector::Validity(path_finding_result->data[0]);
  size_t list_len = 0;
  //! Reconstruct the paths
  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    int64_t search_num = state->lane_to_num[lane];
    if (search_num == -1) { // empty lanes
      continue;
    }

    //! Searches that have stopped have found a path
    int64_t src_pos = state->vdata_src.sel->get_index(search_num);
    int64_t dst_pos = state->vdata_dst.sel->get_index(search_num);
    if (state->src[src_pos] == state->dst[dst_pos]) { // Source == destination
      unique_ptr<Vector> output =
          make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
      ListVector::PushBack(*output, state->src[src_pos]);
      ListVector::Append(path_finding_result->data[0], ListVector::GetEntry(*output),
                         ListVector::GetListSize(*output));
      result_data[search_num].length = ListVector::GetListSize(*output);
      result_data[search_num].offset = state->total_len;
      list_len += result_data[search_num].length;
      continue;
    }
    std::vector<int64_t> output_vector;
    std::vector<int64_t> output_edge;
    auto source_v = state->src[src_pos]; // Take the source

    auto parent_vertex = state->parents_ve[state->dst[dst_pos]][lane].GetV();
    auto parent_edge = state->parents_ve[state->dst[dst_pos]][lane].GetE();

    output_vector.push_back(state->dst[dst_pos]); // Add destination vertex
    output_vector.push_back(parent_edge);
    while (parent_vertex != source_v) { // Continue adding vertices until we
                                        // have reached the source vertex
      //! -1 is used to signify no parent
      if (parent_vertex == -1 ||
          parent_vertex == state->parents_ve[parent_vertex][lane].GetV()) {
        result_validity.SetInvalid(search_num);
        break;
      }
      output_vector.push_back(parent_vertex);
      parent_edge = state->parents_ve[parent_vertex][lane].GetE();
      parent_vertex = state->parents_ve[parent_vertex][lane].GetV();
      output_vector.push_back(parent_edge);
    }

    if (!result_validity.RowIsValid(search_num)) {
      continue;
    }
    output_vector.push_back(source_v);
    std::reverse(output_vector.begin(), output_vector.end());
    auto output = make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
    for (auto val : output_vector) {
      Value value_to_insert = val;
      ListVector::PushBack(*output, value_to_insert);
    }

    result_data[search_num].length = ListVector::GetListSize(*output);
    result_data[search_num].offset = list_len;
    ListVector::Append(path_finding_result->data[0], ListVector::GetEntry(*output),
                       ListVector::GetListSize(*output));
    list_len += result_data[search_num].length;
  }
  path_finding_result->SetCardinality(state->current_pairs_batch->size());

  state->current_pairs_batch->Fuse(*path_finding_result);
  // state->current_pairs_batch->Print();
  state->results->Append(*state->current_pairs_batch);
  // state->results->Print();
}

} // namespace core
} // namespace duckpgq