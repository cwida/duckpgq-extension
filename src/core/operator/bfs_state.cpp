#include "duckpgq/core/operator/bfs_state.hpp"

#include <duckpgq/core/utils/compressed_sparse_row.hpp>
#include <duckpgq/core/utils/duckpgq_barrier.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {

GlobalBFSState::GlobalBFSState(unique_ptr<ColumnDataCollection> &pairs_, CSR* csr_, int64_t vsize_,
               idx_t num_threads_, string mode_, ClientContext &context_)
    : pairs(pairs_), iter(1), csr(csr_), v_size(vsize_), change(false),
      started_searches(0), context(context_), seen(vsize_),
      visit1(vsize_), visit2(vsize_), num_threads(num_threads_),
      element_locks(vsize_),
      mode(std::move(mode_)), parents_ve(vsize_) {
  auto types = pairs->Types();
  if (mode == "iterativelength") {
    types.push_back(LogicalType::BIGINT);
  } else if (mode == "shortestpath") {
    types.push_back(LogicalType::LIST(LogicalType::BIGINT));
  } else {
    throw NotImplementedException("Mode not supported");
  }

  results = make_uniq<ColumnDataCollection>(Allocator::Get(context), types);
  results->InitializeScan(result_scan_state);

  // Only have to initialize the current batch and state once.
  pairs->InitializeScan(input_scan_state);
  total_pairs_processed = 0; // Initialize the total pairs processed


  CreateTasks();
  scheduled_threads = std::min(num_threads, (idx_t)global_task_queue.size());
  barrier = make_uniq<Barrier>(scheduled_threads);
}

void GlobalBFSState::Clear() {
  iter = 1;
  active = 0;
  change = false;
  // empty visit vectors
  for (auto i = 0; i < v_size; i++) {
    visit1[i] = 0;
    if (mode == "shortestpath") {
      for (auto j = 0; j < LANE_LIMIT; j++) {
        parents_ve[i][j] = {-1, -1};
      }
    }
  }
}

void GlobalBFSState::CreateTasks() {
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
  // std::cout << "Set the number of tasks to " << global_task_queue.size() << std::endl;
}


shared_ptr<std::pair<idx_t, idx_t>> GlobalBFSState::FetchTask() {
  std::unique_lock<std::mutex> lock(queue_mutex);  // Lock the mutex to access the queue

  // Log entry into FetchTask
  // std::cout << "FetchTask: Checking tasks. Current index: " << current_task_index
  //           << ", Total tasks: " << global_task_queue.size() << std::endl;

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

    // Log the fetched task
    // std::cout << "FetchTask: Fetched task " << current_task_index - 1
              // << " -> [" << task->first << ", " << task->second << "]" << std::endl;

    return task;
  }

  // Log no tasks available after wait
  // std::cout << "FetchTask: No more tasks available after wait. Exiting." << std::endl;
  return nullptr;
}

void GlobalBFSState::ResetTaskIndex() {
  std::lock_guard<std::mutex> lock(queue_mutex);  // Lock to reset index safely
  current_task_index = 0;  // Reset the task index for the next stage
  queue_cv.notify_all();  // Notify all threads that tasks are available
}

pair<idx_t, idx_t> GlobalBFSState::BoundaryCalculation(idx_t worker_id) const {
  idx_t block_size = ceil((double)v_size / num_threads);
  block_size = block_size == 0 ? 1 : block_size;
  idx_t left = block_size * worker_id;
  idx_t right = std::min(block_size * (worker_id + 1), (idx_t)v_size);
  return {left, right};
}

} // namespace core

} // namespace duckpgq