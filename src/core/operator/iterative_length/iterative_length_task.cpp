#include "duckpgq/core/operator/iterative_length/iterative_length_task.hpp"
#include <duckpgq/core/operator/iterative_length/iterative_length_state.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckdb/parallel/event.hpp>


namespace duckpgq {
namespace core {

IterativeLengthTask::IterativeLengthTask(shared_ptr<Event> event_p,
                                   ClientContext &context,
                                   shared_ptr<IterativeLengthState> &state, idx_t worker_id,
                                   const PhysicalOperator &op_p)
: ExecutorTask(context, std::move(event_p), op_p), context(context),
  state(state), worker_id(worker_id) {
  explore_done = false;
}

void IterativeLengthTask::CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen,
                                      std::vector<std::bitset<LANE_LIMIT>> &next,
                                      std::pair<idx_t, idx_t> &partition_range) const {
  for (auto i = partition_range.first; i < partition_range.second; i++) {
    if (next[i].any()) {
      next[i] &= ~seen[i];
      seen[i] |= next[i];
      if (!state->change && next[i].any()) {
        state->change = true;
      }
    }
  }
  // Printer::PrintF("%d finished partition %d to %d, state now: %d\n", worker_id, partition_range.first, partition_range.second, state->change);
}


TaskExecutionResult IterativeLengthTask::ExecuteTask(TaskExecutionMode mode) {
  auto &barrier = state->barrier;
  // Printer::PrintF("CSR Sizes - (worker %d): vsize: %d esize: %d", worker_id, local_csr->vsize, local_csr->e.size());
  while (state->started_searches < state->pairs->size()) {
    barrier->Wait(worker_id);

    // Printer::PrintF("worker %d\n%s", worker_id, local_csr->ToString());
    if (worker_id == 0) {
      // barrier->LogMessage(worker_id, "Initializing lanes");
      state->InitializeLanes();
      // Printer::Print("Finished intialize lanes");
    }
    barrier->Wait(worker_id);
    do {
      IterativeLength();
      // if (worker_id == 0) {
        // barrier->LogMessage(worker_id, "Finished IterativeLength");
      // }
      barrier->Wait(worker_id);
      if (worker_id == 0) {
        ReachDetect();
        // barrier->LogMessage(worker_id, "Finished ReachDetect");
      }
      barrier->Wait(worker_id);
    } while (state->change);
    if (worker_id == 0) {
      UnReachableSet();
      // barrier->LogMessage(worker_id, "Finished UnreachableSet");
    }

    // Final synchronization before finishing
    barrier->Wait(worker_id);
    if (worker_id == 0) {
      state->Clear();
      // barrier->LogMessage(worker_id, "Cleared state");
    }
    barrier->Wait(worker_id);
  }

  event->FinishTask();
  return TaskExecutionResult::TASK_FINISHED;
}

double IterativeLengthTask::Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                  std::vector<std::bitset<LANE_LIMIT>> &next,
                                  const std::vector<uint64_t> &v, const std::vector<uint16_t> &e, size_t v_size, idx_t v_offset) {
  auto start_time = std::chrono::high_resolution_clock::now();
  for (auto i = 0; i < v_size; i++) {
    if (visit[i].any()) {
      auto start_edges = v[i];
      auto end_edges = v[i+1];
      for (auto offset = start_edges; offset < end_edges; offset++) {
        auto n = e[offset] + v_offset; // Use the local edge index directly
        next[n] |= visit[i]; // Propagate the visit bitset
      }
    }
  }
  // Capture end time
  auto end_time = std::chrono::high_resolution_clock::now();
  return std::chrono::duration<double, std::milli>(end_time - start_time).count(); // Return time in ms
}

void PrintMatrix(const std::string &label, const std::vector<std::bitset<LANE_LIMIT>> &matrix) {
  std::cout << label << ":\n";
  for (size_t i = 0; i < matrix.size(); i++) {
    std::cout << "Row " << i << ": ";
    for (size_t j = 0; j < matrix[i].size(); j++) {
      std::cout << matrix[i][j] << " ";
    }
    std::cout << "\n";
  }
  std::cout << std::endl;
}

// Wrapper function to call Explore and log data
void IterativeLengthTask::RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                std::vector<std::bitset<LANE_LIMIT>> &next,
                const std::vector<uint64_t> &v, const std::vector<uint16_t> &e, size_t v_size, idx_t v_offset) {
  double duration_ms = Explore(visit, next, v, e, v_size, v_offset);

  // Get thread & core info *outside* Explore to reduce per-call overhead
  std::thread::id thread_id = std::this_thread::get_id();
  int core_id = -1; // Default if not available
#ifdef __linux__
  core_id = sched_getcpu();
#elif defined(__APPLE__)
  uint64_t tid;
  pthread_threadid_np(NULL, &tid);
  core_id = static_cast<int>(tid % std::thread::hardware_concurrency()); // Approximate core ID mapping
#endif

  // Store result safely
  {
    std::lock_guard<std::mutex> guard(state->log_mutex);
    state->timing_data.emplace_back(thread_id, core_id, duration_ms, state->num_threads);
  }
}

void IterativeLengthTask::IterativeLength() {
    auto &seen = state->seen;
    const auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
    auto &next = state->iter & 1 ? state->visit2 : state->visit1;
    auto &barrier = state->barrier;
    // Clear `next` array
    while (state->partition_counter < state->partition_ranges.size()) {
      state->local_csr_lock.lock();
      if (state->partition_counter >= state->partition_ranges.size()) {
        state->local_csr_lock.unlock();
        break;
      }
      auto partition_range = state->partition_ranges[state->partition_counter++];
      state->local_csr_lock.unlock();
      for (auto i = partition_range.first; i < partition_range.second; i++) {
        next[i] = 0;
      }
    }
    barrier->Wait(worker_id);
    state->partition_counter = 0;
    state->local_csr_counter = 0;
    static std::atomic<int> finished_threads(0);
    barrier->Wait(worker_id);
    while (state->local_csr_counter < state->local_csrs.size()) {
      state->local_csr_lock.lock();
      if (state->local_csr_counter >= state->local_csrs.size()) {
        state->local_csr_lock.unlock();
        break;
      }
      auto local_csr = state->local_csrs[state->local_csr_counter++].get();
      if (!local_csr) {
        throw InternalException("Tried to reference nullptr for LocalCSR");
      }
      // Printer::PrintF("CSR counter: %d, max size %d\n", state->local_csr_counter.load(), state->local_csrs.size());
      state->local_csr_lock.unlock();
      RunExplore(visit, next, local_csr->v, local_csr->e, local_csr->GetVertexSize(), local_csr->start_offset);
    }
    state->change = false;
    // Mark this thread as finished
    finished_threads.fetch_add(1);
    // Last thread reaching here should reset the counter for the next iteration
    if (finished_threads.load() == state->num_threads) {
      finished_threads.store(0); // Reset for the next phase
    }

    // Printer::PrintF("worker %d finished all partitions\n", worker_id);
    barrier->Wait(worker_id);
    // Printer::PrintF("partition counter: %d\n", state->partition_counter.load());
    while (state->partition_counter < state->partition_ranges.size()) {
      state->local_csr_lock.lock();
      if (state->partition_counter < state->partition_ranges.size()) {
        auto partition_range = state->partition_ranges[state->partition_counter++];
        state->local_csr_lock.unlock();
        CheckChange(seen, next, partition_range);
      } else {
        state->local_csr_lock.unlock();
        break; // Avoids reading invalid memory
      }
    }
    barrier->Wait(worker_id);
    state->partition_counter = 0;
    // Printer::PrintF("Finished iteration: %d", state->iter);
}

void IterativeLengthTask::ReachDetect() const {
  auto result_data = FlatVector::GetData<int64_t>(state->pf_results->data[0]);

  // detect lanes that finished
  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    int64_t search_num = state->lane_to_num[lane];
    if (search_num >= 0) { // active lane
      int64_t dst_pos = state->vdata_dst.sel->get_index(search_num);
      if (state->seen[state->dst[dst_pos]][lane]) {
        result_data[search_num] =
            state->iter; /* found at iter => iter = path length */
        state->lane_to_num[lane] = -1; // mark inactive
        state->active--;
      }
    }
  }
  if (state->active == 0) {
    state->change = false;
  }
  // into the next iteration
  state->iter++;
  // Printer::PrintF("Starting next iteration: %d %d\n", worker_id, state->iter);
}

void IterativeLengthTask::UnReachableSet() const {
  auto result_data = FlatVector::GetData<int64_t>(state->pf_results->data[0]);
  auto &result_validity = FlatVector::Validity(state->pf_results->data[0]);

  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    int64_t search_num = state->lane_to_num[lane];
    if (search_num >= 0) { // active lane
      result_validity.SetInvalid(search_num);
      result_data[search_num] = (int64_t)-1; /* no path */
      state->lane_to_num[lane] = -1;     // mark inactive
    }
  }
}

} // namespace core
} // namespace duckpgq