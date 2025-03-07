#include "duckpgq/core/operator/iterative_length/iterative_length_task.hpp"
#include <duckpgq/core/operator/iterative_length/iterative_length_state.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

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

template<typename T, typename E>
void IterativeLengthTask::Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                  std::vector<std::bitset<LANE_LIMIT>> &next,
                                  const std::vector<T> &v, const std::vector<E> &e, idx_t v_size) {
  for (auto i = 0; i < v_size; i++) {
    if (visit[i].any()) {
      auto start_edges = v[i];
      auto end_edges = v[i+1];
      for (auto offset = start_edges; offset < end_edges; offset++) {
        auto n = e[offset]; // Use the local edge index directly
        next[n] |= visit[i]; // Propagate the visit bitset
      }
    }
  }
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
      // Printer::PrintF("CSR counter: %d, max size %d\n", state->local_csr_counter.load(), state->local_csrs.size());
      state->local_csr_lock.unlock();
      auto v_type = local_csr->v_type;
      auto e_type = local_csr->e_type;
      auto v_size = local_csr->GetVertexSize();
      if (v_type == 16 && e_type == 16) {
        Explore<int16_t, int16_t>(visit, next, local_csr->GetVertexVectorTyped<int16_t>(), local_csr->GetEdgeVectorTyped<int16_t>(), v_size);
      } else if (v_type == 16 && e_type == 32) {
        Explore<int16_t, int32_t>(visit, next, local_csr->GetVertexVectorTyped<int16_t>(), local_csr->GetEdgeVectorTyped<int32_t>(), v_size);
      } else if (v_type == 16 && e_type == 64) {
        Explore<int16_t, int64_t>(visit, next, local_csr->GetVertexVectorTyped<int16_t>(), local_csr->GetEdgeVectorTyped<int64_t>(), v_size);
      } else if (v_type == 32 && e_type == 16) {
        Explore<int32_t, int16_t>(visit, next, local_csr->GetVertexVectorTyped<int32_t>(), local_csr->GetEdgeVectorTyped<int16_t>(), v_size);
      } else if (v_type == 32 && e_type == 32) {
        Explore<int32_t, int32_t>(visit, next, local_csr->GetVertexVectorTyped<int32_t>(), local_csr->GetEdgeVectorTyped<int32_t>(), v_size);
      } else if (v_type == 32 && e_type == 64) {
        Explore<int32_t, int64_t>(visit, next, local_csr->GetVertexVectorTyped<int32_t>(), local_csr->GetEdgeVectorTyped<int64_t>(), v_size);
      } else if (v_type == 64 && e_type == 16) {
        Explore<int64_t, int16_t>(visit, next, local_csr->GetVertexVectorTyped<int64_t>(), local_csr->GetEdgeVectorTyped<int16_t>(), v_size);
      } else if (v_type == 64 && e_type == 32) {
        Explore<int64_t, int32_t>(visit, next, local_csr->GetVertexVectorTyped<int64_t>(), local_csr->GetEdgeVectorTyped<int32_t>(), v_size);
      } else {
        Explore<int64_t, int64_t>(visit, next, local_csr->GetVertexVectorTyped<int64_t>(), local_csr->GetEdgeVectorTyped<int64_t>(), v_size);
      }
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
      if (state->partition_counter >= state->partition_ranges.size()) {
        state->local_csr_lock.unlock();
        break;
      }
      auto partition_range = state->partition_ranges[state->partition_counter++];
      state->local_csr_lock.unlock();
      // Printer::PrintF("worker %d processing partition %d to %d\n", worker_id, partition_range.first, partition_range.second);
      // Printer::PrintF("Partition counter: %d, max size %d\n", state->partition_counter.load(), state->partition_ranges.size());
      CheckChange(seen, next, partition_range);
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