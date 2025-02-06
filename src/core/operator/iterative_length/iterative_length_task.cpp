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
}

void IterativeLengthTask::CheckChange(vector<std::bitset<LANE_LIMIT>> &seen,
                                      vector<std::bitset<LANE_LIMIT>> &next) const {
  for (auto i = 0; i < state->v_size; i++) {
    if (state->thread_assignment[i] == worker_id) {
      if (next[i].any()) {
        next[i] &= ~seen[i];
        seen[i] |= next[i];
        if (next[i].any()) {
          state->change_atomic.store(true, std::memory_order_relaxed);
        }
      }
    }
  }
}


  TaskExecutionResult IterativeLengthTask::ExecuteTask(TaskExecutionMode mode) {
  auto &barrier = state->barrier;
  while (state->started_searches < state->pairs->size()) {
    barrier->Wait();

    if (worker_id == 0) {
      // Calculate the range size for each thread
      size_t range_size = (state->v_size + state->num_threads - 1) / state->num_threads;

      // Assign ranges to threads
      for (auto thread_id = 0; thread_id < state->num_threads; thread_id++) {
        size_t start = thread_id * range_size;
        size_t end = std::min<size_t>((thread_id + 1) * range_size, static_cast<size_t>(state->v_size));
        
        for (auto n = start; n < end; n++) {
          state->thread_assignment[n] = thread_id;
        }
      }
      state->InitializeLanes();
    }
    barrier->Wait();
    do {
      IterativeLength();
      barrier->Wait();

      if (worker_id == 0) {
        ReachDetect();
      }
      barrier->Wait();
    } while (state->change_atomic);
    if (worker_id == 0) {
      UnReachableSet();
    }

    // Final synchronization before finishing
    barrier->Wait();
    if (worker_id == 0) {
      state->Clear();
    }
    barrier->Wait();
  }

  event->FinishTask();
  return TaskExecutionResult::TASK_FINISHED;
}

void IterativeLengthTask::Explore(vector<std::bitset<LANE_LIMIT>> &visit,
                                  vector<std::bitset<LANE_LIMIT>> &next,
                                  int64_t *v, vector<int64_t> &e,
                                  std::vector<int64_t> &thread_assignment) {
#pragma omp parallel
  {
    std::vector<std::bitset<LANE_LIMIT>> thread_local_next(state->v_size);

#pragma omp for nowait
    for (auto i = 0; i < state->v_size; i++) {
      if (visit[i].any()) {
        for (auto offset = v[i]; offset < v[i + 1]; offset++) {
          auto n = e[offset];
          thread_local_next[n] |= visit[i];
        }
      }
    }

#pragma omp critical
    {
      for (auto i = 0; i < state->v_size; i++) {
        next[i] |= thread_local_next[i];
      }
    }
  }
}

void IterativeLengthTask::IterativeLength() {
    auto &seen = state->seen;
    auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
    auto &next = state->iter & 1 ? state->visit2 : state->visit1;
    auto &barrier = state->barrier;
    int64_t *v = (int64_t *)state->csr->v;
    vector<int64_t> &e = state->csr->e;
    auto &thread_assignment = state->thread_assignment;

    // Clear `next` array regardless of task availability
    if (worker_id == 0) {
      for (int64_t i = 0; i < state->v_size; i++) {
        next[i] = 0;
      }
    }

    // Synchronize after clearing
    barrier->Wait();

    Explore(visit, next, v, e, thread_assignment);

    // Check and process tasks for the next phase
    state->change_atomic.store(false, std::memory_order_relaxed);
    barrier->Wait();
    CheckChange(seen, next);

    barrier->Wait();
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
      state->change_atomic.store(false, std::memory_order_relaxed);
    }
    // into the next iteration
    state->iter++;
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