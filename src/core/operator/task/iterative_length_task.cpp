#include "duckpgq/core/operator/task/iterative_length_task.hpp"
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <chrono>
#include <thread>


namespace duckpgq {
namespace core {

IterativeLengthTask::IterativeLengthTask(shared_ptr<Event> event_p,
                                   ClientContext &context,
                                   shared_ptr<BFSState> &state, idx_t worker_id,
                                   const PhysicalOperator &op_p)
: ExecutorTask(context, std::move(event_p), op_p), context(context),
  state(state), worker_id(worker_id) {
}

void IterativeLengthTask::CheckChange(vector<std::bitset<LANE_LIMIT>> &seen,
                                      vector<std::bitset<LANE_LIMIT>> &next,
                                      bool &change) const {
  for (auto c = 0; c <= (state->v_size >> 9); c ++) {
    if (state->thread_assignment[c] == worker_id) {
      auto lo = c << 9;
      auto hi = (c +1 ) << 9;
      if (hi > state->v_size) {
        hi = state->v_size;
      }
      for (auto i = lo; i < hi; i++) {
        if (next[i].any()) {
          next[i] &= ~seen[i];
          seen[i] |= next[i];
          change |= true;
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
      size_t range_size = ((state->v_size >> 9)+ state->num_threads - 1) / state->num_threads;

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
    } while (state->change);
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
    for (auto i = 0; i < state->v_size; i++) {
      if (visit[i].any()) {
        for (auto offset = v[i]; offset < v[i + 1]; offset++) {
          auto n = e[offset];
          if (0) {
            int64_t not_for_me = (thread_assignment[n >> 9] != worker_id);
            int64_t mask_for_me = not_for_me - 1;
            auto pos_for_me = n & mask_for_me;
            auto pos_not_for_me = (state->v_size + (worker_id << 9)) & ~mask_for_me;
            next[pos_for_me | pos_not_for_me] |= visit[i]; // Needs to be LANE_LIMIT bits, not 64
          } else { //if (thread_assignment[n >> 9] == worker_id) {
            next[n] |= visit[i]; // Needs to be LANE_LIMIT bits, not 64
          }
        }
      }
    }
}

void IterativeLengthTask::IterativeLength() {
    auto &seen = state->seen;
    auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
    auto &next = state->iter & 1 ? state->visit2 : state->visit1;
    auto &barrier = state->barrier;
    auto &change = state->change;
    int64_t *v = (int64_t *)state->csr->v;
    vector<int64_t> &e = state->csr->e;
    auto &thread_assignment = state->thread_assignment;
    auto thread_id = std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));;


    // Clear `next` array regardless of task availability
    if (worker_id == 0) {
      for (int64_t i = 0; i < state->v_size; i++) {
        next[i] = 0;
      }
    }

    // Synchronize after clearing
    volatile size_t gen = barrier->Wait();

    Explore(visit, next, v, e, thread_assignment);

    // Check and process tasks for the next phase
    change = false;
    gen = barrier->Wait();
    CheckChange(seen, next, change);

    gen = barrier->Wait();


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