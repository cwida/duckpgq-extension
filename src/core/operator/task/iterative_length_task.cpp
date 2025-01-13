#include "duckpgq/core/operator/task/iterative_length_task.hpp"
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

IterativeLengthTask::IterativeLengthTask(shared_ptr<Event> event_p,
                                   ClientContext &context,
                                   shared_ptr<BFSState> &state, idx_t worker_id,
                                   const PhysicalOperator &op_p)
: ExecutorTask(context, std::move(event_p), op_p), context(context),
  state(state), worker_id(worker_id) {
  left = right = UINT64_MAX; // NOLINT
}

bool IterativeLengthTask::SetTaskRange() {
  auto task = state->FetchTask();
  if (task == nullptr) {
    return false;
  }
  left = task->first;
  right = task->second;
  return true;
}

void IterativeLengthTask::CheckChange(vector<std::bitset<LANE_LIMIT>> &seen,
                                      vector<std::bitset<LANE_LIMIT>> &next,
                                      bool &change) const {
  for (auto i = left; i < right; i++) {
    auto updated = next[i] & ~seen[i];
    seen[i] |= updated;
    change |= updated.any();
  }
}


  TaskExecutionResult IterativeLengthTask::ExecuteTask(TaskExecutionMode mode) {
    auto &barrier = state->barrier;
    while (state->started_searches < state->pairs->size()) {
      barrier->Wait();
      if (worker_id == 0) {
        state->InitializeLanes();
      }
      barrier->Wait();
      do {

        IterativeLength();
        barrier->Wait([&]() {
              state->ResetTaskIndex();  // Reset task index safely
          });

        barrier->Wait();

        if (worker_id == 0) {
          ReachDetect();
        }

        barrier->Wait([&]() {
          state->ResetTaskIndex();  // Reset task index safely
        });

        barrier->Wait();
      } while (state->change);

      if (worker_id == 0) {
        UnReachableSet();
      }

      // Final synchronization before finishing
      barrier->Wait();
      if (worker_id == 0) {
        state->Clear();
        // std::cout << "Started searches: " << state->started_searches << std::endl;
        // std::cout << "Number of pairs: " << state->pairs->size() << std::endl;
      }

      barrier->Wait();
    }

    event->FinishTask();
    return TaskExecutionResult::TASK_FINISHED;
  }

  void IterativeLengthTask::IterativeLength() {
    auto &seen = state->seen;
    auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
    auto &next = state->iter & 1 ? state->visit2 : state->visit1;
    auto &barrier = state->barrier;
    int64_t *v = (int64_t *)state->csr->v;
    vector<int64_t> &e = state->csr->e;
    auto &change = state->change;

    // Attempt to get a task range
    bool has_tasks = SetTaskRange();

    // Clear `next` array regardless of task availability
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
                    {
                        std::lock_guard<std::mutex> lock(state->element_locks[n]);
                        next[n] |= visit[i];
                    }
                }
            }
        }
        has_tasks = SetTaskRange();
    }

    // Synchronize at the end of the main processing
    barrier->Wait([&]() {
        state->ResetTaskIndex();
    });
    barrier->Wait();

    // Check and process tasks for the next phase
    has_tasks = SetTaskRange();
    change = false;

    while (has_tasks) {
        CheckChange(seen, next, change);
        has_tasks = SetTaskRange();
    }

    // Final synchronization after processing
    barrier->Wait([&]() {
        state->ResetTaskIndex();
    });
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