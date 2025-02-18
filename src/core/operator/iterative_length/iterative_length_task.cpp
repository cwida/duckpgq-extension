#include "duckpgq/core/operator/iterative_length/iterative_length_task.hpp"
#include <duckpgq/core/operator/iterative_length/iterative_length_state.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

IterativeLengthTask::IterativeLengthTask(shared_ptr<Event> event_p,
                                   ClientContext &context,
                                   shared_ptr<IterativeLengthState> &state, idx_t worker_id,
                                   const PhysicalOperator &op_p,
                                   unique_ptr<LocalCSR> local_csr_)
: ExecutorTask(context, std::move(event_p), op_p), context(context),
  state(state), worker_id(worker_id), local_csr(std::move(local_csr_)) {
}

void IterativeLengthTask::CheckChange(vector<std::bitset<LANE_LIMIT>> &seen,
                                      vector<std::bitset<LANE_LIMIT>> &next,
                                      vector<int64_t> &v, vector<int64_t> &e) const {
  // Printer::PrintF("CheckChange: %d %d %d\n", worker_id, local_csr->v_offset,
  //                 local_csr->vsize);
  // Printer::PrintF("%d: Checking change for upper %d, v_offset %d", worker_id, upper, local_csr->v_offset);
  for (auto i = 0; i < next.size(); i++) {
    // Printer::PrintF("%d %d", worker_id, i);
    if (next[i].any()) {
      next[i] &= ~seen[i];
      seen[i] |= next[i];
      if (next[i].any()) {
        state->change = true;
      }
    }
  }
}


TaskExecutionResult IterativeLengthTask::ExecuteTask(TaskExecutionMode mode) {
  auto &barrier = state->barrier;
  // Printer::PrintF("CSR Sizes - (worker %d): vsize: %d esize: %d", worker_id, local_csr->vsize, local_csr->e.size());
  while (state->started_searches < state->pairs->size()) {
    barrier->Wait(worker_id);

    // Printer::PrintF("worker %d\n%s", worker_id, local_csr->ToString());
    if (worker_id == 0) {
      barrier->LogMessage(worker_id, "Initializing lanes");
      state->InitializeLanes();
      // Printer::Print("Finished intialize lanes");
    }
    barrier->Wait(worker_id);
    do {
      IterativeLength();
      if (worker_id == 0) {
        barrier->LogMessage(worker_id, "Finished IterativeLength");
      }
      barrier->Wait(worker_id);
      if (worker_id == 0) {
        ReachDetect();
        barrier->LogMessage(worker_id, "Finished ReachDetect");
      }
      barrier->Wait(worker_id);
    } while (state->change);
    if (worker_id == 0) {
      UnReachableSet();
      barrier->LogMessage(worker_id, "Finished UnreachableSet");
    }

    // Final synchronization before finishing
    barrier->Wait(worker_id);
    if (worker_id == 0) {
      state->Clear();
      barrier->LogMessage(worker_id, "Cleared state");
    }
    barrier->Wait(worker_id);
  }

  event->FinishTask();
  return TaskExecutionResult::TASK_FINISHED;
}

void IterativeLengthTask::Explore(vector<std::bitset<LANE_LIMIT>> &visit,
                                  vector<std::bitset<LANE_LIMIT>> &next,
                                  vector<int64_t> &v, vector<int64_t> &e) {
  for (auto i = 0; i < v.size() - 2; i++) {
    // Printer::PrintF("worker %d %d\n", worker_id, i);
    if (visit[i].any()) { // If the vertex has been visited
      for (auto offset = v[i]; offset < v[i + 1]; offset++) {
        // Printer::PrintF("worker %d %d %d\n", worker_id, i, offset);
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
    auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
    auto &next = state->iter & 1 ? state->visit2 : state->visit1;
    auto &barrier = state->barrier;
    vector<int64_t> &v = local_csr->v;
    vector<int64_t> &e = local_csr->e;
    // Clear `next` array regardless of task availability
    if (worker_id == 0) {
      for (auto i = 0; i < next.size(); i++) {
        next[i] = 0;
      }
    }

    // Synchronize after clearing
    barrier->Wait(worker_id);
    // Printer::PrintF("%d starting explore\n", worker_id);

    Explore(visit, next, v, e);
    // Printer::PrintF("%d finished explore\n", worker_id);
    barrier->Wait(worker_id);
    // if (worker_id == 0) {
    //   std::cout << "Iteration: " << state->iter << std::endl;
    //   PrintMatrix("Visit", visit);
    //   PrintMatrix("Next", next);
    //   PrintMatrix("Seen", seen);
    // }
    // Check and process tasks for the next phase
    state->change = false;
    barrier->Wait(worker_id);
    if (worker_id == 0) {
      CheckChange(seen, next, v, e);
    }
    barrier->Wait(worker_id);
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