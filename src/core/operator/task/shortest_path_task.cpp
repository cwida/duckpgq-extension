#include "duckpgq/core/operator/task/shortest_path_task.hpp"
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

PhysicalShortestPathTask::PhysicalShortestPathTask(shared_ptr<Event> event_p, ClientContext &context,
                           PathFindingGlobalSinkState &state, idx_t worker_id, const PhysicalOperator &op_p)
      : ExecutorTask(context, std::move(event_p), op_p), context(context),
        state(state), worker_id(worker_id) {
  left = right = UINT64_MAX; // NOLINT
}

  TaskExecutionResult PhysicalShortestPathTask::ExecuteTask(TaskExecutionMode mode) {
    auto &bfs_state = state.global_bfs_state;
    auto &barrier = bfs_state->barrier;

  do {
    IterativePath();

    // Synchronize after IterativePath
    barrier->Wait();
    if (worker_id == 0) {
      bfs_state->ResetTaskIndex();
    }
    barrier->Wait();

    if (worker_id == 0) {
      ReachDetect();
    }

    // std::cout << "Worker " << worker_id << ": Waiting at barrier before ResetTaskIndex." << std::endl;
    barrier->Wait();
    if (worker_id == 0) {
      bfs_state->ResetTaskIndex();
      // std::cout << "Worker " << worker_id << ": ResetTaskIndex completed." << std::endl;
    }
    barrier->Wait();
    // std::cout << "Worker " << worker_id << ": Passed barrier after ResetTaskIndex." << std::endl;
  } while (bfs_state->change);

  barrier->Wait();
  if (worker_id == 0) {
    // std::cout << "Worker " << worker_id << " started path construction" << std::endl;
    PathConstruction();
    // std::cout << "Worker " << worker_id << " finished path construction" << std::endl;
  }

  // Final synchronization before finishing
  barrier->Wait();
  // std::cout << "Worker " << worker_id << " finishing task" << std::endl;
  event->FinishTask();
  return TaskExecutionResult::TASK_FINISHED;

  }

  bool PhysicalShortestPathTask::SetTaskRange() {
    auto task = state.global_bfs_state->FetchTask();
    if (task == nullptr) {
      return false;
    }
    left = task->first;
    right = task->second;
    return true;
}

  void PhysicalShortestPathTask::IterativePath() {
    auto &bfs_state = state.global_bfs_state;
    auto &seen = bfs_state->seen;
    auto &visit = bfs_state->iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
    auto &next = bfs_state->iter & 1 ? bfs_state->visit2 : bfs_state->visit1;
    auto &barrier = bfs_state->barrier;
    int64_t *v = (int64_t *)state.csr->v;
    vector<int64_t> &e = state.csr->e;
    auto &edge_ids = state.csr->edge_ids;
    auto &parents_ve = bfs_state->parents_ve;
    auto &change = bfs_state->change;

    // Attempt to get a task range
    bool has_tasks = SetTaskRange();
    // std::cout << "Worker " << worker_id << ": Has tasks = " << has_tasks << std::endl;

    // Clear next array regardless of whether the worker has tasks
    for (auto i = left; i < right; i++) {
        next[i] = 0;
    }

    // Synchronize after clearing
    barrier->Wait();
    // std::cout << "Worker " << worker_id << ": Passed first barrier." << std::endl;

    // Main processing loop
    while (has_tasks) {
        for (auto i = left; i < right; i++) {
            if (visit[i].any()) {
                for (auto offset = v[i]; offset < v[i + 1]; offset++) {
                    auto n = e[offset];
                    auto edge_id = edge_ids[offset];
                    {
                        std::lock_guard<std::mutex> lock(bfs_state->element_locks[n]);
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
        if (!has_tasks) {
            // std::cout << "Worker " << worker_id << ": No more tasks found to explore." << std::endl;
        }
    }

    // Synchronize at the end of the main processing
    barrier->Wait([&]() {
        // std::cout << "Worker " << worker_id << ": Resetting task index." << std::endl;
        bfs_state->ResetTaskIndex();
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
        // std::cout << "Worker " << worker_id << ": Resetting task index at second barrier." << std::endl;
        bfs_state->ResetTaskIndex();
    });
    barrier->Wait();
    // std::cout << "Worker " << worker_id << ": Passed second barrier." << std::endl;
}

  void PhysicalShortestPathTask::ReachDetect() {
    auto &bfs_state = state.global_bfs_state;
    // detect lanes that finished
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = bfs_state->lane_to_num[lane];
      if (search_num >= 0) { // active lane
        //! Check if dst for a source has been seen
        int64_t dst_pos = bfs_state->vdata_dst.sel->get_index(search_num);
        if (bfs_state->seen[bfs_state->dst[dst_pos]][lane]) {
          bfs_state->active--;
        }
      }
    }
    if (bfs_state->active == 0) {
      bfs_state->change = false;
    }
    // into the next iteration
    bfs_state->iter++;
  }

  void PhysicalShortestPathTask::PathConstruction() {
    auto &bfs_state = state.global_bfs_state;
    auto &result = bfs_state->result.data[0];
    auto result_data = FlatVector::GetData<list_entry_t>(result);
    auto &result_validity = FlatVector::Validity(result);
    //! Reconstruct the paths
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = bfs_state->lane_to_num[lane];
      if (search_num == -1) { // empty lanes
        continue;
      }

      //! Searches that have stopped have found a path
      int64_t src_pos = bfs_state->vdata_src.sel->get_index(search_num);
      int64_t dst_pos = bfs_state->vdata_dst.sel->get_index(search_num);
      if (bfs_state->src[src_pos] ==
          bfs_state->dst[dst_pos]) { // Source == destination
        unique_ptr<Vector> output =
            make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
        ListVector::PushBack(*output, bfs_state->src[src_pos]);
        ListVector::Append(result, ListVector::GetEntry(*output),
                           ListVector::GetListSize(*output));
        result_data[search_num].length = ListVector::GetListSize(*output);
        result_data[search_num].offset = bfs_state->total_len;
        bfs_state->total_len += result_data[search_num].length;
        continue;
      }
      std::vector<int64_t> output_vector;
      std::vector<int64_t> output_edge;
      auto source_v = bfs_state->src[src_pos]; // Take the source

      auto parent_vertex =
          bfs_state->parents_ve[bfs_state->dst[dst_pos]][lane].GetV();
      auto parent_edge =
          bfs_state->parents_ve[bfs_state->dst[dst_pos]][lane].GetE();

      output_vector.push_back(
          bfs_state->dst[dst_pos]); // Add destination vertex
      output_vector.push_back(parent_edge);
      while (parent_vertex != source_v) { // Continue adding vertices until we
                                          // have reached the source vertex
        //! -1 is used to signify no parent
        if (parent_vertex == -1 ||
            parent_vertex ==
                bfs_state->parents_ve[parent_vertex][lane].GetV()) {
          result_validity.SetInvalid(search_num);
          break;
        }
        output_vector.push_back(parent_vertex);
        parent_edge = bfs_state->parents_ve[parent_vertex][lane].GetE();
        parent_vertex = bfs_state->parents_ve[parent_vertex][lane].GetV();
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
      result_data[search_num].offset = bfs_state->total_len;
      ListVector::Append(result, ListVector::GetEntry(*output),
                         ListVector::GetListSize(*output));
      bfs_state->total_len += result_data[search_num].length;
    }
  }

} // namespace core
} // namespace duckpgq