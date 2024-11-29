// #include "duckpgq/core/operator/task/iterative_length_task.hpp"
// #include <duckpgq/core/operator/physical_path_finding_operator.hpp>
//
// namespace duckpgq {
// namespace core {
//
// PhysicalIterativeTask::PhysicalIterativeTask(shared_ptr<Event> event_p, ClientContext &context,
//                         PathFindingGlobalSinkState &state, idx_t worker_id,
//                                              const PhysicalOperator &op_p)
//       : ExecutorTask(context, std::move(event_p), op_p), context(context),
//         state(state), worker_id(worker_id) {}
//
// bool PhysicalIterativeTask::SetTaskRange() {
//   auto task = state.global_bfs_state->FetchTask();
//   if (task == nullptr) {
//     return false;
//   }
//   left = task->first;
//   right = task->second;
//   return true;
// }
//
//
//   TaskExecutionResult PhysicalIterativeTask::ExecuteTask(TaskExecutionMode mode) {
//     auto &bfs_state = state.global_bfs_state;
//     auto &barrier = bfs_state->barrier;
//     do {
//
//       IterativeLength();
//       barrier->Wait([&]() {
//             bfs_state->ResetTaskIndex();  // Reset task index safely
//         });
//
//       barrier->Wait();
//
//       if (worker_id == 0) {
//         ReachDetect();
//       }
//
//       barrier->Wait([&]() {
//         bfs_state->ResetTaskIndex();  // Reset task index safely
//       });
//
//       barrier->Wait();
//     } while (bfs_state->change);
//
//     if (worker_id == 0) {
//       UnReachableSet();
//     }
//
//     event->FinishTask();
//     return TaskExecutionResult::TASK_FINISHED;
//   }
//
//   void PhysicalIterativeTask::IterativeLength() {
//     auto &bfs_state = state.global_bfs_state;
//     auto &seen = bfs_state->seen;
//     auto &visit = bfs_state->iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
//     auto &next = bfs_state->iter & 1 ? bfs_state->visit2 : bfs_state->visit1;
//     auto &barrier = bfs_state->barrier;
//     int64_t *v = (int64_t *)state.csr->v;
//     vector<int64_t> &e = state.csr->e;
//     auto &change = bfs_state->change;
//
//     // Attempt to get a task range
//     bool has_tasks = SetTaskRange();
//     // std::cout << "Worker " << worker_id << ": Has tasks = " << has_tasks << std::endl;
//
//     // Clear `next` array regardless of task availability
//     for (auto i = left; i < right; i++) {
//         next[i] = 0;
//     }
//
//     // Synchronize after clearing
//     barrier->Wait();
//
//     // Main processing loop
//     while (has_tasks) {
//         for (auto i = left; i < right; i++) {
//             if (visit[i].any()) {
//                 for (auto offset = v[i]; offset < v[i + 1]; offset++) {
//                     auto n = e[offset];
//                     {
//                         std::lock_guard<std::mutex> lock(bfs_state->element_locks[n]);
//                         next[n] |= visit[i];
//                     }
//                 }
//             }
//         }
//         has_tasks = SetTaskRange();
//     }
//
//     // Synchronize at the end of the main processing
//     barrier->Wait([&]() {
//         // std::cout << "Worker " << worker_id << ": Resetting task index." << std::endl;
//         bfs_state->ResetTaskIndex();
//     });
//     barrier->Wait();
//
//     // Check and process tasks for the next phase
//     has_tasks = SetTaskRange();
//     change = false;
//
//     while (has_tasks) {
//         for (auto i = left; i < right; i++) {
//             if (next[i].any()) {
//                 next[i] &= ~seen[i];
//                 seen[i] |= next[i];
//                 change |= next[i].any();
//             }
//         }
//         has_tasks = SetTaskRange();
//     }
//
//     // Final synchronization after processing
//     barrier->Wait([&]() {
//         // std::cout << "Worker " << worker_id << ": Resetting task index at second barrier." << std::endl;
//         bfs_state->ResetTaskIndex();
//     });
//     barrier->Wait();
// }
//
//   void PhysicalIterativeTask::ReachDetect() const {
//     auto &bfs_state = state.global_bfs_state;
//     auto result_data = FlatVector::GetData<int64_t>(bfs_state->result.data[0]);
//
//     // detect lanes that finished
//     for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
//       int64_t search_num = bfs_state->lane_to_num[lane];
//       if (search_num >= 0) { // active lane
//         int64_t dst_pos = bfs_state->vdata_dst.sel->get_index(search_num);
//         if (bfs_state->seen[bfs_state->dst[dst_pos]][lane]) {
//           result_data[search_num] =
//               bfs_state->iter; /* found at iter => iter = path length */
//           bfs_state->lane_to_num[lane] = -1; // mark inactive
//           bfs_state->active--;
//         }
//       }
//     }
//     if (bfs_state->active == 0) {
//       bfs_state->change = false;
//     }
//     // into the next iteration
//     bfs_state->iter++;
//   }
//
//   void PhysicalIterativeTask::UnReachableSet() const {
//     auto &bfs_state = state.global_bfs_state;
//     auto result_data = FlatVector::GetData<int64_t>(bfs_state->result.data[0]);
//     auto &result_validity = FlatVector::Validity(bfs_state->result.data[0]);
//
//     for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
//       int64_t search_num = bfs_state->lane_to_num[lane];
//       if (search_num >= 0) { // active lane
//         result_validity.SetInvalid(search_num);
//         result_data[search_num] = (int64_t)-1; /* no path */
//         bfs_state->lane_to_num[lane] = -1;     // mark inactive
//       }
//     }
//   }
//
// } // namespace core
// } // namespace duckpgq