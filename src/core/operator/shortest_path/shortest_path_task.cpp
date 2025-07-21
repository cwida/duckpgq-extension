// #include "duckpgq/core/operator/task/shortest_path_task.hpp"
// #include <duckpgq/core/operator/physical_path_finding_operator.hpp>
// namespace duckpgq {
// namespace core {
//
// ShortestPathTask::ShortestPathTask(shared_ptr<Event> event_p,
//                                    ClientContext &context,
//                                    shared_ptr<BFSState> &state, idx_t worker_id,
//                                    const PhysicalOperator &op_p)
//     : ExecutorTask(context, std::move(event_p), op_p), context(context),
//       state(state), worker_id(worker_id) {
// }
//
// TaskExecutionResult ShortestPathTask::ExecuteTask(TaskExecutionMode mode) {
//   auto &barrier = state->barrier;
//   while (state->started_searches < state->pairs->size()) {
//     barrier->Wait();
//     if (worker_id == 0) {
//       for (idx_t n = 0; n < state->v_size; n++) {
//         state->thread_assignment[n] = n % state->num_threads;
     //   }
     //   state->InitializeLanes();
     // }
     //
     // barrier->Wait();
     // do {
     //   IterativePath();
//
//       // Synchronize after IterativePath
//       barrier->Wait();
//
//       if (worker_id == 0) {
//         ReachDetect();
//       }
//       barrier->Wait();
//     } while (state->change);
//
//     barrier->Wait();
//     if (worker_id == 0) {
//       PathConstruction();
//     }
//
//     // Final synchronization before finishing
//     barrier->Wait();
//     if (worker_id == 0) {
//       state->Clear();
//       // std::cout << "Started searches: " << state->started_searches << std::endl;
//       // std::cout << "Number of pairs: " << state->pairs->size() << std::endl;
//     }
//
//     barrier->Wait();
//   }
//   event->FinishTask();
//   return TaskExecutionResult::TASK_FINISHED;
// }
//
// void ShortestPathTask::IterativePath() {
//   auto &seen = state->seen;
//   auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
//   auto &next = state->iter & 1 ? state->visit2 : state->visit1;
//   auto &barrier = state->barrier;
//   int64_t *v = (int64_t *)state->csr->v;
//   vector<int64_t> &e = state->csr->e;
//   auto &edge_ids = state->csr->edge_ids;
//   auto &parents_ve = state->parents_ve;
//   auto &change = state->change;
//   auto &thread_assignment = state->thread_assignment;
//
//   // Attempt to get a task range
//   if (worker_id == 0) {
//     for (auto i = 0; i < state->v_size; i++) {
//       next[i].reset();
//     }
//   }
//
//
//   // Synchronize after clearing
//   barrier->Wait();
//
//   // Main processing loop
//   for (auto i = 0; i < state->v_size; i++) {
//     if (visit[i].any()) {
//       for (auto offset = v[i]; offset < v[i + 1]; offset++) {
//         auto n = e[offset];
//         if (thread_assignment[n] == worker_id) {
//           auto edge_id = edge_ids[offset];
//
//           next[n] |= visit[i];
//           for (auto l = 0; l < LANE_LIMIT; l++) {
//             // Create the mask: true (-1 in all bits) if condition is met, else
//             // 0
//             uint64_t mask = ((parents_ve[n][l].GetV() == -1) && visit[i][l])
//                                 ? ~uint64_t(0)
//                                 : 0;
//
//             // Use the mask to conditionally update the `value` field of the
//             // `ve` struct
//             uint64_t new_value =
//                 (static_cast<uint64_t>(i) << parents_ve[n][l].e_bits) |
//                 (edge_id & parents_ve[n][l].e_mask);
//             parents_ve[n][l].value =
//                 (mask & new_value) | (~mask & parents_ve[n][l].value);
//           }
//         }
//       }
//     }
//
//   }
//
//   // Second processing stage (if needed)
//   change = false;
//   barrier->Wait();
//   if (worker_id == 0) {
//     for (auto i = 0; i < state->v_size; i++) {
//       if (next[i].any()) {
//         next[i] &= ~seen[i];
//         seen[i] |= next[i];
//         if (next[i].any()) {
//           change = true;
//         }
//       }
//     }
//   }
//   barrier->Wait();
// }
//
// void ShortestPathTask::ReachDetect() {
//   // detect lanes that finished
//   // std::cout << "Got in reach detect" << std::endl;
//   for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
//     int64_t search_num = state->lane_to_num[lane];
//     if (search_num >= 0 &&
//         !state->lane_completed.test(
//             lane)) { // Active lane that has not yet completed
//       //! Check if dst for a source has been seen
//       int64_t dst_pos = state->vdata_dst.sel->get_index(search_num);
//       if (state->seen[state->dst[dst_pos]][lane]) {
//         state->active--;                 // Decrement active count
//         state->lane_completed.set(lane); // Mark this lane as completed
//       }
//     }
//   }
//   // std::cout << "Active lanes: " << state->active << std::endl;
//   if (state->active == 0) {
//     state->change = false;
//   }
//   // into the next iteration
//   state->iter++;
//   // std::cout << "Change: " << state->change << std::endl;
//   // std::cout << "Incremented iteration counter" << std::endl;
// }
//
// void ShortestPathTask::PathConstruction() {
//   auto result_data =
//       FlatVector::GetData<list_entry_t>(state->pf_results->data[0]);
//   auto &result_validity = FlatVector::Validity(state->pf_results->data[0]);
//   //! Reconstruct the paths
//   for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
//     int64_t search_num = state->lane_to_num[lane];
//     if (search_num == -1) { // empty lanes
//       continue;
//     }
//
//     //! Searches that have stopped have found a path
//     int64_t src_pos = state->vdata_src.sel->get_index(search_num);
//     int64_t dst_pos = state->vdata_dst.sel->get_index(search_num);
//
//     // std::cout << state->src[src_pos] << " " << state->dst[dst_pos] << std::endl;
//
//     if (state->src[src_pos] == state->dst[dst_pos]) { // Source == destination
//       unique_ptr<Vector> output =
//           make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
//       ListVector::PushBack(*output, state->src[src_pos]);
//       ListVector::Append(state->pf_results->data[0],
//                          ListVector::GetEntry(*output),
//                          ListVector::GetListSize(*output));
//       result_data[search_num].length = ListVector::GetListSize(*output);
//       result_data[search_num].offset = state->current_batch_path_list_len;
//       state->current_batch_path_list_len += result_data[search_num].length;
//       continue;
//     }
//     std::vector<int64_t> output_vector;
//     std::vector<int64_t> output_edge;
//     auto source_v = state->src[src_pos]; // Take the source
//
//     auto parent_vertex = state->parents_ve[state->dst[dst_pos]][lane].GetV();
//     auto parent_edge = state->parents_ve[state->dst[dst_pos]][lane].GetE();
//     output_vector.push_back(state->dst[dst_pos]); // Add destination vertex
//     output_vector.push_back(parent_edge);
//     while (parent_vertex != source_v) { // Continue adding vertices until we
//                                         // have reached the source vertex
//       //! -1 is used to signify no parent
//       if (parent_vertex == -1 ||
//           parent_vertex == state->parents_ve[parent_vertex][lane].GetV()) {
//         result_validity.SetInvalid(search_num);
//         break;
//       }
//       output_vector.push_back(parent_vertex);
//       parent_edge = state->parents_ve[parent_vertex][lane].GetE();
//       parent_vertex = state->parents_ve[parent_vertex][lane].GetV();
//       output_vector.push_back(parent_edge);
//     }
//
//     if (!result_validity.RowIsValid(search_num)) {
//       continue;
//     }
//     output_vector.push_back(source_v);
//     std::reverse(output_vector.begin(), output_vector.end());
//     auto output = make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
//     for (auto val : output_vector) {
//       Value value_to_insert = val;
//       ListVector::PushBack(*output, value_to_insert);
//     }
//     auto list_len = ListVector::GetListSize(*output);
//
//     result_data[search_num].length = list_len;
//     result_data[search_num].offset = state->current_batch_path_list_len;
//     ListVector::Append(state->pf_results->data[0],
//                        ListVector::GetEntry(*output), list_len);
//     state->current_batch_path_list_len += list_len;
//   }
// }
//
// } // namespace core
// } // namespace duckpgq