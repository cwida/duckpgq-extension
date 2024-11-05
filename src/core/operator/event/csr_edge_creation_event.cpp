// #include "duckpgq/core/operator/event/csr_edge_creation_event.hpp"
// #include <duckpgq/core/operator/task/csr_edge_creation_task.hpp>
//
// namespace duckpgq {
// namespace core {
//
// CSREdgeCreationEvent::CSREdgeCreationEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p, const PhysicalOperator &op_p)
//       : BasePipelineEvent(pipeline_p), gstate(gstate_p), op(op_p) {}
//
// void CSREdgeCreationEvent::Schedule() {
//   auto &context = pipeline->GetClientContext();
//   auto &ts = TaskScheduler::GetScheduler(context);
//   idx_t num_threads = ts.NumberOfThreads();
//   auto &scan_state = gstate.scan_state;
//   auto &global_inputs = gstate.global_csr_id;
//
//   global_inputs->InitializeScan(scan_state);
//
//   vector<shared_ptr<Task>> tasks;
//   for (idx_t tnum = 0; tnum < num_threads; tnum++) {
//     tasks.push_back(make_uniq<PhysicalCSREdgeCreationTask>(shared_from_this(),
//                                                            context, gstate, op));
//   }
//   SetTasks(std::move(tasks));
// }
//
// void CSREdgeCreationEvent::FinishEvent() {
//   auto &gstate = this->gstate;
//   auto &global_csr = gstate.global_csr;
//   global_csr->is_ready = true;
// }
//
// } // namespace core
// } // namespace duckpgq
