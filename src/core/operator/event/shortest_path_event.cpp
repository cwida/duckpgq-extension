#include "duckpgq/core/operator/event/shortest_path_event.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/operator/task/shortest_path_task.hpp>

namespace duckpgq {
namespace core {

ShortestPathEvent::ShortestPathEvent(shared_ptr<GlobalBFSState> gbfs_state_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
    : BasePipelineEvent(pipeline_p), gbfs_state(std::move(gbfs_state_p)), op(op_p) {

}

void ShortestPathEvent::Schedule() {
  auto &context = pipeline->GetClientContext();
  // std::cout << gbfs_state->csr->ToString();
  vector<shared_ptr<Task>> bfs_tasks;
  std::cout << "Scheduling threads " << gbfs_state->scheduled_threads << std::endl;
  for (idx_t tnum = 0; tnum < gbfs_state->scheduled_threads; tnum++) {
    std::cout << "Scheduling task" << std::endl;
    bfs_tasks.push_back(make_uniq<ShortestPathTask>(
        shared_from_this(), context, gbfs_state, tnum, op));
  }
  SetTasks(std::move(bfs_tasks));
}

// void ShortestPathEvent::FinishEvent() {
//   std::cout << "Total pairs processed: " << gbfs_state->started_searches << std::endl;
//   std::cout << "Number of pairs: " << gbfs_state->current_pairs_batch->size() << std::endl;
//   gbfs_state->path_finding_result->Print();
//   // if remaining pairs, schedule the BFS for the next batch
//   if (gbfs_state->started_searches == gbfs_state->current_pairs_batch->size()) {
//     gbfs_state->path_finding_result->SetCardinality(gbfs_state->current_pairs_batch->size());
//     gbfs_state->total_pairs_processed += gbfs_state->current_pairs_batch->size();
//     gbfs_state->current_pairs_batch->Fuse(*gbfs_state->path_finding_result);
//     gbfs_state->results->Append(*gbfs_state->current_pairs_batch);
//
//   }
//   std::cout << gbfs_state->started_searches << " " << gbfs_state->pairs->Count() << std::endl;
//   std::cout << "Finished event" << std::endl;
// }

} // namespace core
} // namespace duckpgq