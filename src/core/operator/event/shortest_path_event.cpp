#include "duckpgq/core/operator/event/shortest_path_event.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/operator/task/shortest_path_task.hpp>

namespace duckpgq {
namespace core {

ShortestPathEvent::ShortestPathEvent(shared_ptr<BFSState> gbfs_state_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
    : BasePipelineEvent(pipeline_p), gbfs_state(std::move(gbfs_state_p)), op(op_p) {

}

void ShortestPathEvent::Schedule() {
  auto &context = pipeline->GetClientContext();
  // std::cout << gbfs_state->csr->ToString();
  vector<shared_ptr<Task>> bfs_tasks;
  for (idx_t tnum = 0; tnum < gbfs_state->scheduled_threads; tnum++) {
    bfs_tasks.push_back(make_uniq<ShortestPathTask>(
        shared_from_this(), context, gbfs_state, tnum, op));
  }
  SetTasks(std::move(bfs_tasks));
}

void ShortestPathEvent::FinishEvent() {
  std::cout << "Finished BFSEvent" << std::endl;
}

} // namespace core
} // namespace duckpgq