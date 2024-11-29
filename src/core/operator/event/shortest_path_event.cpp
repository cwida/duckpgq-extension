#include "duckpgq/core/operator/event/shortest_path_event.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/operator/task/shortest_path_task.hpp>

namespace duckpgq {
namespace core {

ShortestPathEvent::ShortestPathEvent(GlobalBFSState &gbfs_state_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
    : BasePipelineEvent(pipeline_p), gbfs_state(gbfs_state_p), op(op_p) {

}

void ShortestPathEvent::Schedule() {
  auto &context = pipeline->GetClientContext();

  vector<shared_ptr<Task>> bfs_tasks;
  size_t threads_to_schedule = std::min(gbfs_state.num_threads, (idx_t)gbfs_state.global_task_queue.size());
  for (idx_t tnum = 0; tnum < threads_to_schedule; tnum++) {
    bfs_tasks.push_back(make_uniq<ShortestPathTask>(
        shared_from_this(), context, gbfs_state, tnum, op));
  }
  SetTasks(std::move(bfs_tasks));
}


} // namespace core
} // namespace duckpgq