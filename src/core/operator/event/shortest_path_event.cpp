#include "duckpgq/core/operator/event/shortest_path_event.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/operator/task/shortest_path_task.hpp>

namespace duckpgq {
namespace core {

ParallelShortestPathEvent::ParallelShortestPathEvent(PathFindingGlobalSinkState &gstate_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
    : BasePipelineEvent(pipeline_p), gstate(gstate_p), op(op_p) {

}

void ParallelShortestPathEvent::Schedule() {
  auto &bfs_state = gstate.global_bfs_state;
  auto &context = pipeline->GetClientContext();

  vector<shared_ptr<Task>> bfs_tasks;
  size_t threads_to_schedule = std::min(bfs_state->num_threads, (idx_t)bfs_state->global_task_queue.size());
  for (idx_t tnum = 0; tnum < threads_to_schedule; tnum++) {
    bfs_tasks.push_back(make_uniq<PhysicalShortestPathTask>(
        shared_from_this(), context, gstate, tnum, op));
  }
  SetTasks(std::move(bfs_tasks));
}

void ParallelShortestPathEvent::FinishEvent() {
  auto &bfs_state = gstate.global_bfs_state;

  // if remaining pairs, schedule the BFS for the next batch
  if (bfs_state->started_searches < gstate.global_pairs->Count()) {
    op.ScheduleBFSEvent(*pipeline, *this, gstate);
  }
};



} // namespace core
} // namespace duckpgq