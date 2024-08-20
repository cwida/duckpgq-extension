#include "duckpgq/core/operator/event/shortest_path_event.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/operator/task/shortest_path_task.hpp>

namespace duckpgq {
namespace core {

ParallelShortestPathEvent::ParallelShortestPathEvent(PathFindingGlobalState &gstate_p,
                          Pipeline &pipeline_p)
    : BasePipelineEvent(pipeline_p), gstate(gstate_p) {

}

void ParallelShortestPathEvent::Schedule() {
  auto &bfs_state = gstate.global_bfs_state;
  auto &context = pipeline->GetClientContext();

  vector<shared_ptr<Task>> bfs_tasks;
  for (idx_t tnum = 0; tnum < bfs_state->num_threads; tnum++) {
    bfs_tasks.push_back(make_uniq<PhysicalShortestPathTask>(
        shared_from_this(), context, gstate, tnum));
  }
  SetTasks(std::move(bfs_tasks));
}

void ParallelShortestPathEvent::FinishEvent() {
  auto &bfs_state = gstate.global_bfs_state;

  // if remaining pairs, schedule the BFS for the next batch
  if (bfs_state->started_searches < gstate.global_tasks->Count()) {
    PhysicalPathFinding::ScheduleBFSEvent(*pipeline, *this, gstate);
  }
};



} // namespace core
} // namespace duckpgq