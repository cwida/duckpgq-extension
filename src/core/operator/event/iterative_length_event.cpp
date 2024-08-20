#include "duckpgq/core/operator/event/iterative_length_event.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

ParallelIterativeEvent::ParallelIterativeEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p)
      : BasePipelineEvent(pipeline_p), gstate(gstate_p) {}


void ParallelIterativeEvent::Schedule() {
  auto &bfs_state = gstate.global_bfs_state;
  auto &context = pipeline->GetClientContext();

  vector<shared_ptr<Task>> bfs_tasks;
  for (idx_t tnum = 0; tnum < bfs_state->num_threads; tnum++) {
    bfs_tasks.push_back(make_uniq<PhysicalIterativeTask>(
        shared_from_this(), context, gstate, tnum));
  }
  SetTasks(std::move(bfs_tasks));
}

void ParallelIterativeEvent::FinishEvent() {
  auto &bfs_state = gstate.global_bfs_state;

  // if remaining pairs, schedule the BFS for the next batch
  if (bfs_state->started_searches < gstate.global_tasks->Count()) {
    PhysicalPathFinding::ScheduleBFSEvent(*pipeline, *this, gstate);
  }
}

} // namespace core
} // namespace duckpgq

