#include "duckpgq/core/operator/event/iterative_length_event.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

ParallelIterativeEvent::ParallelIterativeEvent(GlobalBFSState &gstate_p, Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
      : BasePipelineEvent(pipeline_p), gstate(gstate_p), op(op_p) {}


void ParallelIterativeEvent::Schedule() {
  auto &context = pipeline->GetClientContext();

  vector<shared_ptr<Task>> bfs_tasks;
  size_t threads_to_schedule = std::min(gstate.num_threads, (idx_t)gstate.global_task_queue.size());

  for (idx_t tnum = 0; tnum < threads_to_schedule; tnum++) {
    bfs_tasks.push_back(make_uniq<PhysicalIterativeTask>(
        shared_from_this(), context, gstate, tnum, op));
  }
  SetTasks(std::move(bfs_tasks));
}

void ParallelIterativeEvent::FinishEvent() {

  // // if remaining pairs, schedule the BFS for the next batch
  // if (bfs_state->started_searches < gstate.global_pairs->Count()) {
  //   op.ScheduleBFSEvent(*pipeline, *this, gstate);
  // }
}

} // namespace core
} // namespace duckpgq

