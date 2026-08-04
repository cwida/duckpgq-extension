#include "duckpgq/core/operator/iterative_length/iterative_length_event.hpp"

namespace duckdb {

IterativeLengthEvent::IterativeLengthEvent(shared_ptr<IterativeLengthState> gbfs_state_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
    : BasePipelineEvent(pipeline_p), gbfs_state(std::move(gbfs_state_p)), op(op_p) {

}


void IterativeLengthEvent::Schedule() {
  auto &context = pipeline->GetClientContext();
  vector<shared_ptr<Task>> bfs_tasks;
  idx_t num_partitions = gbfs_state->local_csrs.size();
  for (idx_t tnum = 0; tnum < std::min(gbfs_state->num_threads, num_partitions); tnum++) {
    bfs_tasks.push_back(make_uniq<IterativeLengthTask>(
        shared_from_this(), context, gbfs_state, tnum, op));
    gbfs_state->tasks_scheduled++;
  }
  gbfs_state->barrier = make_uniq<Barrier>(gbfs_state->tasks_scheduled);
  SetTasks(std::move(bfs_tasks));
}

void IterativeLengthEvent::FinishEvent() {
}

} // namespace duckdb
