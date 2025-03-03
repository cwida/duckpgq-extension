#include "duckpgq/core/operator/iterative_length/iterative_length_event.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

IterativeLengthEvent::IterativeLengthEvent(shared_ptr<IterativeLengthState> gbfs_state_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
    : BasePipelineEvent(pipeline_p), gbfs_state(std::move(gbfs_state_p)), op(op_p) {

}


void IterativeLengthEvent::Schedule() {
  auto &context = pipeline->GetClientContext();
  vector<shared_ptr<Task>> bfs_tasks;
  gbfs_state->CreateThreadLocalCSRs();
  for (idx_t tnum = 0; tnum < gbfs_state->num_threads; tnum++) {
    bfs_tasks.push_back(make_uniq<IterativeLengthTask>(
        shared_from_this(), context, gbfs_state, tnum, op));
  }
  SetTasks(std::move(bfs_tasks));
}

void IterativeLengthEvent::FinishEvent() {
  // gbfs_state->barrier->PrintTimingLogs();
}

} // namespace core
} // namespace duckpgq

