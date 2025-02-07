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
  for (idx_t tnum = 0; tnum < gbfs_state->local_csrs.size(); tnum++) {
    bfs_tasks.push_back(make_uniq<IterativeLengthTask>(
        shared_from_this(), context, gbfs_state, tnum, op, std::move(gbfs_state->local_csrs[tnum])));
  }
  SetTasks(std::move(bfs_tasks));
}

void IterativeLengthEvent::FinishEvent() {
}

} // namespace core
} // namespace duckpgq

