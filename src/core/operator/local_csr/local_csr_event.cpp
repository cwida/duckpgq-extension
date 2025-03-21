#include "duckpgq/core/operator/local_csr/local_csr_event.hpp"

#include <duckpgq/core/option/duckpgq_option.hpp>

namespace duckpgq {
namespace core {

LocalCSREvent::LocalCSREvent(shared_ptr<LocalCSRState> local_csr_state_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p, ClientContext &context_p)
    : BasePipelineEvent(pipeline_p), local_csr_state(std::move(local_csr_state_p)), op(op_p), context(context_p) {
}

void LocalCSREvent::Schedule() {
  auto &context = pipeline->GetClientContext();
  vector<shared_ptr<Task>> csr_tasks;

  while (!local_csr_state->local_csrs_to_partition.empty()) {
    auto input_csr = std::move(local_csr_state->local_csrs_to_partition.back());
    local_csr_state->local_csrs_to_partition.pop_back();  // Remove from the list
    csr_tasks.push_back(make_uniq<LocalCSRTask>(
      shared_from_this(), context, local_csr_state, op, input_csr));
  }
  SetTasks(std::move(csr_tasks));
}

void LocalCSREvent::FinishEvent() {
 if (!local_csr_state->local_csrs_to_partition.empty()) {
   Schedule();
 }

 std::sort(local_csr_state->local_csrs.begin(), local_csr_state->local_csrs.end(),
              [](const shared_ptr<LocalCSR>& a, const shared_ptr<LocalCSR>& b) {
                  return a->GetEdgeSize() > b->GetEdgeSize();  // Sort by edge count
              });
}




} // namespace core
} // namespace duckpgq