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
  // Printer::Print("Scheduling event");
  for (idx_t tnum = 0; tnum < local_csr_state->num_threads; tnum++) {
    csr_tasks.push_back(make_uniq<LocalCSRTask>(
        shared_from_this(), context, local_csr_state, tnum, op));
    local_csr_state->tasks_scheduled++;
  }
  local_csr_state->barrier = make_uniq<Barrier>(local_csr_state->tasks_scheduled);
  SetTasks(std::move(csr_tasks));
}

void LocalCSREvent::FinishEvent() {
  // Printer::Print("Finished Local CSR event");
}

} // namespace core
} // namespace duckpgq