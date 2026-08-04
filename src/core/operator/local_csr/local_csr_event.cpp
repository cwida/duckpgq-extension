#include "duckpgq/core/operator/local_csr/local_csr_event.hpp"

namespace duckdb {

LocalCSREvent::LocalCSREvent(shared_ptr<LocalCSRState> local_csr_state_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p, ClientContext &context_p)
    : BasePipelineEvent(pipeline_p), local_csr_state(std::move(local_csr_state_p)), op(op_p), context(context_p) {
}

void LocalCSREvent::Schedule() {
  auto &context = pipeline->GetClientContext();
  vector<shared_ptr<Task>> csr_tasks;
  for (idx_t tnum = 0; tnum < local_csr_state->num_threads; tnum++) {
    csr_tasks.push_back(make_uniq<LocalCSRTask>(
        shared_from_this(), context, local_csr_state, tnum, op));
    local_csr_state->tasks_scheduled++;
  }
  local_csr_state->barrier = make_uniq<Barrier>(local_csr_state->tasks_scheduled);
  SetTasks(std::move(csr_tasks));
}

void LocalCSREvent::FinishEvent() {
  // Assume at least one partition exists
  D_ASSERT(!local_csr_state->partition_csrs.empty());

  std::sort(local_csr_state->partition_csrs.begin(), local_csr_state->partition_csrs.end(),
          [](const shared_ptr<LocalCSR>& a, const shared_ptr<LocalCSR>& b) {
              return a->GetEdgeSize() > b->GetEdgeSize();  // Sort by edge count
          });

}

} // namespace duckdb
