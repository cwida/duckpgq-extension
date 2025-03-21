#pragma once

#include <duckdb/parallel/base_pipeline_event.hpp>
#include <duckpgq/core/operator/local_csr/local_csr_task.hpp>

namespace duckpgq {
namespace core {

class LocalCSREvent : public BasePipelineEvent {
public:
  LocalCSREvent(shared_ptr<LocalCSRState> local_csr_state_p, Pipeline &pipeline_p, const PhysicalPathFinding& op_p, ClientContext &context_p);

  void Schedule() override;
  void FinishEvent() override;

private:
  ClientContext &context;
  shared_ptr<LocalCSRState> local_csr_state;
  const PhysicalPathFinding &op;
};

} // namespace core
} // namespace duckpgq
