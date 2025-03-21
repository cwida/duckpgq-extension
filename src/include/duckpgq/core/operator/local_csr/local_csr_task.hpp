#pragma once

#include "duckpgq/common.hpp"
#include "local_csr_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

class LocalCSRTask : public ExecutorTask {
public:
  LocalCSRTask(shared_ptr<Event> event_p, ClientContext &context,
                           shared_ptr<LocalCSRState> &state,
                           const PhysicalOperator &op_p,
                           shared_ptr<LocalCSR> &input_csr_p);

  TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;
  void PartitionGraph();
  void FillLocalCSR();

public:
  shared_ptr<LocalCSRState> &local_csr_state;
  shared_ptr<LocalCSR> &input_csr;
  shared_ptr<LocalCSR> output_csr;


};


} // namespace core
} // namespace duckpgq
