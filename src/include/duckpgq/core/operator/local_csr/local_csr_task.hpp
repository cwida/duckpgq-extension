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
                           idx_t worker_id,
                           const PhysicalOperator &op_p);

  TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

  void CreateStatistics() const;
  void DeterminePartitions() const;
  void CountOutgoingEdgesPerPartition();
  idx_t GetPartitionForVertex(idx_t vertex) const;
  void CreateRunningSum() const;
  void DistributeEdges();

  shared_ptr<LocalCSRState> &local_csr_state;
  idx_t worker_id;

};

} // namespace core
} // namespace duckpgq
