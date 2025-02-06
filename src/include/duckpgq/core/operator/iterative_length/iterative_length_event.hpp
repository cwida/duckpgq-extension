#pragma once

#include <duckdb/parallel/base_pipeline_event.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/operator/iterative_length/iterative_length_task.hpp>

namespace duckpgq {
namespace core {

class IterativeLengthEvent : public BasePipelineEvent {
public:
  IterativeLengthEvent(shared_ptr<IterativeLengthState> gbfs_state_p, Pipeline &pipeline_p, const PhysicalPathFinding& op_p);

  void Schedule() override;
  void FinishEvent() override;

private:
  shared_ptr<IterativeLengthState> gbfs_state;
  const PhysicalPathFinding &op;
};

} // namespace core
} // namespace duckpgq
