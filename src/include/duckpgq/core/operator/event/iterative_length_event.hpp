#pragma once

#include "duckpgq/common.hpp"

#include <duckdb/parallel/base_pipeline_event.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/operator/task/iterative_length_task.hpp>

namespace duckpgq {
namespace core {

class ParallelIterativeEvent : public BasePipelineEvent {
public:
  ParallelIterativeEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p, const PhysicalPathFinding &op_p);

  PathFindingGlobalState &gstate;
  const PhysicalPathFinding &op;

  void Schedule() override;

  void FinishEvent() override;
};

} // namespace core
} // namespace duckpgq
