#pragma once

#include "duckpgq/common.hpp"

#include <duckdb/parallel/base_pipeline_event.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {
class CSREdgeCreationEvent : public BasePipelineEvent {
public:
  CSREdgeCreationEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p, const PhysicalOperator &op_p);
  PathFindingGlobalState &gstate;
  const PhysicalOperator &op;

  void Schedule() override;
  void FinishEvent() override;
};



} // namespace core
} // namespace duckpgq