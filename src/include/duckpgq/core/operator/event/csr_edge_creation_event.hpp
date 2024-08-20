#pragma once

#include "duckpgq/common.hpp"

#include <duckdb/parallel/base_pipeline_event.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {
class CSREdgeCreationEvent : public BasePipelineEvent {
public:
  CSREdgeCreationEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p);
  PathFindingGlobalState &gstate;

  void Schedule() override;
  void FinishEvent() override;
};



} // namespace core
} // namespace duckpgq