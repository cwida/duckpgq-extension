//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckpgq/core/operators/event/parallel_shortest_path_event.hpp
//
//
//===----------------------------------------------------------------------===//


#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

namespace duckpgq {
namespace core {
class PathFindingGlobalState;

class ParallelShortestPathEvent : public BasePipelineEvent {
public:
  explicit ParallelShortestPathEvent(PathFindingGlobalSinkState &gstate_p, Pipeline &pipeline_p, const PhysicalPathFinding& op_p);

  void Schedule() override;
  void FinishEvent() override;

private:
  PathFindingGlobalSinkState &gstate;
  const PhysicalPathFinding &op;
};

} // namespace core
} // namespace duckpgq