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
namespace duckpgq {
namespace core {
class PathFindingGlobalState;

class ParallelShortestPathEvent : public BasePipelineEvent {
public:
  explicit ParallelShortestPathEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p, const PhysicalOperator& op_p);

  void Schedule() override;
  void FinishEvent() override;

private:
  PathFindingGlobalState &gstate;
  const PhysicalOperator &op;
};

} // namespace core
} // namespace duckpgq