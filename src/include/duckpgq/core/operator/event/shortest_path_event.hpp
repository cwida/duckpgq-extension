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

class ShortestPathEvent : public BasePipelineEvent {
public:
  explicit ShortestPathEvent(GlobalBFSState &gbfs_state_p, Pipeline &pipeline_p, const PhysicalPathFinding& op_p);

  void Schedule() override;

private:
  GlobalBFSState &gbfs_state;
  const PhysicalPathFinding &op;
};

} // namespace core
} // namespace duckpgq