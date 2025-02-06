//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckpgq/core/operators/event/parallel_shortest_path_event.hpp
//
//
//===----------------------------------------------------------------------===//


#pragma once

#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "shortest_path_state.hpp"

namespace duckpgq {
namespace core {

class ShortestPathEvent : public BasePipelineEvent {
public:
  explicit ShortestPathEvent(shared_ptr<ShortestPathState> gbfs_state_p, Pipeline &pipeline_p, const PhysicalPathFinding& op_p);

  void Schedule() override;
  void FinishEvent() override;

private:
  shared_ptr<ShortestPathState> gbfs_state;
  const PhysicalPathFinding &op;
};

} // namespace core
} // namespace duckpgq