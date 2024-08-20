#pragma once

#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

class PhysicalShortestPathTask : public ExecutorTask {
public:
  PhysicalShortestPathTask(shared_ptr<Event> event_p, ClientContext &context,
                           PathFindingGlobalState &state, idx_t worker_id);

  TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
  void IterativePath();

  void ReachDetect();

  void PathConstruction();

  ClientContext &context;
  PathFindingGlobalState &state;
  // [left, right)
  idx_t left;
  idx_t right;
  idx_t worker_id;
};


} // namespace core
} // namespace duckpgq