#pragma once

#include "duckpgq/common.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

class PhysicalIterativeTask : public ExecutorTask {
public:
  PhysicalIterativeTask(shared_ptr<Event> event_p, ClientContext &context,
                        PathFindingGlobalState &state, idx_t worker_id);

  TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;
private:
  void IterativeLength() const;
  void ReachDetect() const;
  void UnReachableSet() const;

private:
  ClientContext &context;
  PathFindingGlobalState &state;
  // [left, right)
  idx_t left;
  idx_t right;
  idx_t worker_id;
};


} // namespace core
} // namespace duckpgq