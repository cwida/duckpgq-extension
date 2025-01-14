#pragma once

#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/bfs_state.hpp>

namespace duckpgq {
namespace core {

class ShortestPathTask : public ExecutorTask {
public:
  ShortestPathTask(shared_ptr<Event> event_p, ClientContext &context,
                           shared_ptr<BFSState> &state, idx_t worker_id,
                           const PhysicalOperator &op_p);

  TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
  void IterativePath();

  void ReachDetect();

  void PathConstruction();


  ClientContext &context;
  shared_ptr<BFSState> &state;
  idx_t worker_id;
};


} // namespace core
} // namespace duckpgq