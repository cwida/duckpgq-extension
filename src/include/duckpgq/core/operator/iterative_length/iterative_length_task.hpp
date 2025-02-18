#pragma once

#include "duckpgq/common.hpp"
#include "iterative_length_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

class IterativeLengthTask : public ExecutorTask {
public:
  IterativeLengthTask(shared_ptr<Event> event_p, ClientContext &context,
                           shared_ptr<IterativeLengthState> &state, idx_t worker_id,
                           const PhysicalOperator &op_p);

  TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;
private:
  void IterativeLength();
  void ReachDetect() const;
  void CheckChange(vector<std::bitset<LANE_LIMIT>> &seen,
                    vector<std::bitset<LANE_LIMIT>> &next) const;
  void UnReachableSet() const;

  void Explore(vector<std::bitset<LANE_LIMIT>> &visit,
    vector<std::bitset<LANE_LIMIT>> &next,
    vector<int64_t> &v,
    vector<int64_t> &e);

private:
  ClientContext &context;
  shared_ptr<IterativeLengthState> &state;
  idx_t worker_id;
};


} // namespace core
} // namespace duckpgq