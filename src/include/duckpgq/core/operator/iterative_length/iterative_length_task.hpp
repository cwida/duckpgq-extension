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
  void CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen,
                   std::vector<std::bitset<LANE_LIMIT>> &next,
                   std::pair<idx_t, idx_t> &partition_range) const;
  void UnReachableSet() const;

  double Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
    std::vector<std::bitset<LANE_LIMIT>> &next,
    const std::vector<uint64_t> &v,
    const std::vector<uint16_t> &e,
    size_t v_size, idx_t v_offset);

  void RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                std::vector<std::bitset<LANE_LIMIT>> &next,
                const std::vector<uint64_t> &v, const std::vector<uint16_t> &e, size_t v_size, idx_t v_offset);
private:
  ClientContext &context;
  shared_ptr<IterativeLengthState> &state;
  idx_t worker_id;
  bool explore_done; // Flag to indicate task completion
};


} // namespace core
} // namespace duckpgq