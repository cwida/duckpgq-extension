#pragma once

#include "bidirectional_iterative_length_state.hpp"
#include "duckpgq/common.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckdb {

enum class BidirectionalSearchSide : uint8_t { SOURCE, DESTINATION };

class BidirectionalIterativeLengthTask : public ExecutorTask {
public:
	BidirectionalIterativeLengthTask(shared_ptr<Event> event_p, ClientContext &context,
	                                 shared_ptr<BidirectionalIterativeLengthState> &state, idx_t worker_id,
	                                 const PhysicalOperator &op_p);

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	void ExpandSide(BidirectionalSearchSide side);
	void CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen, std::vector<std::bitset<LANE_LIMIT>> &next,
	                 std::vector<std::bitset<LANE_LIMIT>> &other_seen, shared_ptr<LocalCSR> &local_csr) const;
	idx_t CheckCandidateChange(std::vector<std::bitset<LANE_LIMIT>> &seen,
	                          std::vector<std::bitset<LANE_LIMIT>> &next,
	                          std::vector<std::bitset<LANE_LIMIT>> &other_seen,
	                          shared_ptr<LocalCSR> &local_csr) const;
	idx_t CompleteFoundLanes(std::bitset<LANE_LIMIT> found_lanes, int64_t path_length) const;
	void UnReachableSet() const;
	void RecordGlobalPhaseTiming(idx_t batch_id, idx_t step_id, const string &phase, idx_t completed_lanes,
	                             double time_ms) const;
	void RecordPhaseTiming(idx_t batch_id, idx_t step_id, BidirectionalSearchSide side, const string &phase,
	                       idx_t frontier_vertices, idx_t partitions, idx_t vertices, idx_t edges,
	                       idx_t new_frontier_count, idx_t completed_lanes, double time_ms) const;
	void TimedBarrier(idx_t batch_id, idx_t step_id, BidirectionalSearchSide side, const string &phase);

	idx_t Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit, std::vector<std::bitset<LANE_LIMIT>> &next,
	              const LocalCSR &local_csr, const std::vector<idx_t> &frontier_vertices);

	idx_t RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit, std::vector<std::bitset<LANE_LIMIT>> &next,
	                 const LocalCSR &local_csr, const std::vector<idx_t> &frontier_vertices);

private:
	ClientContext &context;
	shared_ptr<BidirectionalIterativeLengthState> &state;
	idx_t worker_id;
};

} // namespace duckdb
