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
	void CompleteFoundLanes(std::bitset<LANE_LIMIT> found_lanes, int64_t path_length) const;
	void UnReachableSet() const;

	void Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit, std::vector<std::bitset<LANE_LIMIT>> &next,
	             const std::atomic<uint32_t> *v, const std::vector<uint16_t> &e,
	             const std::vector<idx_t> &frontier_vertices, idx_t start_vertex);

	void RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit, std::vector<std::bitset<LANE_LIMIT>> &next,
	                const std::atomic<uint32_t> *v, const std::vector<uint16_t> &e,
	                const std::vector<idx_t> &frontier_vertices, idx_t start_vertex);

private:
	ClientContext &context;
	shared_ptr<BidirectionalIterativeLengthState> &state;
	idx_t worker_id;
};

} // namespace duckdb
