#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/iterative_length/bidirectional_iterative_length_state.hpp"

namespace duckdb {

//! The per-worker phases of the bidirectional MS-BFS; the state (a PhasedKernel) sequences them
class BidirectionalIterativeLengthWorker {
public:
	BidirectionalIterativeLengthWorker(shared_ptr<BidirectionalIterativeLengthState> state_p, idx_t worker_id_p);

	//! Parallel phases of one step
	void ClearPhase(BidirectionalSearchSide side);
	void ExplorePhase(BidirectionalSearchSide side);
	void CheckPhase(BidirectionalSearchSide side);
	//! Serial steps, run through worker 0
	void PrepareSide(BidirectionalSearchSide side);
	void PrepareExplore(BidirectionalSearchSide side);
	void PrepareCheck(BidirectionalSearchSide side);
	void Aggregate(BidirectionalSearchSide side);
	void UnReachableSet() const;
	void RecordGlobalPhaseTiming(idx_t batch_id, idx_t step_id, const string &phase, idx_t completed_lanes,
	                             double time_ms) const;

private:
	struct SideRefs {
		std::vector<shared_ptr<LocalCSR>> &partition_csrs;
		std::vector<std::bitset<LANE_LIMIT>> &side_seen;
		std::vector<std::bitset<LANE_LIMIT>> &other_seen;
		int64_t &side_depth;
		idx_t &side_frontier_size;
		std::vector<idx_t> &frontier_vertices;
		std::vector<std::bitset<LANE_LIMIT>> &visit;
		std::vector<std::bitset<LANE_LIMIT>> &next;
	};
	SideRefs Refs(BidirectionalSearchSide side) const;
	void CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen, std::vector<std::bitset<LANE_LIMIT>> &next,
	                 std::vector<std::bitset<LANE_LIMIT>> &other_seen, shared_ptr<LocalCSR> &local_csr) const;
	idx_t CheckCandidateChange(std::vector<std::bitset<LANE_LIMIT>> &seen, std::vector<std::bitset<LANE_LIMIT>> &next,
	                           std::vector<std::bitset<LANE_LIMIT>> &other_seen, shared_ptr<LocalCSR> &local_csr) const;
	idx_t CompleteFoundLanes(std::bitset<LANE_LIMIT> found_lanes, int64_t path_length) const;
	void RecordPhaseTiming(idx_t batch_id, idx_t step_id, BidirectionalSearchSide side, const string &phase,
	                       idx_t frontier_vertices, idx_t partitions, idx_t vertices, idx_t edges,
	                       idx_t new_frontier_count, idx_t completed_lanes, double time_ms) const;
	idx_t Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit, std::vector<std::bitset<LANE_LIMIT>> &next,
	              const LocalCSR &local_csr, const std::vector<idx_t> &frontier_vertices);
	idx_t RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit, std::vector<std::bitset<LANE_LIMIT>> &next,
	                 const LocalCSR &local_csr, const std::vector<idx_t> &frontier_vertices);

	shared_ptr<BidirectionalIterativeLengthState> state;
	idx_t worker_id;
};

} // namespace duckdb
