#pragma once

#include "duckpgq/common.hpp"

#include <duckpgq/core/operator/bfs_state.hpp>

namespace duckdb {
class PhysicalPathFinding; // Forward declaration

class BidirectionalIterativeLengthState : public BFSState {
public:
	BidirectionalIterativeLengthState(const shared_ptr<DataChunk> &pairs_,
	                                  std::vector<shared_ptr<LocalCSR>> &local_csrs_,
	                                  std::vector<shared_ptr<LocalCSR>> &reverse_local_csrs_, idx_t num_threads_,
	                                  ClientContext &context_, int64_t vsize_);

	void ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op) override;
	void InitializeBidirectionalLanes();
	void Clear() override;

	void WriteTimingResults(const std::string &filename);

public:
	std::vector<shared_ptr<LocalCSR>> reverse_local_csrs;

	vector<bitset<LANE_LIMIT>> src_seen;
	vector<bitset<LANE_LIMIT>> src_visit1;
	vector<bitset<LANE_LIMIT>> src_visit2;
	vector<bitset<LANE_LIMIT>> dst_seen;
	vector<bitset<LANE_LIMIT>> dst_visit1;
	vector<bitset<LANE_LIMIT>> dst_visit2;

	vector<bitset<LANE_LIMIT>> worker_meet_masks;
	int64_t src_depth;
	int64_t dst_depth;
	bool last_side_changed;
	bool has_more_batches;
	bool continue_search;
};

} // namespace duckdb
