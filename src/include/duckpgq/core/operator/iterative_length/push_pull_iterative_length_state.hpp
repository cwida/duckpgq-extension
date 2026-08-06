#pragma once

#include "duckpgq/common.hpp"

#include <duckpgq/core/operator/bfs_state.hpp>

namespace duckdb {
class PhysicalPathFinding;

struct PushPullIterationStats {
	idx_t iteration;
	string mode;
	idx_t active_lanes;
	idx_t frontier_vertices;
	idx_t vertex_count;
	idx_t pull_gate;
};

class PushPullIterativeLengthState : public BFSState {
public:
	PushPullIterativeLengthState(const shared_ptr<DataChunk> &pairs_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
	                             std::vector<shared_ptr<LocalCSR>> &reverse_local_csrs_, idx_t num_threads_,
	                             ClientContext &context_, int64_t vsize_);

	void ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op) override;
	void Clear() override;

	void WriteTimingResults(const std::string &filename);
	void WriteIterationStats(const std::string &filename);

public:
	std::vector<shared_ptr<LocalCSR>> reverse_local_csrs;
	vector<PushPullIterationStats> iteration_stats;
	idx_t frontier_vertices;
	idx_t pull_frontier_gate;
	bool use_pull;
};

} // namespace duckdb
