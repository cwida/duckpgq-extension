#pragma once

#include "duckpgq/common.hpp"

#include <duckpgq/core/operator/bfs_state.hpp>
#include <duckpgq/core/operator/phased_kernel_event.hpp>
#include <duckdb/common/vector_size.hpp>

namespace duckdb {
class PhysicalPathFinding;

struct PushPullIterationStats {
	idx_t batch;
	idx_t iteration;
	string mode;
	idx_t active_lanes;
	idx_t frontier_vertices;
	idx_t vertex_count;
	idx_t pull_gate;
};

struct PushPullPhaseTiming {
	idx_t batch;
	idx_t iteration;
	string mode;
	string phase;
	idx_t worker_id;
	idx_t active_lanes;
	idx_t frontier_vertices;
	idx_t vertex_count;
	idx_t partition_count;
	idx_t vertices;
	idx_t edges;
	idx_t candidates;
	idx_t changed_vertices;
	double time_ms;
};

struct PushPullPullBlock {
	idx_t partition_idx;
	idx_t local_start;
	idx_t local_end;
};

class PushPullIterativeLengthWorker;

//! Push/pull MS-BFS as a phased kernel: PREPARE (frontier count and clear), then PUSH_EXPLORE + PUSH_CHECK or PULL,
//! and a serial reach detection between iterations
class PushPullIterativeLengthState : public BFSState, public PhasedKernel {
public:
	PushPullIterativeLengthState(const shared_ptr<PathFindingBatch> &batch_,
	                             std::vector<shared_ptr<LocalCSR>> &local_csrs_,
	                             std::vector<shared_ptr<PullCSR>> &pull_local_csrs_, idx_t num_threads_,
	                             ClientContext &context_, int64_t vsize_);

	shared_ptr<Event> CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op) override;
	~PushPullIterativeLengthState() override;

	bool Start() override;
	idx_t WorkerCount() const override;
	void RunPhase(idx_t worker_id) override;
	bool AdvancePhase() override;
	void Finished() override;
	void Clear() override;

	void WriteTimingResults(const std::string &filename);
	void WriteIterationStats(const std::string &filename);
	void WritePhaseTimingResults(const std::string &filename);

public:
	std::vector<shared_ptr<PullCSR>> pull_local_csrs;
	vector<PushPullIterationStats> iteration_stats;
	vector<vector<std::tuple<std::thread::id, int, double, idx_t, size_t, size_t, size_t, int64_t>>> worker_timing_data;
	vector<vector<PushPullPhaseTiming>> phase_timing_data;
	vector<PushPullPullBlock> pull_blocks;
	vector<idx_t> frontier_count_by_worker;
	atomic<int64_t> pull_block_counter;
	idx_t current_batch;
	idx_t frontier_vertices;
	idx_t pull_frontier_gate;
	bool use_pull;
	bool continue_search;
	bool has_more_batches;

private:
	enum class Phase : uint8_t { PREPARE, PUSH_EXPLORE, PUSH_CHECK, PULL };
	void BeginBatch();
	void BeginIteration();

	Phase phase = Phase::PREPARE;
	idx_t worker_count = 1;
	vector<unique_ptr<PushPullIterativeLengthWorker>> workers;
};

} // namespace duckdb
