#pragma once

#include "duckpgq/common.hpp"

#include <duckpgq/core/operator/bfs_state.hpp>
#include <duckpgq/core/operator/phased_kernel_event.hpp>

namespace duckdb {
class PhysicalPathFinding; // Forward declaration

struct BidirectionalPhaseTiming {
	idx_t batch_id;
	idx_t step_id;
	string side;
	string phase;
	idx_t worker_id;
	idx_t thread_count;
	idx_t active_lanes;
	int64_t src_depth;
	int64_t dst_depth;
	idx_t src_frontier_size;
	idx_t dst_frontier_size;
	idx_t frontier_vertices;
	idx_t partitions;
	idx_t vertices;
	idx_t edges;
	idx_t new_frontier_count;
	idx_t completed_lanes;
	double time_ms;
};

enum class BidirectionalSearchSide : uint8_t { SOURCE, DESTINATION };

class BidirectionalIterativeLengthWorker;

//! Bidirectional MS-BFS as a phased kernel: each step expands one side in a CLEAR, EXPLORE and CHECK phase, with the
//! serial preparation and aggregation between them
class BidirectionalIterativeLengthState : public BFSState, public PhasedKernel {
public:
	BidirectionalIterativeLengthState(const shared_ptr<PathFindingBatch> &batch_,
	                                  std::vector<shared_ptr<LocalCSR>> &local_csrs_,
	                                  std::vector<shared_ptr<LocalCSR>> &reverse_local_csrs_, idx_t num_threads_,
	                                  ClientContext &context_, int64_t vsize_);

	shared_ptr<Event> CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op) override;
	~BidirectionalIterativeLengthState() override;

	bool Start() override;
	idx_t WorkerCount() const override;
	void RunPhase(idx_t worker_id) override;
	bool AdvancePhase() override;
	void Finished() override;
	void InitializeBidirectionalLanes();
	void Clear() override;

	void WriteTimingResults(const std::string &filename);
	void WritePhaseTimingResults(const std::string &filename);

public:
	std::vector<shared_ptr<LocalCSR>> reverse_local_csrs;

	vector<bitset<LANE_LIMIT>> src_seen;
	vector<bitset<LANE_LIMIT>> src_visit1;
	vector<bitset<LANE_LIMIT>> src_visit2;
	vector<bitset<LANE_LIMIT>> dst_seen;
	vector<bitset<LANE_LIMIT>> dst_visit1;
	vector<bitset<LANE_LIMIT>> dst_visit2;

	vector<bitset<LANE_LIMIT>> worker_meet_masks;
	vector<idx_t> worker_frontier_counts;
	vector<vector<idx_t>> worker_frontier_vertices;
	vector<vector<uint64_t>> worker_candidate_words;
	vector<vector<idx_t>> worker_dirty_candidate_words;
	vector<idx_t> src_frontier_vertices;
	vector<idx_t> dst_frontier_vertices;
	mutex phase_timing_lock;
	vector<BidirectionalPhaseTiming> bidirectional_phase_timing_data;
	idx_t current_batch_id;
	idx_t current_step_id;
	idx_t candidate_word_count;
	idx_t candidate_dirty_word_count;
	bool use_candidate_check;
	int64_t src_depth;
	int64_t dst_depth;
	idx_t src_frontier_size;
	idx_t dst_frontier_size;
	bool last_side_changed;
	bool has_more_batches;
	bool continue_search;
	bool expand_source_next;

private:
	enum class Phase : uint8_t { CLEAR, EXPLORE, CHECK };
	//! Starts the next step of the current batch
	bool BeginStep();
	//! Finishes the current batch and starts the next one that has work; false when none is left
	bool FinishBatch();

	Phase phase = Phase::CLEAR;
	BidirectionalSearchSide side = BidirectionalSearchSide::SOURCE;
	idx_t worker_count = 1;
	vector<unique_ptr<BidirectionalIterativeLengthWorker>> workers;
};

} // namespace duckdb
