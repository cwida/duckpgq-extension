#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/bfs_benchmark.hpp"
#include "duckpgq/core/operator/path_finding_batch.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

#include "duckdb/storage/buffer/buffer_handle.hpp"
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <chrono>

namespace duckdb {
class PhysicalPathFinding; // Forward declaration

//! The per-vertex lane bitsets (visit1, visit2, seen) of one running MS-BFS batch.
//!
//! The memory comes from the buffer manager so that it counts towards memory_limit, and a working set is handed from
//! batch to batch within a worker group, so the footprint scales with the number of concurrently running batches
//! rather than with the number of batches of the query. The arrays are never cleared when a set is allocated or when
//! a batch is done: every wave of a batch starts with a phase in which the kernel's workers zero their slice and fill
//! their slice of `seen` (BFSState::PrepareWorkingSetSlice), so the last batch of a group does not pay for a clear
//! nobody reads (29 Oct: a serial memset of 229 MB per batch at SF300 and 724 MB on cit-Patents was the lane kernel's
//! fixed cost for small searches, 66 ms per empty single pair).
class BFSWorkingSet {
public:
	BFSWorkingSet(ClientContext &context, idx_t v_size);

	//! Words of the frontier summary bitmap: one bit per vertex, set when any lane is active at the vertex
	static idx_t ActiveWordsFor(idx_t v_size) {
		return (v_size + 63) / 64 + 1;
	}
	static idx_t BytesFor(idx_t v_size) {
		return 3 * v_size * sizeof(std::bitset<LANE_LIMIT>) + ActiveWordsFor(v_size) * sizeof(uint64_t);
	}

	//! The vertex range of worker `worker` of `workers`, aligned to the 64-vertex words of the summary bitmap so that
	//! no two workers write the same word
	void Slice(idx_t worker, idx_t workers, idx_t &begin, idx_t &end) const;
	//! Zeroes visit1, visit2 and the summary bitmap of the vertices in [begin, end) and sets their seen line to
	//! `seen_mask` (a set bit for every lane that is not searching, so that the lane never propagates)
	void Prepare(idx_t begin, idx_t end, const std::bitset<LANE_LIMIT> &seen_mask);

	std::bitset<LANE_LIMIT> *visit1 = nullptr;
	std::bitset<LANE_LIMIT> *visit2 = nullptr;
	std::bitset<LANE_LIMIT> *seen = nullptr;
	//! Frontier summary: bit v set when visit[v] (the frontier of the coming iteration) has any lane active, so the
	//! explore phase skips the 64-byte visit line of inactive sources (W31 step 3; the bitmap is 1/512 of visit)
	uint64_t *active_bitmap = nullptr;
	idx_t v_size;

private:
	BufferHandle handle;
};

//! The state of one MS-BFS batch (a chunk of up to STANDARD_VECTOR_SIZE pairs searched in waves of LANE_LIMIT lanes),
//! shared by the lane kernels: the lane bookkeeping, the result chunk, the working set and the benchmark settings.
class BFSState : public enable_shared_from_this<BFSState> {
public:
	BFSState(const shared_ptr<PathFindingBatch> &batch_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
	         idx_t num_threads_, string mode_, ClientContext &context_, int64_t vsize_);

	virtual ~BFSState();

	//! Creates the event that runs this batch. The caller registers it with the executor.
	virtual shared_ptr<Event> CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op);

	//! ---- A wave of lanes, in three steps around the kernel's prepare phase
	//! Serial: gives the next pairs a lane each (lane_to_num, lane_active, active) and answers the pairs that need
	//! no search (NULL or out-of-range endpoints, source equals destination); computes `seen_mask`
	virtual void AssignLanes();
	//! Parallel, one worker's slice: zeroes the frontier arrays and fills `seen` with `seen_mask`
	void PrepareWorkingSetSlice(idx_t worker, idx_t workers);
	//! Serial, after every slice is prepared: sets the source vertex of every assigned lane in visit1 and the bitmap
	virtual void MarkLaneSources();

	//! ---- After an iteration or a wave
	//! Lanes whose destination is now seen record `iter` as the length and stop; advances `iter` and clears `change`
	//! when no lane is active or the hop bound is reached
	void ReachDetect();
	//! Lanes still active found no path (within the bound): their result is NULL
	void UnReachableSet();
	//! Whether paths of `length` hops are the longest the query asks for (max_depth, 0 = unbounded)
	bool DepthBoundReached(int64_t length) const {
		return max_depth > 0 && length >= static_cast<int64_t>(max_depth);
	}

	//! Whether a pair endpoint can index the per-vertex lane arrays
	bool IsVertexInRange(int64_t vertex) const {
		return vertex >= 0 && vertex < v_size;
	}
	virtual void Clear();

	//! Uses a working set shared with the other batches of a worker group
	void AttachWorkingSet(shared_ptr<BFSWorkingSet> set);
	//! Allocates a private working set unless one is attached; called by worker 0 when the batch starts running
	void EnsureWorkingSet();
	//! Drops the working set when the batch is done: a private set is freed, a shared one stays with its group
	void ReleaseWorkingSet();

	//! Appends the batch's row (label, pairs, graph size, time) to the phase timing file
	void AppendBatchPhaseTiming(const string &label, double time_ms) const;

	// Common members
	shared_ptr<PathFindingBatch> batch;
	shared_ptr<DataChunk> pairs;
	std::vector<shared_ptr<LocalCSR>> local_csrs;
	//! Index snapshot overlay, aligned with local_csrs (empty when traversing a plain CSR): delta edges of the
	//! partition's destination range and the epoch edge positions this reader must skip
	std::vector<shared_ptr<LocalCSR>> delta_csrs;
	std::vector<shared_ptr<vector<uint64_t>>> suppressed;
	atomic<int64_t> partition_counter;
	string mode;
	shared_ptr<DataChunk> pf_results;
	LogicalType bfs_type;
	int64_t iter;
	//! Stop after this many iterations (0 = unbounded); lanes still active are unreachable within the bound
	idx_t max_depth = 0;
	int64_t v_size;
	bool change;
	idx_t started_searches;
	const int64_t *src;
	Vector &src_data;
	Vector &dst_data;
	const int64_t *dst;
	int64_t lane_to_num[LANE_LIMIT];
	bitset<LANE_LIMIT> lane_active;
	//! The seen line of every vertex at the start of the wave: set for the lanes without a search
	bitset<LANE_LIMIT> seen_mask;

	//! Explore samples of the batch (benchmark only), appended under log_mutex
	mutex log_mutex;
	vector<ExploreTiming> timing_data;
	bool benchmark_enabled;
	bool benchmark_lane_activity_enabled;
	string benchmark_output_prefix;
	string benchmark_run_id;
	std::chrono::steady_clock::time_point phase_start_time;

	UnifiedVectorFormat vdata_src;
	UnifiedVectorFormat vdata_dst;
	idx_t active = 0;
	ClientContext &context;
	idx_t num_threads;
	atomic<int64_t> local_csr_counter;
	//! Per-vertex lane bitsets, valid between EnsureWorkingSet/AttachWorkingSet and ReleaseWorkingSet
	shared_ptr<BFSWorkingSet> working_set;
	bitset<LANE_LIMIT> *seen = nullptr;
	bitset<LANE_LIMIT> *visit1 = nullptr;
	bitset<LANE_LIMIT> *visit2 = nullptr;
	//! Frontier summary bitmap of the working set (one bit per vertex), see BFSWorkingSet
	uint64_t *active_bitmap = nullptr;
};

} // namespace duckdb
