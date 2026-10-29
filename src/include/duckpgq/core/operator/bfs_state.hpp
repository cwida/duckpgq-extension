#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/path_finding_batch.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

#include "duckdb/storage/buffer/buffer_handle.hpp"
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <chrono>
#include <thread>

namespace duckdb {
class PhysicalPathFinding; // Forward declaration

//! The per-vertex lane bitsets (visit1, visit2, seen) of one running MS-BFS batch.
//!
//! The memory comes from the buffer manager so that it counts towards memory_limit, and a working set is handed from
//! batch to batch within a worker group, so the footprint scales with the number of concurrently running batches
//! rather than with the number of batches of the query. A working set is zero when allocated; a finished batch marks
//! it dirty and the next batch that attaches it zeroes it (29 Oct: a serial memset of 229 MB per batch at SF300 and
//! 724 MB on cit-Patents was the lane kernel's fixed cost for small searches, 66 ms per empty single pair; zeroing is
//! now parallel and skipped for the last batch of a group).
class BFSWorkingSet {
public:
	BFSWorkingSet(ClientContext &context, idx_t v_size, idx_t threads = 1);

	//! Zeroes every array with up to `threads` threads
	void Zero(idx_t threads);
	//! Set by a finished batch: the arrays hold its last frontier and seen bits
	bool dirty = false;

	//! Words of the frontier summary bitmap: one bit per vertex, set when any lane is active at the vertex
	static idx_t ActiveWordsFor(idx_t v_size) {
		return (v_size + 63) / 64 + 1;
	}
	static idx_t BytesFor(idx_t v_size) {
		return 3 * v_size * sizeof(std::bitset<LANE_LIMIT>) + ActiveWordsFor(v_size) * sizeof(uint64_t);
	}

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

class BFSState : public enable_shared_from_this<BFSState> {
public:
	BFSState(const shared_ptr<PathFindingBatch> &batch_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
	         idx_t num_threads_, string mode_, ClientContext &context_, int64_t vsize_);

	virtual ~BFSState();

	//! Creates the event that runs this batch. The caller registers it with the executor.
	virtual shared_ptr<Event> CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op);
	virtual void InitializeLanes();
	//! seen[v] = seen_mask for every vertex, in parallel
	void FillSeen(const std::bitset<LANE_LIMIT> &seen_mask);
	//! Whether a pair endpoint can index the per-vertex lane arrays
	bool IsVertexInRange(int64_t vertex) const {
		return vertex >= 0 && vertex < v_size;
	}
	virtual void Clear();
	void CreateThreadLocalCSRs(); // Generates LocalCSRs

	//! Uses a working set shared with the other batches of a worker group
	void AttachWorkingSet(shared_ptr<BFSWorkingSet> set);
	//! Allocates a private working set unless one is attached; called by worker 0 when the batch starts running
	void EnsureWorkingSet();
	//! Drops the working set when the batch is done: a private set is freed, a shared one stays with its group
	void ReleaseWorkingSet();

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

	mutex log_mutex;
	std::vector<std::tuple<std::thread::id, int, double, idx_t, size_t, size_t, size_t, int64_t>>
	    timing_data; // (Thread ID, Core ID, Time in ms)
	bool benchmark_enabled;
	bool benchmark_lane_activity_enabled;
	string benchmark_output_prefix;
	string benchmark_run_id;
	std::chrono::steady_clock::time_point phase_start_time;

	UnifiedVectorFormat vdata_src;
	UnifiedVectorFormat vdata_dst;
	idx_t active = 0;
	ClientContext &context;
	idx_t total_pairs_processed;
	idx_t num_threads;
	mutex change_lock;
	mutex local_csr_lock;
	atomic<int64_t> local_csr_counter;
	size_t current_batch_path_list_len;
	//! Per-vertex lane bitsets, valid between EnsureWorkingSet/AttachWorkingSet and ReleaseWorkingSet
	shared_ptr<BFSWorkingSet> working_set;
	bitset<LANE_LIMIT> *seen = nullptr;
	bitset<LANE_LIMIT> *visit1 = nullptr;
	bitset<LANE_LIMIT> *visit2 = nullptr;
	//! Frontier summary bitmap of the working set (one bit per vertex), see BFSWorkingSet
	uint64_t *active_bitmap = nullptr;

	idx_t tasks_scheduled;
	bitset<LANE_LIMIT> lane_completed;
};

} // namespace duckdb
