#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/path_finding_batch.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

#include "duckdb/storage/buffer/buffer_handle.hpp"
#include <duckpgq/core/utils/duckpgq_barrier.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <chrono>
#include <thread>

namespace duckdb {
class PhysicalPathFinding; // Forward declaration

//! The per-vertex lane bitsets (visit1, visit2, seen) of one running MS-BFS batch.
//!
//! The memory comes from the buffer manager so that it counts towards memory_limit, and a working set is handed from
//! batch to batch within a worker group, so the footprint scales with the number of concurrently running batches
//! rather than with the number of batches of the query. A working set is zero when allocated and every batch leaves it
//! zero again (the last step of a wave is Clear()).
class BFSWorkingSet {
public:
	BFSWorkingSet(ClientContext &context, idx_t v_size);

	static idx_t BytesFor(idx_t v_size) {
		return 3 * v_size * sizeof(std::bitset<LANE_LIMIT>);
	}

	std::bitset<LANE_LIMIT> *visit1 = nullptr;
	std::bitset<LANE_LIMIT> *visit2 = nullptr;
	std::bitset<LANE_LIMIT> *seen = nullptr;
	idx_t v_size;

private:
	BufferHandle handle;
};

class BFSState : public enable_shared_from_this<BFSState> {
public:
	BFSState(const shared_ptr<PathFindingBatch> &batch_, std::vector<shared_ptr<LocalCSR>> &local_csrs_, idx_t num_threads_,
	         string mode_, ClientContext &context_, int64_t vsize_);

	virtual ~BFSState();

	//! Creates the event that runs this batch. The caller registers it with the executor.
	virtual shared_ptr<Event> CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op);
	void InitializeLanes();
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
	atomic<int64_t> partition_counter;
	string mode;
	shared_ptr<DataChunk> pf_results;
	LogicalType bfs_type;
	int64_t iter;
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
	unique_ptr<Barrier> barrier;
	mutex change_lock;
	mutex local_csr_lock;
	atomic<int64_t> local_csr_counter;
	size_t current_batch_path_list_len;
	//! Per-vertex lane bitsets, valid between EnsureWorkingSet/AttachWorkingSet and ReleaseWorkingSet
	shared_ptr<BFSWorkingSet> working_set;
	bitset<LANE_LIMIT> *seen = nullptr;
	bitset<LANE_LIMIT> *visit1 = nullptr;
	bitset<LANE_LIMIT> *visit2 = nullptr;

	idx_t tasks_scheduled;
	bitset<LANE_LIMIT> lane_completed;
};

} // namespace duckdb
