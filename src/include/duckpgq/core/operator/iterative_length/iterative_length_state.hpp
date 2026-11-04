#pragma once
#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/bfs_state.hpp>
#include <duckpgq/core/operator/phased_kernel_event.hpp>

namespace duckdb {

class PhysicalPathFinding; // Forward declaration

//! One vertex a source reached and its distance: the unit of the reach mode's result (sources x all), 8 bytes so that
//! 1024 sources x all at SF300 (530M entries) hold in 4.2 GB
struct ReachedEntry {
	uint32_t vertex;
	int32_t distance;
};

//! REACH mode materializes every (vertex, distance) a source reached until the scatter emits them. The entries are
//! charged against this budget as they are recorded, so a result that does not fit fails the query with an
//! out-of-memory error instead of growing the process past memory_limit, which no buffer-manager accounting covers
//! (4 Nov: graph500-24, 1024 sources x all, 9.1 B entries = 73 GB of entries alone; the kernel killed the process).
//! Each entry is charged twice its size: the recorder's vectors grow by doubling, so their capacity is up to twice the
//! entries they hold. Shared by the waves and per-source searches of one query.
class ReachResultBudget {
public:
	explicit ReachResultBudget(idx_t cap_bytes_p) : cap_bytes(cap_bytes_p) {
	}
	static constexpr idx_t BYTES_PER_ENTRY = 2 * sizeof(ReachedEntry);
	//! Charges `entries` further entries; throws OutOfMemoryException when the total exceeds the cap (cap 0: none)
	void Charge(idx_t entries);
	idx_t ChargedEntries() const {
		return charged_entries.load();
	}

private:
	idx_t cap_bytes;
	atomic<idx_t> charged_entries {0};
};

//! Reach mode on the lane kernel (W29 on lane waves): what every lane of a wave newly reached, recorded in the check
//! phase by the worker that owns the vertex range. After CheckChange has masked `next` with the active lanes and
//! `~seen`, next[v] holds exactly the lanes that reached v at this level, so the worker appends (v, level) to its own
//! buffer of that lane: no lock, no per-entry allocation, and the entries are grouped by lane as they are written.
//! The collect phase at the end of the wave concatenates the workers' buffers of a lane into the lane's result.
struct LaneReachRecorder {
	//! buffers[worker][lane]: the entries worker `worker` recorded for `lane`, in level order
	vector<vector<vector<ReachedEntry>>> buffers;
	//! touched[worker]: lanes with new entries in the current level (DetectReached stops the untouched lanes)
	vector<std::bitset<LANE_LIMIT>> touched;
	//! The query's reach result budget (null: unbounded); pending[worker] entries recorded but not yet charged, charged
	//! in blocks of CHARGE_ENTRIES on the worker and at the end of every level
	shared_ptr<ReachResultBudget> budget;
	vector<idx_t> pending;
	static constexpr idx_t CHARGE_ENTRIES = 1 << 20;

	void Resize(idx_t workers) {
		buffers.assign(workers, vector<vector<ReachedEntry>>(LANE_LIMIT));
		touched.assign(workers, std::bitset<LANE_LIMIT>());
		pending.assign(workers, 0);
	}
	//! Charges the pending entries of every worker (serial step after a level)
	void ChargePending();
	//! Appends vertex `vertex` at `level` to every lane set in `lanes`, on worker `worker`
	void Record(idx_t worker, idx_t vertex, int32_t level, const std::bitset<LANE_LIMIT> &lanes);
	//! Lanes any worker touched in this level; clears the per-worker sets for the next level
	std::bitset<LANE_LIMIT> TakeTouched();
	//! Entries recorded for `lane` over all workers
	idx_t CountOf(idx_t lane) const;
	//! Appends the workers' entries of `lane` to `out` (worker order, each in level order) and frees them
	void Drain(idx_t lane, vector<ReachedEntry> &out);
};

//! MS-BFS over one batch of up to LANE_LIMIT pairs, written as a phased kernel: a wave opens with a PREPARE phase
//! (every worker zeroes its slice of the working set and fills its slice of `seen`), then every BFS iteration is an
//! EXPLORE phase (partitions expanded in parallel) followed by a CHECK phase (newly reached lanes folded into `seen`
//! per partition); the serial step after CHECK detects reached destinations and starts the next iteration or wave.
//! A reach wave ends with a COLLECT phase that gathers what every lane reached (LaneReachRecorder).
class IterativeLengthState : public BFSState, public PhasedKernel {
public:
	IterativeLengthState(const shared_ptr<PathFindingBatch> &batch_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
	                     idx_t num_threads_, ClientContext &context_, int64_t vsize_,
	                     const PathFindingSettings &settings);

	shared_ptr<Event> CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op) override;
	void Clear() override;

	//! ---- PhasedKernel
	bool Start() override;
	idx_t WorkerCount() const override;
	void RunPhase(idx_t worker_id) override;
	bool AdvancePhase() override;
	void Finished() override;

	//! Workers the kernel runs with; set before Start when a group shares its workers and working set
	void SetWorkerCount(idx_t workers);

	//! ---- Lane bookkeeping hooks (the lane-per-pair batch is the default; LaneWaveState gives every lane a source
	//! group with a set of destinations)
	//! After a check phase: which lanes reached what they were looking for; advances `iter`, clears `change` when no
	//! lane is active or the depth bound is hit
	virtual void DetectReached();
	//! After a wave ends: lanes still active found nothing (within the bound)
	virtual void MarkUnreachable();
	//! Whether pairs are left for another wave of this batch; false lets the finished batch skip the clear
	virtual bool HasNextWave();
	//! Label of the batch timing row ("bfs_batch" or "bfs_batch_grouped")
	void SetTimingLabel(const string &label);
	//! After the last wave: whether a COLLECT phase follows (reach lanes gather the recorded entries per lane in
	//! parallel there, not in the serial completion step: the output is the whole reached set)
	virtual bool StartCollect() {
		return false;
	}
	//! The parallel part of the COLLECT phase on one worker
	virtual void RunCollect(idx_t worker_id) {
	}

public:
	vector<uint8_t> worker_changed;
	//! experimental_path_finding_operator_frontier_bitmap: skip inactive sources through the summary bitmap
	bool use_frontier_bitmap = true;
	//! Test the bitmap per source on dense levels too: on when one working set no longer stays cache resident
	//! (SF100 1.2-1.6x faster), off when it does (SF10: 10% overhead) unless the option fixes it
	bool bitmap_filter = false;
	//! Applies the option: -1 decides from the size of one working set (on when it exceeds
	//! bitmap_filter_working_set, 32 MiB by default: SF10's 14 MB stays off, SF100's 41 MB goes on; measured 27 Oct:
	//! the test also pays for a single SF100 batch, so the total of the working sets is not the right measure)
	void SetBitmapFilter(const PathFindingSettings &settings) {
		if (settings.frontier_bitmap_filter >= 0) {
			bitmap_filter = settings.frontier_bitmap_filter > 0;
			return;
		}
		bitmap_filter = BFSWorkingSet::BytesFor(static_cast<idx_t>(v_size)) > settings.bitmap_filter_working_set;
	}
	//! experimental_path_finding_operator_visit_prefetch: visit lines prefetched this many sources ahead (0 off)
	idx_t visit_prefetch = 8;
	//! Sparse levels (W31 step 3): when few vertices have any lane active (count x divisor < V), the explore phase
	//! expands the active vertices by row lookup in each partition instead of sweeping the partition's source list;
	//! the tail of a wave, kept alive by a few long-distance lanes, runs ten or more such levels on LDBC knows
	idx_t sparse_level_divisor = 16;
	bool sparse_level = false;
	bool row_lookup_available = true;
	vector<uint32_t> frontier_list;
	vector<idx_t> worker_active_counts;
	//! Destination sub-ranges per partition in the parallel phases (1 = one task per partition); chosen in Start()
	idx_t sub_ranges = 1;
	//! experimental_path_finding_operator_sub_range_tasks
	bool sub_range_tasks = true;
	//! Sub-ranges the scheduler asks for (from the query's batch count); 0 derives from this batch's workers alone
	idx_t sub_range_hint = 0;
	//! Reach lanes: set by the reach wave state before Start; the check phase records every newly reached vertex of
	//! every lane into it (null for the destination checks of lengths)
	unique_ptr<LaneReachRecorder> reach_recorder;

private:
	enum class Phase : uint8_t { PREPARE, EXPLORE, CHECK, COLLECT };
	//! Assigns the lanes of the next wave and queues the PREPARE phase
	void BeginWave();
	void BeginIteration();
	void PrepareLevel(idx_t active_total);
	void SubRange(const LocalCSR &local_csr, idx_t sub_idx, idx_t &begin, idx_t &end) const;

	Phase phase = Phase::PREPARE;
	idx_t worker_count = 1;
	string timing_label = "bfs_batch";
};

} // namespace duckdb
