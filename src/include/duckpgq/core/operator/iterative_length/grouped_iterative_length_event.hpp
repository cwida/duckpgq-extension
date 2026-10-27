#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"
#include "duckpgq/core/operator/phased_kernel_event.hpp"

#include <chrono>

namespace duckdb {

//! The batches of one query and the admission controller that decides how many worker groups run them at once.
//!
//! A worker group owns one working set (the lane bitsets, 192 B per vertex at 512 lanes) and runs batches one after
//! the other. The number of live groups is the window: it bounds the memory and the number of working sets the cores
//! stream concurrently. The controller grows the window (doubling) while the operator's throughput, edges swept per
//! second over all groups, keeps improving by more than a tenth, steps back once when it drops by more than a tenth,
//! and otherwise holds. Measured on 32 cores (W31, 27 Oct): at SF10 every doubling up to 32 groups pays (the
//! working sets are cache resident and the per-level barrier of a wide group is the cost), at SF100 the gain stops
//! between 8 and 16 groups when the memory system saturates, so the window settles there.
struct GroupedIterativeLengthBatches {
	GroupedIterativeLengthBatches(vector<shared_ptr<IterativeLengthState>> states_p, ClientContext &context_p,
	                              idx_t v_size_p, idx_t threads_p, idx_t partitions_p, idx_t graph_edges_p,
	                              idx_t initial_window_p, bool adaptive_p, idx_t fixed_workers_p,
	                              idx_t oversubscription_p);

	//! The next unclaimed batch, or nullptr
	shared_ptr<IterativeLengthState> Claim();
	//! A zeroed working set from the pool (allocating one when the pool is empty)
	shared_ptr<BFSWorkingSet> AcquireWorkingSet();
	void ReleaseWorkingSet(shared_ptr<BFSWorkingSet> set);
	//! Workers for a batch started now: threads / window, times the oversubscription factor, never more than the
	//! partitions
	idx_t WorkersPerGroup();
	//! Called by a group when its batch is done. Decides on the window, tells the group whether to run another
	//! batch, and returns the number of groups to spawn alongside it.
	bool OnBatchDone(idx_t &spawn_count);
	//! Groups to start when the operator begins
	idx_t InitialGroups() const {
		return std::min<idx_t>(window, states.size());
	}
	void NoteGroupStarted() {
		lock_guard<mutex> guard(lock);
		live_groups++;
	}
	//! Every kernel calls this once when it finishes, whether or not it ran a batch
	void NoteGroupEnded() {
		lock_guard<mutex> guard(lock);
		if (live_groups > 0) {
			live_groups--;
		}
	}
	//! "window=4 rate=1.2G/s > window=8 rate=2.1G/s ..." for the strategy string
	string Trace() const;

	vector<shared_ptr<IterativeLengthState>> states;
	atomic<idx_t> next_batch {0};
	//! Edges swept by every explore phase of every batch; the throughput clock
	atomic<idx_t> edges_scanned {0};

private:
	void Decide(idx_t edges_now, std::chrono::steady_clock::time_point now);

	ClientContext &context;
	idx_t v_size;
	idx_t threads;
	idx_t partitions;
	//! Edges of the graph: one level of one batch sweeps this many
	idx_t graph_edges;
	bool adaptive;
	idx_t fixed_workers;
	idx_t oversubscription;
	idx_t max_groups;

	mutex lock;
	idx_t window;
	idx_t previous_window = 0;
	idx_t live_groups = 0;
	bool converged = false;
	vector<shared_ptr<BFSWorkingSet>> pool;
	//! Throughput measured at each window visited (edges per second)
	vector<pair<idx_t, double>> rate_at;
	std::chrono::steady_clock::time_point window_start;
	idx_t window_start_edges = 0;
	//! Batches finished since the window was last changed; a decision waits for one per live group
	idx_t batches_done_in_window = 0;
	string trace;
};

//! One worker group: runs the batches it claims one after the other with the controller's worker count, sharing one
//! working set across them. A group is a phased kernel that delegates its phases to the batch it currently runs.
class GroupedIterativeLengthKernel : public PhasedKernel {
public:
	GroupedIterativeLengthKernel(shared_ptr<GroupedIterativeLengthBatches> batches_p);

	bool Start() override;
	idx_t WorkerCount() const override;
	void RunPhase(idx_t worker_id) override;
	bool AdvancePhase() override;
	void Finished() override;
	void CollectSpawned(vector<shared_ptr<PhasedKernel>> &spawned) override;

private:
	//! Claims batches until one has work to do; false when none is left or the controller shrank the window
	bool StartNextBatch();

	shared_ptr<GroupedIterativeLengthBatches> batches;
	idx_t workers = 1;
	shared_ptr<BFSWorkingSet> working_set;
	shared_ptr<IterativeLengthState> current;
	idx_t pending_spawns = 0;
};

} // namespace duckdb
