#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"
#include "duckpgq/core/operator/phased_kernel_event.hpp"

namespace duckdb {

//! The batches of one query and the worker groups that run them: a group owns one working set (the lane bitsets,
//! 3 x LANE_LIMIT / 8 bytes per vertex) and runs the batches it claims from the queue one after the other, so at
//! most `window` working sets are live. The window is min(batches, threads), bounded by the working-set cap; a
//! group's phases run as `WorkersPerGroup()` tasks: its share of the threads (threads / window), each split into
//! one task per `task_edges` edges of the graph, at most the threads in all, so the level tasks of all groups fill
//! the executor on a large graph while a small graph (SF10, 1.8M edges) keeps one task per group and no barrier
//! spin at its short levels (W31).
struct GroupedIterativeLengthBatches {
	GroupedIterativeLengthBatches(vector<shared_ptr<IterativeLengthState>> states_p, ClientContext &context_p,
	                              idx_t v_size_p, idx_t threads_p, idx_t partitions_p, idx_t graph_edges_p,
	                              idx_t window_p, idx_t fixed_workers_p, idx_t task_edges_p);

	//! The next unclaimed batch, or nullptr
	shared_ptr<IterativeLengthState> Claim();
	//! A zeroed working set from the pool (allocating one when the pool is empty)
	shared_ptr<BFSWorkingSet> AcquireWorkingSet();
	void ReleaseWorkingSet(shared_ptr<BFSWorkingSet> set);
	//! Tasks per phase of one group: the fixed count when set, else threads / window x ceil(graph_edges / task_edges)
	//! capped at the threads
	idx_t WorkersPerGroup() const;
	//! Destination sub-ranges per partition a batch should use so that the query's batches together fill the threads
	idx_t SubRangeHint() const;
	//! Worker groups to start: the window, never more than there are batches
	idx_t Groups() const {
		return std::max<idx_t>(1, std::min<idx_t>(window, states.size()));
	}

	vector<shared_ptr<IterativeLengthState>> states;
	atomic<idx_t> next_batch {0};

private:
	ClientContext &context;
	idx_t v_size;
	idx_t threads;
	idx_t partitions;
	//! Edges of the graph: one level of one batch sweeps this many
	idx_t graph_edges;
	idx_t window;
	//! experimental_path_finding_operator_threads_per_batch (0: the rule) and _task_edges (0: no split by edges)
	idx_t fixed_workers;
	idx_t task_edges;

	mutex lock;
	vector<shared_ptr<BFSWorkingSet>> pool;
};

//! One worker group: runs the batches it claims one after the other, sharing one working set across them. A group
//! is a phased kernel that delegates its phases to the batch it currently runs.
class GroupedIterativeLengthKernel : public PhasedKernel {
public:
	explicit GroupedIterativeLengthKernel(shared_ptr<GroupedIterativeLengthBatches> batches_p);

	bool Start() override;
	idx_t WorkerCount() const override;
	void RunPhase(idx_t worker_id) override;
	bool AdvancePhase() override;
	void Finished() override;

private:
	//! Claims batches until one has work to do; false when none is left
	bool StartNextBatch();

	shared_ptr<GroupedIterativeLengthBatches> batches;
	idx_t workers = 1;
	shared_ptr<BFSWorkingSet> working_set;
	shared_ptr<IterativeLengthState> current;
};

} // namespace duckdb
