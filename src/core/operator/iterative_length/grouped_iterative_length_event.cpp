#include "duckpgq/core/operator/iterative_length/grouped_iterative_length_event.hpp"

namespace duckdb {

GroupedIterativeLengthBatches::GroupedIterativeLengthBatches(vector<shared_ptr<IterativeLengthState>> states_p,
                                                             ClientContext &context_p, idx_t v_size_p,
                                                             idx_t threads_p, idx_t partitions_p, idx_t graph_edges_p,
                                                             idx_t window_p, idx_t fixed_workers_p,
                                                             idx_t task_edges_p)
    : states(std::move(states_p)), context(context_p), v_size(v_size_p), threads(std::max<idx_t>(1, threads_p)),
      partitions(std::max<idx_t>(1, partitions_p)), graph_edges(std::max<idx_t>(1, graph_edges_p)),
      window(std::max<idx_t>(1, window_p)), fixed_workers(fixed_workers_p), task_edges(task_edges_p) {
}

shared_ptr<IterativeLengthState> GroupedIterativeLengthBatches::Claim() {
	auto batch_idx = next_batch.fetch_add(1);
	return batch_idx < states.size() ? states[batch_idx] : nullptr;
}

shared_ptr<BFSWorkingSet> GroupedIterativeLengthBatches::AcquireWorkingSet() {
	{
		lock_guard<mutex> guard(lock);
		if (!pool.empty()) {
			auto set = std::move(pool.back());
			pool.pop_back();
			return set;
		}
	}
	return make_shared_ptr<BFSWorkingSet>(context, v_size);
}

void GroupedIterativeLengthBatches::ReleaseWorkingSet(shared_ptr<BFSWorkingSet> set) {
	if (!set) {
		return;
	}
	lock_guard<mutex> guard(lock);
	pool.push_back(std::move(set));
}

idx_t GroupedIterativeLengthBatches::SubRangeHint() const {
	// Parallelism a query lacks when it has few batches on coarse partitions: threads / (partitions x batches)
	auto batch_slots = std::max<idx_t>(1, std::min<idx_t>(states.size(), threads));
	return std::max<idx_t>(1, (threads + partitions * batch_slots - 1) / (partitions * batch_slots));
}

idx_t GroupedIterativeLengthBatches::WorkersPerGroup() const {
	if (fixed_workers > 0) {
		return std::max<idx_t>(1, std::min<idx_t>(fixed_workers, threads));
	}
	// A group's share of the threads, each split into one task per task_edges edges of the graph: on a large graph
	// the level tasks of all groups pool in the executor, so a core waiting at one group's level barrier runs
	// another group's task (SF100 and SF300: 65536 to 4M edges per task within 2% at 32 groups, the share alone
	// 5-28% slower at SF300; at 8 and 4 groups 32 tasks beat 5-14 by 7-13%), while a small graph keeps its share
	// (SF10, 1.8M edges: one task per group at 32 groups, 1.5-1.8x faster than 2-29 tasks). Measured 3 Nov.
	idx_t share = std::max<idx_t>(1, threads / window);
	idx_t splits = task_edges > 0 ? (graph_edges + task_edges - 1) / task_edges : 1;
	// Not capped by the partition count: a batch with more tasks than partitions splits the partitions into
	// destination sub-ranges (IterativeLengthState::Start), which is what keeps coarse partitions parallel
	return std::max<idx_t>(1, std::min<idx_t>(share * splits, threads));
}

GroupedIterativeLengthKernel::GroupedIterativeLengthKernel(shared_ptr<GroupedIterativeLengthBatches> batches_p)
    : batches(std::move(batches_p)) {
}

bool GroupedIterativeLengthKernel::StartNextBatch() {
	while (true) {
		current = batches->Claim();
		if (!current) {
			return false;
		}
		if (!working_set) {
			working_set = batches->AcquireWorkingSet();
		}
		workers = batches->WorkersPerGroup();
		current->AttachWorkingSet(working_set);
		current->SetWorkerCount(workers);
		current->SetTimingLabel("bfs_batch_grouped");
		current->sub_range_hint = batches->SubRangeHint();
		if (current->Start()) {
			// The batch may have fewer tasks than the group's workers (partitions x sub-ranges)
			workers = current->WorkerCount();
			return true;
		}
		current->Finished();
	}
}

bool GroupedIterativeLengthKernel::Start() {
	return StartNextBatch();
}

idx_t GroupedIterativeLengthKernel::WorkerCount() const {
	return workers;
}

void GroupedIterativeLengthKernel::RunPhase(idx_t worker_id) {
	current->RunPhase(worker_id);
}

bool GroupedIterativeLengthKernel::AdvancePhase() {
	if (current->AdvancePhase()) {
		return true;
	}
	current->Finished();
	current.reset();
	return StartNextBatch();
}

void GroupedIterativeLengthKernel::Finished() {
	if (current) {
		// Start() declined: the last claimed batch had nothing to run
		current->Finished();
		current.reset();
	}
	batches->ReleaseWorkingSet(std::move(working_set));
}

} // namespace duckdb
