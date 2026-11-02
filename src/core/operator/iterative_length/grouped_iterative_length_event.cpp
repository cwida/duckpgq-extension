#include "duckpgq/core/operator/iterative_length/grouped_iterative_length_event.hpp"

namespace duckdb {

GroupedIterativeLengthBatches::GroupedIterativeLengthBatches(vector<shared_ptr<IterativeLengthState>> states_p,
                                                             ClientContext &context_p, idx_t v_size_p,
                                                             idx_t threads_p, idx_t partitions_p, idx_t graph_edges_p,
                                                             idx_t initial_window_p, bool adaptive_p,
                                                             idx_t fixed_workers_p, idx_t oversubscription_p)
    : states(std::move(states_p)), context(context_p), v_size(v_size_p), threads(std::max<idx_t>(1, threads_p)),
      partitions(std::max<idx_t>(1, partitions_p)), graph_edges(std::max<idx_t>(1, graph_edges_p)),
      adaptive(adaptive_p), fixed_workers(fixed_workers_p), oversubscription(std::max<idx_t>(1, oversubscription_p)) {
	max_groups = std::max<idx_t>(1, std::min<idx_t>(states.size(), threads));
	window = std::max<idx_t>(1, std::min<idx_t>(initial_window_p, max_groups));
	converged = !adaptive || window == max_groups;
	window_start = std::chrono::steady_clock::now();
	trace = "window=" + std::to_string(window);
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

idx_t GroupedIterativeLengthBatches::SubRangeHint() {
	// Parallelism a query lacks when it has few batches on coarse partitions: threads / (partitions x batches)
	auto batch_slots = std::max<idx_t>(1, std::min<idx_t>(states.size(), threads));
	return std::max<idx_t>(1, (threads + partitions * batch_slots - 1) / (partitions * batch_slots));
}

idx_t GroupedIterativeLengthBatches::WorkersPerGroup() {
	lock_guard<mutex> guard(lock);
	idx_t workers = fixed_workers > 0 ? fixed_workers : std::max<idx_t>(1, threads / std::max<idx_t>(1, window));
	// Not capped by the partition count: a batch with more workers than partitions splits the partitions into
	// destination sub-ranges (IterativeLengthState::Start), which is what keeps coarse partitions parallel
	return std::max<idx_t>(1, std::min<idx_t>(workers * oversubscription, threads));
}

void GroupedIterativeLengthBatches::Decide(idx_t edges_now, std::chrono::steady_clock::time_point now) {
	// A decision waits until every live group has finished about one batch under the current window and the window
	// has swept the graph a few times, so neither a light level nor the start-up of freshly spawned groups decides
	if (converged || batches_done_in_window < window || edges_now - window_start_edges < 4 * graph_edges) {
		return;
	}
	auto seconds = std::chrono::duration<double>(now - window_start).count();
	if (seconds <= 0) {
		return;
	}
	double rate = static_cast<double>(edges_now - window_start_edges) / seconds;
	rate_at.emplace_back(window, rate);
	trace += " rate=" + std::to_string(static_cast<idx_t>(rate / 1000000)) + "M/s";
	double previous_rate = 0;
	for (auto &entry : rate_at) {
		if (entry.first == previous_window) {
			previous_rate = entry.second;
		}
	}
	if (previous_window != 0 && rate < 0.9 * previous_rate) {
		// The last doubling hurt: step back and stay
		window = previous_window;
		converged = true;
		trace += " < back to window=" + std::to_string(window);
	} else if (window < max_groups && (previous_window == 0 || rate > 1.1 * previous_rate)) {
		previous_window = window;
		window = std::min<idx_t>(max_groups, window * 2);
		trace += " > window=" + std::to_string(window);
	} else {
		converged = true;
		trace += " = hold";
	}
	window_start = now;
	window_start_edges = edges_now;
	batches_done_in_window = 0;
}

bool GroupedIterativeLengthBatches::OnBatchDone(idx_t &spawn_count) {
	lock_guard<mutex> guard(lock);
	spawn_count = 0;
	batches_done_in_window++;
	Decide(edges_scanned.load(std::memory_order_relaxed), std::chrono::steady_clock::now());
	auto remaining = next_batch.load() < states.size() ? states.size() - next_batch.load() : 0;
	if (remaining == 0 || live_groups > window) {
		// Nothing left, or the window shrank: this group ends (NoteGroupEnded runs when its kernel finishes)
		return false;
	}
	// This group continues; others are spawned up to the window, never more than the batches left to claim
	auto wanted = std::min<idx_t>(window, remaining);
	if (wanted > live_groups) {
		spawn_count = wanted - live_groups;
		live_groups += spawn_count;
	}
	return true;
}

string GroupedIterativeLengthBatches::Trace() const {
	return trace;
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
		current->edges_scanned = &batches->edges_scanned;
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
	idx_t spawn_count = 0;
	bool continues = batches->OnBatchDone(spawn_count);
	pending_spawns += spawn_count;
	return continues && StartNextBatch();
}

void GroupedIterativeLengthKernel::Finished() {
	if (current) {
		// Start() declined: the last claimed batch had nothing to run
		current->Finished();
		current.reset();
	}
	batches->ReleaseWorkingSet(std::move(working_set));
	batches->NoteGroupEnded();
}

void GroupedIterativeLengthKernel::CollectSpawned(vector<shared_ptr<PhasedKernel>> &spawned) {
	for (idx_t i = 0; i < pending_spawns; i++) {
		spawned.push_back(make_shared_ptr<GroupedIterativeLengthKernel>(batches));
	}
	pending_spawns = 0;
}

} // namespace duckdb
