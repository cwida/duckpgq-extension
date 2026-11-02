#include "duckpgq/core/operator/iterative_length/push_pull_iterative_length_task.hpp"

namespace duckdb {

PushPullIterativeLengthWorker::PushPullIterativeLengthWorker(shared_ptr<PushPullIterativeLengthState> state_p,
                                                             idx_t worker_id_p)
    : state(std::move(state_p)), worker_id(worker_id_p) {
}

idx_t PushPullIterativeLengthWorker::CountFrontierVertices(const std::bitset<LANE_LIMIT> *visit, idx_t start_vertex,
                                                           idx_t end_vertex) const {
	idx_t frontier_vertices = 0;
	for (idx_t i = start_vertex; i < end_vertex; i++) {
		if ((visit[i] & state->lane_active).any()) {
			frontier_vertices++;
		}
	}
	return frontier_vertices;
}

//! INITIALIZE phase of a batch: this worker's slice of the working set is zeroed and its `seen` lines filled
void PushPullIterativeLengthWorker::InitializePhase() {
	PhaseTimer timer(state->benchmark_enabled);
	idx_t begin;
	idx_t end;
	state->working_set->Slice(worker_id, state->WorkerCount(), begin, end);
	state->working_set->Prepare(begin, end, state->seen_mask);
	RecordPhaseTiming("initialize_slice", "batch", static_cast<idx_t>(state->iter), 0, end - begin, 0, 0, 0,
	                  timer.ElapsedMs());
}

//! PREPARE phase: counts the frontier of this worker's vertex range and clears `next` for the partitions it claims;
//! the two touch different arrays, so they run as one phase
void PushPullIterativeLengthWorker::PreparePhase() {
	auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;
	auto iteration = static_cast<idx_t>(state->iter);

	auto workers = state->WorkerCount();
	idx_t vertices_per_worker = (static_cast<idx_t>(state->v_size) + workers - 1) / workers;
	idx_t vertex_start = worker_id * vertices_per_worker;
	idx_t vertex_end = std::min(vertex_start + vertices_per_worker, static_cast<idx_t>(state->v_size));
	PhaseTimer frontier_timer(state->benchmark_enabled);
	auto local_frontier_vertices = CountFrontierVertices(visit, vertex_start, vertex_end);
	state->frontier_count_by_worker[worker_id] = local_frontier_vertices;
	RecordPhaseTiming("frontier_count", "decide", iteration, 0, vertex_end - vertex_start, 0, local_frontier_vertices,
	                  0, frontier_timer.ElapsedMs());

	PhaseTimer clear_timer(state->benchmark_enabled);
	idx_t cleared_vertices = 0;
	idx_t cleared_partitions = 0;
	while (true) {
		auto partition_idx = state->partition_counter.fetch_add(1);
		if (partition_idx >= state->local_csrs.size()) {
			break;
		}
		auto &local_csr = state->local_csrs[partition_idx];
		for (auto i = local_csr->start_vertex; i < local_csr->end_vertex; i++) {
			next[i] = 0;
			cleared_vertices++;
		}
		cleared_partitions++;
	}
	RecordPhaseTiming("clear_next", "decide", iteration, cleared_partitions, cleared_vertices, 0, 0, 0,
	                  clear_timer.ElapsedMs());
}

//! Serial step after PREPARE: pull when the frontier is large enough; resets the shared cursors
void PushPullIterativeLengthWorker::DecideMode() {
	auto iteration = static_cast<idx_t>(state->iter);
	idx_t total_frontier_vertices = 0;
	PhaseTimer timer(state->benchmark_enabled);
	for (idx_t i = 0; i < state->WorkerCount(); i++) {
		total_frontier_vertices += state->frontier_count_by_worker[i];
	}
	state->frontier_vertices = total_frontier_vertices;
	state->use_pull = state->frontier_vertices * state->pull_frontier_gate >= static_cast<idx_t>(state->v_size);
	if (state->benchmark_enabled) {
		state->iteration_stats.push_back({state->current_batch, static_cast<idx_t>(state->iter),
		                                  state->use_pull ? "pull" : "push", state->active, state->frontier_vertices,
		                                  static_cast<idx_t>(state->v_size), state->pull_frontier_gate});
	}
	state->partition_counter = 0;
	state->local_csr_counter = 0;
	state->pull_block_counter = 0;
	RecordPhaseTiming("choose_mode", state->use_pull ? "pull" : "push", iteration, 0, state->WorkerCount(), 0,
	                  state->frontier_vertices, 0, timer.ElapsedMs());
}

idx_t PushPullIterativeLengthWorker::Explore(const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next,
                                             const LocalCSR &local_csr) {
	idx_t explored_edges = 0;
	const bool all_lanes_active = state->lane_active.all();
	if (local_csr.HasSparseRows()) {
		for (idx_t row_idx = 0; row_idx < local_csr.source_vertices.size(); row_idx++) {
			auto source_vertex = local_csr.source_vertices[row_idx];
			auto lanes = visit[source_vertex];
			if (!all_lanes_active) {
				lanes &= state->lane_active;
			}
			if (lanes.any()) {
				auto start_edges = local_csr.row_offsets[row_idx];
				auto end_edges = local_csr.row_offsets[row_idx + 1];
				explored_edges += end_edges - start_edges;
				for (auto offset = start_edges; offset < end_edges; offset++) {
					auto n = local_csr.e[offset] + local_csr.start_vertex;
					next[n] |= lanes;
				}
			}
		}
		return explored_edges;
	}

	for (idx_t i = 0; i < local_csr.GetVertexSize(); i++) {
		auto lanes = visit[i];
		if (!all_lanes_active) {
			lanes &= state->lane_active;
		}
		if (lanes.any()) {
			auto start_edges = local_csr.v[i].load(std::memory_order_relaxed);
			auto end_edges = local_csr.v[i + 1].load(std::memory_order_relaxed);
			explored_edges += end_edges - start_edges;
			for (auto offset = start_edges; offset < end_edges; offset++) {
				auto n = local_csr.e[offset] + local_csr.start_vertex;
				next[n] |= lanes;
			}
		}
	}
	return explored_edges;
}

idx_t PushPullIterativeLengthWorker::RunExplore(const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next,
                                                const LocalCSR &local_csr) {
	PhaseTimer timer(state->benchmark_enabled);
	auto explored_edges = Explore(visit, next, local_csr);
	if (!state->benchmark_enabled) {
		return 0;
	}
	RecordExploreTiming(state->worker_timing_data[worker_id], timer.ElapsedMs(), state->num_threads,
	                    local_csr.GetVertexSize(), explored_edges, state->local_csrs.size(), state->iter);
	return explored_edges;
}

void PushPullIterativeLengthWorker::PushExplore(idx_t iteration) {
	auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;

	PhaseTimer timer(state->benchmark_enabled);
	idx_t explored_partitions = 0;
	idx_t explored_vertices = 0;
	idx_t explored_edges = 0;
	while (true) {
		auto partition_idx = state->local_csr_counter.fetch_add(1);
		if (partition_idx >= state->local_csrs.size()) {
			break;
		}
		auto local_csr = state->local_csrs[partition_idx].get();
		if (!local_csr) {
			throw InternalException("Tried to reference nullptr for LocalCSR");
		}
		explored_edges += RunExplore(visit, next, *local_csr);
		explored_vertices += local_csr->GetVertexSize();
		explored_partitions++;
	}
	RecordPhaseTiming("push_explore", "push", iteration, explored_partitions, explored_vertices, explored_edges, 0, 0,
	                  timer.ElapsedMs());
}

void PushPullIterativeLengthWorker::PushCheck(idx_t iteration) {
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;
	PhaseTimer timer(state->benchmark_enabled);
	idx_t checked_partitions = 0;
	idx_t checked_vertices = 0;
	idx_t candidate_vertices = 0;
	idx_t changed_vertices = 0;
	while (true) {
		auto partition_idx = state->partition_counter.fetch_add(1);
		if (partition_idx >= state->local_csrs.size()) {
			break;
		}
		idx_t local_candidates = 0;
		changed_vertices += CheckChange(state->seen, next, state->local_csrs[partition_idx], local_candidates);
		candidate_vertices += local_candidates;
		checked_vertices +=
		    state->local_csrs[partition_idx]->end_vertex - state->local_csrs[partition_idx]->start_vertex;
		checked_partitions++;
	}
	RecordPhaseTiming("push_check", "push", iteration, checked_partitions, checked_vertices, 0, candidate_vertices,
	                  changed_vertices, timer.ElapsedMs());
}

void PushPullIterativeLengthWorker::Pull(idx_t iteration) {
	if (state->pull_local_csrs.empty()) {
		throw InternalException("Push/pull path finding requires pull CSR partitions.");
	}

	auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;
	bool local_change = false;

	PhaseTimer timer(state->benchmark_enabled);
	idx_t pulled_partitions = 0;
	idx_t scanned_vertices = 0;
	idx_t scanned_edges = 0;
	idx_t candidate_vertices = 0;
	idx_t changed_vertices = 0;
	while (true) {
		auto block_idx = state->pull_block_counter.fetch_add(1);
		if (block_idx >= static_cast<int64_t>(state->pull_blocks.size())) {
			break;
		}
		auto &pull_block = state->pull_blocks[block_idx];

		auto pull_csr = state->pull_local_csrs[pull_block.partition_idx].get();
		if (!pull_csr) {
			throw InternalException("Tried to reference nullptr for PullCSR");
		}

		pulled_partitions++;
		scanned_vertices += pull_block.local_end - pull_block.local_start;
		for (idx_t local_vertex = pull_block.local_start; local_vertex < pull_block.local_end; local_vertex++) {
			idx_t vertex = pull_csr->start_vertex + local_vertex;
			auto lanes = ~state->seen[vertex];
			lanes &= state->lane_active;
			if (!lanes.any()) {
				continue;
			}
			candidate_vertices++;

			std::bitset<LANE_LIMIT> found;
			auto start_edges = pull_csr->offsets[local_vertex].load(std::memory_order_relaxed);
			auto end_edges = pull_csr->offsets[local_vertex + 1].load(std::memory_order_relaxed);
			for (auto offset = start_edges; offset < end_edges; offset++) {
				scanned_edges++;
				auto predecessor = pull_csr->predecessors[offset];
				found |= visit[predecessor] & lanes;
				if ((found & lanes) == lanes) {
					break;
				}
			}

			found &= lanes;
			if (found.any()) {
				next[vertex] = found;
				state->seen[vertex] |= found;
				local_change = true;
				changed_vertices++;
			}
		}
	}
	RecordPhaseTiming("pull_scan", "pull", iteration, pulled_partitions, scanned_vertices, scanned_edges,
	                  candidate_vertices, changed_vertices, timer.ElapsedMs());
	if (local_change) {
		state->worker_changed[worker_id] = 1;
	}
}

idx_t PushPullIterativeLengthWorker::CheckChange(std::bitset<LANE_LIMIT> *seen, std::bitset<LANE_LIMIT> *next,
                                                 shared_ptr<LocalCSR> &local_csr, idx_t &candidate_vertices) const {
	idx_t changed_vertices = 0;
	const bool all_lanes_active = state->lane_active.all();
	for (auto i = local_csr->start_vertex; i < local_csr->end_vertex; i++) {
		auto lanes = next[i];
		if (!all_lanes_active) {
			lanes &= state->lane_active;
		}
		if (lanes.any()) {
			lanes &= ~seen[i];
			next[i] = lanes;
			seen[i] |= lanes;
			candidate_vertices++;
			if (lanes.any()) {
				changed_vertices++;
			}
		}
	}
	if (changed_vertices > 0) {
		state->worker_changed[worker_id] = 1;
	}
	return changed_vertices;
}

void PushPullIterativeLengthWorker::RecordPhaseTiming(const char *phase, const char *mode, idx_t iteration,
                                                      idx_t partition_count, idx_t vertices, idx_t edges,
                                                      idx_t candidates, idx_t changed_vertices, double time_ms) const {
	if (!state->benchmark_enabled) {
		return;
	}
	state->phase_timing_data[worker_id].push_back(
	    {state->current_batch, iteration, mode, phase, worker_id, state->active, state->frontier_vertices,
	     static_cast<idx_t>(state->v_size), partition_count, vertices, edges, candidates, changed_vertices, time_ms});
}

} // namespace duckdb
