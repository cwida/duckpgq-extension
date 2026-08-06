#include "duckpgq/core/operator/iterative_length/push_pull_iterative_length_task.hpp"

#include <duckdb/parallel/event.hpp>
#include <chrono>
#include <fstream>
#include <thread>

#ifdef __linux__
#include <sched.h>
#elif defined(__APPLE__)
#include <pthread.h>
#endif

namespace duckdb {

PushPullIterativeLengthTask::PushPullIterativeLengthTask(shared_ptr<Event> event_p, ClientContext &context,
                                                         shared_ptr<PushPullIterativeLengthState> &state,
                                                         idx_t worker_id, const PhysicalOperator &op_p)
    : ExecutorTask(context, std::move(event_p), op_p), context(context), state(state), worker_id(worker_id) {
}

TaskExecutionResult PushPullIterativeLengthTask::ExecuteTask(TaskExecutionMode mode) {
	auto &barrier = state->barrier;
	while (true) {
		if (worker_id == 0) {
			state->has_more_batches = state->started_searches < state->pairs->size();
		}
		TimedBarrier("barrier_after_batch_decision", "batch", state->iter);
		if (!state->has_more_batches) {
			break;
		}

		if (worker_id == 0) {
			state->current_batch++;
			auto start_time = std::chrono::steady_clock::now();
			state->InitializeLanes();
			auto end_time = std::chrono::steady_clock::now();
			auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
			RecordPhaseTiming("initialize_lanes", "batch", state->iter, 0, state->pairs->size(), 0, state->active, 0,
			                  duration_ms);
		}
		TimedBarrier("barrier_after_initialize_lanes", "batch", state->iter);
		do {
			auto iteration = static_cast<idx_t>(state->iter);
			PushPullIterativeLength();
			TimedBarrier("barrier_before_reach_detect", state->use_pull ? "pull" : "push", iteration);
			if (worker_id == 0) {
				auto start_time = std::chrono::steady_clock::now();
				ReachDetect();
				auto end_time = std::chrono::steady_clock::now();
				auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
				RecordPhaseTiming("reach_detect", state->use_pull ? "pull" : "push", iteration, 0, LANE_LIMIT, 0,
				                  state->active, 0, duration_ms);
			}
			TimedBarrier("barrier_after_reach_detect", state->use_pull ? "pull" : "push", iteration);
			if (worker_id == 0) {
				state->continue_search = state->change;
			}
			TimedBarrier("barrier_after_search_decision", state->use_pull ? "pull" : "push", iteration);
		} while (state->continue_search);
		if (worker_id == 0) {
			auto start_time = std::chrono::steady_clock::now();
			UnReachableSet();
			auto end_time = std::chrono::steady_clock::now();
			auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
			RecordPhaseTiming("unreachable_set", "batch", state->iter, 0, LANE_LIMIT, 0, 0, 0, duration_ms);
		}

		TimedBarrier("barrier_before_clear_state", "batch", state->iter);
		if (worker_id == 0) {
			auto start_time = std::chrono::steady_clock::now();
			state->Clear();
			auto end_time = std::chrono::steady_clock::now();
			auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
			RecordPhaseTiming("clear_state", "batch", state->iter, 0, static_cast<idx_t>(state->v_size) * 3, 0, 0, 0,
			                  duration_ms);
		}
		TimedBarrier("barrier_after_clear_state", "batch", state->iter);
	}

	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

idx_t PushPullIterativeLengthTask::CountFrontierVertices(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                                         idx_t start_vertex, idx_t end_vertex) const {
	idx_t frontier_vertices = 0;
	for (idx_t i = start_vertex; i < end_vertex; i++) {
		if ((visit[i] & state->lane_active).any()) {
			frontier_vertices++;
		}
	}
	return frontier_vertices;
}

void PushPullIterativeLengthTask::PushPullIterativeLength() {
	auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;
	auto iteration = static_cast<idx_t>(state->iter);
	const char *mode_name;

	if (worker_id == 0) {
		state->change = false;
		state->partition_counter = 0;
		state->local_csr_counter = 0;
		state->pull_block_counter = 0;
	}
	TimedBarrier("barrier_before_frontier_count", "decide", iteration);

	idx_t vertices_per_worker = (static_cast<idx_t>(state->v_size) + state->tasks_scheduled - 1) / state->tasks_scheduled;
	idx_t vertex_start = worker_id * vertices_per_worker;
	idx_t vertex_end = std::min(vertex_start + vertices_per_worker, static_cast<idx_t>(state->v_size));
	auto frontier_count_start_time = std::chrono::steady_clock::now();
	auto local_frontier_vertices = CountFrontierVertices(visit, vertex_start, vertex_end);
	auto frontier_count_end_time = std::chrono::steady_clock::now();
	state->frontier_count_by_worker[worker_id] = local_frontier_vertices;
	RecordPhaseTiming("frontier_count", "decide", iteration, 0, vertex_end - vertex_start, 0, local_frontier_vertices,
	                  0,
	                  std::chrono::duration<double, std::milli>(frontier_count_end_time - frontier_count_start_time)
	                      .count());

	TimedBarrier("barrier_after_frontier_count", "decide", iteration);
	if (worker_id == 0) {
		idx_t total_frontier_vertices = 0;
		auto decide_start_time = std::chrono::steady_clock::now();
		for (idx_t i = 0; i < state->tasks_scheduled; i++) {
			total_frontier_vertices += state->frontier_count_by_worker[i];
		}
		state->frontier_vertices = total_frontier_vertices;
		state->use_pull =
		    state->frontier_vertices * state->pull_frontier_gate >= static_cast<idx_t>(state->v_size);
		auto decide_end_time = std::chrono::steady_clock::now();
		if (state->benchmark_enabled) {
			state->iteration_stats.push_back({state->current_batch, static_cast<idx_t>(state->iter),
			                                  state->use_pull ? "pull" : "push",
			                                  state->active, state->frontier_vertices, static_cast<idx_t>(state->v_size),
			                                  state->pull_frontier_gate});
		}
		state->partition_counter = 0;
		state->local_csr_counter = 0;
		state->pull_block_counter = 0;
		RecordPhaseTiming("choose_mode", state->use_pull ? "pull" : "push", iteration, 0, state->tasks_scheduled, 0,
		                  state->frontier_vertices, 0,
		                  std::chrono::duration<double, std::milli>(decide_end_time - decide_start_time).count());
	}
	TimedBarrier("barrier_after_choose_mode", state->use_pull ? "pull" : "push", iteration);
	mode_name = state->use_pull ? "pull" : "push";

	auto clear_start_time = std::chrono::steady_clock::now();
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
	auto clear_end_time = std::chrono::steady_clock::now();
	RecordPhaseTiming("clear_next", mode_name, iteration, cleared_partitions, cleared_vertices, 0, 0, 0,
	                  std::chrono::duration<double, std::milli>(clear_end_time - clear_start_time).count());
	TimedBarrier("barrier_after_clear_next", mode_name, iteration);

	if (worker_id == 0) {
		state->partition_counter = 0;
		state->local_csr_counter = 0;
		state->pull_block_counter = 0;
	}
	TimedBarrier("barrier_after_clear_counter_reset", mode_name, iteration);

	if (state->use_pull) {
		Pull(iteration);
	} else {
		Push(iteration);
	}

	if (worker_id == 0) {
		state->partition_counter = 0;
		state->local_csr_counter = 0;
	}
}

idx_t PushPullIterativeLengthTask::Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                           std::vector<std::bitset<LANE_LIMIT>> &next, const LocalCSR &local_csr) {
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

idx_t PushPullIterativeLengthTask::RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                              std::vector<std::bitset<LANE_LIMIT>> &next, const LocalCSR &local_csr) {
	if (!state->benchmark_enabled) {
		Explore(visit, next, local_csr);
		return 0;
	}

	auto start_time = std::chrono::high_resolution_clock::now();
	auto explored_edges = Explore(visit, next, local_csr);
	auto end_time = std::chrono::high_resolution_clock::now();
	auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

	std::thread::id thread_id = std::this_thread::get_id();
	int core_id = -1;
#ifdef __linux__
	core_id = sched_getcpu();
#elif defined(__APPLE__)
	uint64_t tid;
	pthread_threadid_np(nullptr, &tid);
	auto hardware_threads = std::thread::hardware_concurrency();
	if (hardware_threads > 0) {
		core_id = static_cast<int>(tid % hardware_threads);
	}
#endif

	state->worker_timing_data[worker_id].emplace_back(thread_id, core_id, duration_ms, state->num_threads,
	                                                  local_csr.GetVertexSize(), explored_edges,
	                                                  state->local_csrs.size(), state->iter);
	return explored_edges;
}

void PushPullIterativeLengthTask::Push(idx_t iteration) {
	auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;

	auto explore_start_time = std::chrono::steady_clock::now();
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
	auto explore_end_time = std::chrono::steady_clock::now();
	RecordPhaseTiming("push_explore", "push", iteration, explored_partitions, explored_vertices, explored_edges, 0, 0,
	                  std::chrono::duration<double, std::milli>(explore_end_time - explore_start_time).count());

	TimedBarrier("barrier_after_push_explore", "push", iteration);
	if (worker_id == 0) {
		state->partition_counter = 0;
	}
	TimedBarrier("barrier_after_push_check_counter_reset", "push", iteration);

	auto check_start_time = std::chrono::steady_clock::now();
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
		checked_vertices += state->local_csrs[partition_idx]->end_vertex - state->local_csrs[partition_idx]->start_vertex;
		checked_partitions++;
	}
	auto check_end_time = std::chrono::steady_clock::now();
	RecordPhaseTiming("push_check", "push", iteration, checked_partitions, checked_vertices, 0, candidate_vertices,
	                  changed_vertices,
	                  std::chrono::duration<double, std::milli>(check_end_time - check_start_time).count());
	TimedBarrier("barrier_after_push_check", "push", iteration);
}

void PushPullIterativeLengthTask::Pull(idx_t iteration) {
	if (state->pull_local_csrs.empty()) {
		throw InternalException("Push/pull path finding requires pull CSR partitions.");
	}

	auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;
	bool local_change = false;

	auto pull_start_time = std::chrono::steady_clock::now();
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
	auto pull_end_time = std::chrono::steady_clock::now();
	RecordPhaseTiming("pull_scan", "pull", iteration, pulled_partitions, scanned_vertices, scanned_edges,
	                  candidate_vertices, changed_vertices,
	                  std::chrono::duration<double, std::milli>(pull_end_time - pull_start_time).count());

	if (local_change) {
		auto change_start_time = std::chrono::steady_clock::now();
		std::lock_guard<std::mutex> lock(state->change_lock);
		state->change = true;
		auto change_end_time = std::chrono::steady_clock::now();
		RecordPhaseTiming("set_change", "pull", iteration, 0, 0, 0, 0, 0,
		                  std::chrono::duration<double, std::milli>(change_end_time - change_start_time).count());
	}
	TimedBarrier("barrier_after_pull", "pull", iteration);
}

idx_t PushPullIterativeLengthTask::CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen,
                                               std::vector<std::bitset<LANE_LIMIT>> &next,
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
		std::lock_guard<std::mutex> lock(state->change_lock);
		state->change = true;
	}
	return changed_vertices;
}

void PushPullIterativeLengthTask::RecordPhaseTiming(const char *phase, const char *mode, idx_t iteration,
                                                    idx_t partition_count, idx_t vertices, idx_t edges,
                                                    idx_t candidates, idx_t changed_vertices, double time_ms) const {
	if (!state->benchmark_enabled) {
		return;
	}
	state->phase_timing_data[worker_id].push_back(
	    {state->current_batch, iteration, mode, phase, worker_id, state->active, state->frontier_vertices,
	     static_cast<idx_t>(state->v_size), partition_count, vertices, edges, candidates, changed_vertices, time_ms});
}

void PushPullIterativeLengthTask::TimedBarrier(const char *phase, const char *mode, idx_t iteration) {
	auto start_time = std::chrono::steady_clock::now();
	state->barrier->Wait(worker_id);
	auto end_time = std::chrono::steady_clock::now();
	RecordPhaseTiming(phase, mode, iteration, 0, 0, 0, 0, 0,
	                  std::chrono::duration<double, std::milli>(end_time - start_time).count());
}

void PushPullIterativeLengthTask::ReachDetect() const {
	auto result_data = FlatVector::GetDataMutable<int64_t>(state->pf_results->data[0]);

	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		int64_t search_num = state->lane_to_num[lane];
		if (search_num >= 0) {
			int64_t dst_pos = state->vdata_dst.sel->get_index(search_num);
			if (state->seen[state->dst[dst_pos]][lane]) {
				result_data[search_num] = state->iter;
				state->lane_to_num[lane] = -1;
				state->active--;
				state->lane_active[lane] = false;
			}
		}
	}
	if (state->active == 0) {
		state->change = false;
	}
	state->iter++;
}

void PushPullIterativeLengthTask::UnReachableSet() const {
	auto result_data = FlatVector::GetDataMutable<int64_t>(state->pf_results->data[0]);
	auto &result_validity = FlatVector::ValidityMutable(state->pf_results->data[0]);

	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		int64_t search_num = state->lane_to_num[lane];
		if (search_num >= 0) {
			result_validity.SetInvalid(search_num);
			result_data[search_num] = (int64_t)-1;
			state->lane_to_num[lane] = -1;
			state->lane_active[lane] = false;
		}
	}
}

} // namespace duckdb
