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
	while (state->started_searches < state->pairs->size()) {
		barrier->Wait(worker_id);

		if (worker_id == 0) {
			state->InitializeLanes();
		}
		barrier->Wait(worker_id);
		do {
			PushPullIterativeLength();
			barrier->Wait(worker_id);
			if (worker_id == 0) {
				ReachDetect();
			}
			barrier->Wait(worker_id);
		} while (state->change);
		if (worker_id == 0) {
			UnReachableSet();
		}

		barrier->Wait(worker_id);
		if (worker_id == 0) {
			state->Clear();
		}
		barrier->Wait(worker_id);
	}

	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

idx_t PushPullIterativeLengthTask::CountFrontierVertices(const std::vector<std::bitset<LANE_LIMIT>> &visit) const {
	idx_t frontier_vertices = 0;
	for (idx_t i = 0; i < state->v_size; i++) {
		if ((visit[i] & state->lane_active).any()) {
			frontier_vertices++;
		}
	}
	return frontier_vertices;
}

void PushPullIterativeLengthTask::PushPullIterativeLength() {
	auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;
	auto &barrier = state->barrier;

	if (worker_id == 0) {
		state->frontier_vertices = CountFrontierVertices(visit);
		state->use_pull =
		    state->frontier_vertices * state->pull_frontier_gate >= static_cast<idx_t>(state->v_size);
		if (state->benchmark_enabled) {
			state->iteration_stats.push_back({static_cast<idx_t>(state->iter), state->use_pull ? "pull" : "push",
			                                  state->active, state->frontier_vertices, static_cast<idx_t>(state->v_size),
			                                  state->pull_frontier_gate});
		}
		state->change = false;
		state->partition_counter = 0;
		state->local_csr_counter = 0;
	}
	barrier->Wait(worker_id);

	while (true) {
		auto partition_idx = state->partition_counter.fetch_add(1);
		if (partition_idx >= state->local_csrs.size()) {
			break;
		}
		auto &local_csr = state->local_csrs[partition_idx];
		for (auto i = local_csr->start_vertex; i < local_csr->end_vertex; i++) {
			next[i] = 0;
		}
	}
	barrier->Wait(worker_id);

	if (worker_id == 0) {
		state->partition_counter = 0;
		state->local_csr_counter = 0;
	}
	barrier->Wait(worker_id);

	if (state->use_pull) {
		Pull();
	} else {
		Push();
	}

	if (worker_id == 0) {
		state->partition_counter = 0;
		state->local_csr_counter = 0;
	}
}

idx_t PushPullIterativeLengthTask::Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                           std::vector<std::bitset<LANE_LIMIT>> &next, const std::atomic<uint32_t> *v,
                                           const std::vector<uint16_t> &e, size_t v_size, idx_t start_vertex) {
	idx_t explored_edges = 0;
	const bool all_lanes_active = state->lane_active.all();
	for (idx_t i = 0; i < v_size; i++) {
		auto lanes = visit[i];
		if (!all_lanes_active) {
			lanes &= state->lane_active;
		}
		if (lanes.any()) {
			auto start_edges = v[i].load(std::memory_order_relaxed);
			auto end_edges = v[i + 1].load(std::memory_order_relaxed);
			explored_edges += end_edges - start_edges;
			for (auto offset = start_edges; offset < end_edges; offset++) {
				auto n = e[offset] + start_vertex;
				next[n] |= lanes;
			}
		}
	}
	return explored_edges;
}

idx_t PushPullIterativeLengthTask::RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                              std::vector<std::bitset<LANE_LIMIT>> &next, const atomic<uint32_t> *v,
                                              const std::vector<uint16_t> &e, size_t v_size, idx_t start_vertex) {
	if (!state->benchmark_enabled) {
		Explore(visit, next, v, e, v_size, start_vertex);
		return 0;
	}

	auto start_time = std::chrono::high_resolution_clock::now();
	auto explored_edges = Explore(visit, next, v, e, v_size, start_vertex);
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

	std::lock_guard<std::mutex> guard(state->log_mutex);
	state->timing_data.emplace_back(thread_id, core_id, duration_ms, state->num_threads, v_size, explored_edges,
	                                state->local_csrs.size(), state->iter);
	return explored_edges;
}

void PushPullIterativeLengthTask::Push() {
	auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;
	auto &barrier = state->barrier;

	while (true) {
		auto partition_idx = state->local_csr_counter.fetch_add(1);
		if (partition_idx >= state->local_csrs.size()) {
			break;
		}
		auto local_csr = state->local_csrs[partition_idx].get();
		if (!local_csr) {
			throw InternalException("Tried to reference nullptr for LocalCSR");
		}
		RunExplore(visit, next, local_csr->v, local_csr->e, local_csr->GetVertexSize(), local_csr->start_vertex);
	}

	barrier->Wait(worker_id);
	if (worker_id == 0) {
		state->partition_counter = 0;
	}
	barrier->Wait(worker_id);

	while (true) {
		auto partition_idx = state->partition_counter.fetch_add(1);
		if (partition_idx >= state->local_csrs.size()) {
			break;
		}
		CheckChange(state->seen, next, state->local_csrs[partition_idx]);
	}
	barrier->Wait(worker_id);
}

void PushPullIterativeLengthTask::Pull() {
	if (state->reverse_local_csrs.empty()) {
		throw InternalException("Push/pull path finding requires reverse local CSR partitions.");
	}

	auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;
	auto vertices_per_worker =
	    (static_cast<idx_t>(state->v_size) + state->tasks_scheduled - 1) / state->tasks_scheduled;
	auto vertex_start = worker_id * vertices_per_worker;
	auto vertex_end = std::min(vertex_start + vertices_per_worker, static_cast<idx_t>(state->v_size));
	bool local_change = false;

	for (idx_t vertex = vertex_start; vertex < vertex_end; vertex++) {
		auto lanes = ~state->seen[vertex];
		lanes &= state->lane_active;
		if (!lanes.any()) {
			continue;
		}

		std::bitset<LANE_LIMIT> found;
		for (const auto &reverse_csr : state->reverse_local_csrs) {
			auto start_edges = reverse_csr->v[vertex].load(std::memory_order_relaxed);
			auto end_edges = reverse_csr->v[vertex + 1].load(std::memory_order_relaxed);
			for (auto offset = start_edges; offset < end_edges; offset++) {
				auto predecessor = reverse_csr->e[offset] + reverse_csr->start_vertex;
				found |= visit[predecessor] & lanes;
				if ((found & lanes) == lanes) {
					break;
				}
			}
			if ((found & lanes) == lanes) {
				break;
			}
		}

		found &= lanes;
		if (found.any()) {
			next[vertex] = found;
			state->seen[vertex] |= found;
			local_change = true;
		}
	}

	if (local_change) {
		std::lock_guard<std::mutex> lock(state->change_lock);
		state->change = true;
	}
	state->barrier->Wait(worker_id);
}

void PushPullIterativeLengthTask::CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen,
                                              std::vector<std::bitset<LANE_LIMIT>> &next,
                                              shared_ptr<LocalCSR> &local_csr) const {
	bool local_change = false;
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
			local_change |= lanes.any();
		}
	}
	if (local_change) {
		std::lock_guard<std::mutex> lock(state->change_lock);
		state->change = true;
	}
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
