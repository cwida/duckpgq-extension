#include "duckpgq/core/operator/iterative_length/iterative_length_task.hpp"
#include <duckdb/parallel/event.hpp>
#include <duckpgq/core/operator/iterative_length/iterative_length_state.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

#include <chrono>
#include <fstream>
#include <thread>

#ifdef __linux__
#include <sched.h>
#elif defined(__APPLE__)
#include <pthread.h>
#endif

namespace duckdb {

IterativeLengthTask::IterativeLengthTask(shared_ptr<Event> event_p, ClientContext &context,
                                         shared_ptr<IterativeLengthState> &state, idx_t worker_id,
                                         const PhysicalOperator &op_p)
    : ExecutorTask(context, std::move(event_p), op_p), context(context), state(state), worker_id(worker_id) {
	explore_done = false;
}

void IterativeLengthTask::CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen,
                                      std::vector<std::bitset<LANE_LIMIT>> &next,
                                      shared_ptr<LocalCSR> &local_csr) const {
	for (auto i = local_csr->start_vertex; i < local_csr->end_vertex; i++) {
		if (next[i].any()) {
			next[i] &= ~seen[i];
			seen[i] |= next[i];
			if (!state->change && next[i].any()) {
				state->change = true;
			}
		}
	}
}

TaskExecutionResult IterativeLengthTask::ExecuteTask(TaskExecutionMode mode) {
	auto &barrier = state->barrier;
	while (state->started_searches < state->pairs->size()) {
		barrier->Wait(worker_id);

		if (worker_id == 0) {
			state->InitializeLanes();
		}
		barrier->Wait(worker_id);
		do {
			IterativeLength();
			barrier->Wait(worker_id);
			if (worker_id == 0) {
				ReachDetect();
			}
			barrier->Wait(worker_id);
		} while (state->change);
		if (worker_id == 0) {
			UnReachableSet();
		}

		// Final synchronization before finishing
		barrier->Wait(worker_id);
		if (worker_id == 0) {
			state->Clear();
		}
		barrier->Wait(worker_id);
	}

	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

void IterativeLengthTask::Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                  std::vector<std::bitset<LANE_LIMIT>> &next, const LocalCSR &local_csr) {
	if (local_csr.HasSparseRows()) {
		for (idx_t row_idx = 0; row_idx < local_csr.source_vertices.size(); row_idx++) {
			auto source_vertex = local_csr.source_vertices[row_idx];
			if (visit[source_vertex].any()) {
				auto start_edges = local_csr.row_offsets[row_idx];
				auto end_edges = local_csr.row_offsets[row_idx + 1];
				for (auto offset = start_edges; offset < end_edges; offset++) {
					auto n = local_csr.e[offset] + local_csr.start_vertex;
					next[n] |= visit[source_vertex];
				}
			}
		}
		return;
	}

	for (idx_t i = 0; i < local_csr.GetVertexSize(); i++) {
		if (visit[i].any()) {
			auto start_edges = local_csr.v[i].load(std::memory_order_relaxed);
			auto end_edges = local_csr.v[i + 1].load(std::memory_order_relaxed);
			for (auto offset = start_edges; offset < end_edges; offset++) {
				auto n = local_csr.e[offset] + local_csr.start_vertex;
				next[n] |= visit[i];
			}
		}
	}
}

// Wrapper function to call Explore and log data
void IterativeLengthTask::RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                     std::vector<std::bitset<LANE_LIMIT>> &next, const LocalCSR &local_csr) {
	if (!state->benchmark_enabled) {
		Explore(visit, next, local_csr);
		return;
	}

	auto start_time = std::chrono::high_resolution_clock::now();
	Explore(visit, next, local_csr);
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
	state->timing_data.emplace_back(thread_id, core_id, duration_ms, state->num_threads, local_csr.GetVertexSize(),
	                                local_csr.e.size(), state->local_csrs.size(), state->iter);
}

uint64_t get_word(const std::bitset<LANE_LIMIT> &b, int word_idx) {
	uint64_t word = 0;
	for (int i = 0; i < 64; ++i) {
		auto bit_idx = word_idx * 64 + i;
		if (bit_idx < LANE_LIMIT && b.test(bit_idx)) {
			word |= (1ULL << i);
		}
	}
	return word;
}

static void WriteLaneActivity(IterativeLengthState &state, const std::vector<std::bitset<LANE_LIMIT>> &visit) {
	auto file_name = state.benchmark_output_prefix + "_lane_activity_" + state.benchmark_run_id + ".csv";
	std::ofstream outfile(file_name, std::ios_base::app);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding lane activity benchmark file \"%s\"", file_name);
	}
	if (outfile.tellp() == 0) {
		outfile << "Iter,Lane,ActiveVertices\n";
	}

	std::vector<int64_t> lane_activity(LANE_LIMIT, 0);
	static constexpr idx_t WORD_COUNT = (LANE_LIMIT + 63) / 64;
	for (idx_t v = 0; v < state.v_size; ++v) {
		uint64_t words[WORD_COUNT];
		for (idx_t word_idx = 0; word_idx < WORD_COUNT; word_idx++) {
			words[word_idx] = get_word(visit[v], word_idx);
		}

		for (idx_t w = 0; w < WORD_COUNT; ++w) {
			uint64_t word = words[w];
			for (int bit = 0; bit < 64; ++bit) {
				auto lane = w * 64 + bit;
				if (lane < LANE_LIMIT) {
					lane_activity[lane] += (word >> bit) & 1;
				}
			}
		}
	}

	for (idx_t lane = 0; lane < LANE_LIMIT; ++lane) {
		outfile << state.iter << "," << lane << "," << lane_activity[lane] << "\n";
	}
}

void IterativeLengthTask::IterativeLength() {
	auto &seen = state->seen;
	auto &visit = state->iter & 1 ? state->visit1 : state->visit2;
	auto &next = state->iter & 1 ? state->visit2 : state->visit1;
	auto &barrier = state->barrier;
	// Clear `next` array
	while (state->partition_counter < state->local_csrs.size()) {
		state->local_csr_lock.lock();
		if (state->partition_counter >= state->local_csrs.size()) {
			state->local_csr_lock.unlock();
			break;
		}
		auto &local_csr = state->local_csrs[state->partition_counter++];
		state->local_csr_lock.unlock();
		for (auto i = local_csr->start_vertex; i < local_csr->end_vertex; i++) {
			next[i] = 0;
			// visit[i] &= state->lane_active;
		}
	}
	barrier->Wait(worker_id);
	state->partition_counter = 0;
	state->local_csr_counter = 0;
	static std::atomic<int> finished_tasks(0);
	barrier->Wait(worker_id);
	while (state->local_csr_counter < state->local_csrs.size()) {
		state->local_csr_lock.lock();
		if (state->local_csr_counter >= state->local_csrs.size()) {
			state->local_csr_lock.unlock();
			break;
		}
		auto local_csr = state->local_csrs[state->local_csr_counter++].get();
		if (!local_csr) {
			throw InternalException("Tried to reference nullptr for LocalCSR");
		}
		state->local_csr_lock.unlock();
		RunExplore(visit, next, *local_csr);
	}
	state->change = false;
	// Mark this thread as finished
	finished_tasks.fetch_add(1);
	// Last thread reaching here should reset the counter for the next iteration
	if (finished_tasks.load() == state->tasks_scheduled) {
		finished_tasks.store(0); // Reset for the next phase
	}

	if (worker_id == 0 && state->benchmark_lane_activity_enabled) {
		WriteLaneActivity(*state, visit);
	}

	barrier->Wait(worker_id);
	while (state->partition_counter < state->local_csrs.size()) {
		state->local_csr_lock.lock();
		if (state->partition_counter < state->local_csrs.size()) {
			auto &local_csr = state->local_csrs[state->partition_counter++];
			state->local_csr_lock.unlock();
			CheckChange(seen, next, local_csr);
		} else {
			state->local_csr_lock.unlock();
			break; // Avoids reading invalid memory
		}
	}
	barrier->Wait(worker_id);
	state->partition_counter = 0;
}

void IterativeLengthTask::ReachDetect() const {
	auto result_data = FlatVector::GetDataMutable<int64_t>(state->pf_results->data[0]);

	// detect lanes that finished
	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		int64_t search_num = state->lane_to_num[lane];
		if (search_num >= 0) { // active lane
			int64_t dst_pos = state->vdata_dst.sel->get_index(search_num);
			if (state->seen[state->dst[dst_pos]][lane]) {
				result_data[search_num] = state->iter; /* found at iter => iter = path length */
				state->lane_to_num[lane] = -1;         // mark inactive
				state->active--;
				state->lane_active[lane] = false;
			}
		}
	}
	if (state->active == 0) {
		state->change = false;
	}
	// into the next iteration
	state->iter++;
}

void IterativeLengthTask::UnReachableSet() const {
	auto result_data = FlatVector::GetDataMutable<int64_t>(state->pf_results->data[0]);
	auto &result_validity = FlatVector::ValidityMutable(state->pf_results->data[0]);

	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		int64_t search_num = state->lane_to_num[lane];
		if (search_num >= 0) { // active lane
			result_validity.SetInvalid(search_num);
			result_data[search_num] = (int64_t)-1; /* no path */
			state->lane_to_num[lane] = -1;         // mark inactive
			state->lane_active[lane] = false;
		}
	}
}

} // namespace duckdb
