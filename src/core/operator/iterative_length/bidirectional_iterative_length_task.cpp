#include "duckpgq/core/operator/iterative_length/bidirectional_iterative_length_task.hpp"

#include <duckdb/parallel/event.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

#include <chrono>
#include <thread>

#ifdef __linux__
#include <sched.h>
#elif defined(__APPLE__)
#include <pthread.h>
#endif

namespace duckdb {

BidirectionalIterativeLengthTask::BidirectionalIterativeLengthTask(shared_ptr<Event> event_p, ClientContext &context,
                                                                   shared_ptr<BidirectionalIterativeLengthState> &state,
                                                                   idx_t worker_id, const PhysicalOperator &op_p)
    : ExecutorTask(context, std::move(event_p), op_p), context(context), state(state), worker_id(worker_id) {
}

TaskExecutionResult BidirectionalIterativeLengthTask::ExecuteTask(TaskExecutionMode mode) {
	auto &barrier = state->barrier;
	while (true) {
		barrier->Wait(worker_id);
		if (worker_id == 0) {
			state->has_more_batches = state->started_searches < state->pairs->size();
		}
		barrier->Wait(worker_id);
		if (!state->has_more_batches) {
			break;
		}

		if (worker_id == 0) {
			state->InitializeBidirectionalLanes();
		}
		barrier->Wait(worker_id);

		while (state->active > 0) {
			ExpandSide(BidirectionalSearchSide::SOURCE);
			if (worker_id == 0) {
				state->continue_search = state->active > 0 && state->last_side_changed;
			}
			barrier->Wait(worker_id);
			if (!state->continue_search) {
				break;
			}

			ExpandSide(BidirectionalSearchSide::DESTINATION);
			if (worker_id == 0) {
				state->continue_search = state->active > 0 && state->last_side_changed;
			}
			barrier->Wait(worker_id);
			if (!state->continue_search) {
				break;
			}
		}

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

void BidirectionalIterativeLengthTask::ExpandSide(BidirectionalSearchSide side) {
	auto &barrier = state->barrier;
	auto &partition_csrs = side == BidirectionalSearchSide::SOURCE ? state->local_csrs : state->reverse_local_csrs;
	auto &side_seen = side == BidirectionalSearchSide::SOURCE ? state->src_seen : state->dst_seen;
	auto &other_seen = side == BidirectionalSearchSide::SOURCE ? state->dst_seen : state->src_seen;
	auto &side_depth = side == BidirectionalSearchSide::SOURCE ? state->src_depth : state->dst_depth;
	auto &visit1 = side == BidirectionalSearchSide::SOURCE ? state->src_visit1 : state->dst_visit1;
	auto &visit2 = side == BidirectionalSearchSide::SOURCE ? state->src_visit2 : state->dst_visit2;
	auto &visit = side_depth % 2 == 0 ? visit1 : visit2;
	auto &next = side_depth % 2 == 0 ? visit2 : visit1;

	if (worker_id == 0) {
		state->partition_counter = 0;
		state->local_csr_counter = 0;
		state->change = false;
		state->last_side_changed = false;
		for (auto &meet_mask : state->worker_meet_masks) {
			meet_mask.reset();
		}
	}
	barrier->Wait(worker_id);

	while (state->partition_counter < partition_csrs.size()) {
		state->local_csr_lock.lock();
		if (state->partition_counter >= partition_csrs.size()) {
			state->local_csr_lock.unlock();
			break;
		}
		auto &local_csr = partition_csrs[state->partition_counter++];
		state->local_csr_lock.unlock();
		for (auto i = local_csr->start_vertex; i < local_csr->end_vertex; i++) {
			next[i] = 0;
		}
	}
	barrier->Wait(worker_id);

	if (worker_id == 0) {
		state->partition_counter = 0;
	}
	barrier->Wait(worker_id);

	while (state->local_csr_counter < partition_csrs.size()) {
		state->local_csr_lock.lock();
		if (state->local_csr_counter >= partition_csrs.size()) {
			state->local_csr_lock.unlock();
			break;
		}
		auto local_csr = partition_csrs[state->local_csr_counter++].get();
		if (!local_csr) {
			throw InternalException("Tried to reference nullptr for LocalCSR");
		}
		state->local_csr_lock.unlock();
		RunExplore(visit, next, local_csr->v, local_csr->e, local_csr->GetVertexSize(), local_csr->start_vertex);
	}
	barrier->Wait(worker_id);

	if (worker_id == 0) {
		state->local_csr_counter = 0;
		state->partition_counter = 0;
	}
	barrier->Wait(worker_id);

	while (state->partition_counter < partition_csrs.size()) {
		state->local_csr_lock.lock();
		if (state->partition_counter >= partition_csrs.size()) {
			state->local_csr_lock.unlock();
			break;
		}
		auto &local_csr = partition_csrs[state->partition_counter++];
		state->local_csr_lock.unlock();
		CheckChange(side_seen, next, other_seen, local_csr);
	}
	barrier->Wait(worker_id);

	if (worker_id == 0) {
		std::bitset<LANE_LIMIT> found_lanes;
		for (const auto &meet_mask : state->worker_meet_masks) {
			found_lanes |= meet_mask;
		}
		side_depth++;
		state->last_side_changed = state->change;
		CompleteFoundLanes(found_lanes, state->src_depth + state->dst_depth);
		if (state->active == 0) {
			state->last_side_changed = false;
		}
	}
	barrier->Wait(worker_id);
}

void BidirectionalIterativeLengthTask::CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen,
                                                   std::vector<std::bitset<LANE_LIMIT>> &next,
                                                   std::vector<std::bitset<LANE_LIMIT>> &other_seen,
                                                   shared_ptr<LocalCSR> &local_csr) const {
	std::bitset<LANE_LIMIT> found_lanes;
	bool local_change = false;
	for (auto i = local_csr->start_vertex; i < local_csr->end_vertex; i++) {
		if (next[i].any()) {
			next[i] &= ~seen[i];
			next[i] &= state->lane_active;
			if (next[i].any()) {
				found_lanes |= next[i] & other_seen[i] & state->lane_active;
				seen[i] |= next[i];
				local_change = true;
			}
		}
	}

	if (local_change) {
		std::lock_guard<std::mutex> guard(state->change_lock);
		state->change = true;
	}
	state->worker_meet_masks[worker_id] |= found_lanes;
}

void BidirectionalIterativeLengthTask::CompleteFoundLanes(std::bitset<LANE_LIMIT> found_lanes,
                                                          int64_t path_length) const {
	auto result_data = FlatVector::GetDataMutable<int64_t>(state->pf_results->data[0]);
	found_lanes &= state->lane_active;
	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		if (!found_lanes[lane]) {
			continue;
		}
		int64_t search_num = state->lane_to_num[lane];
		if (search_num >= 0) {
			result_data[search_num] = path_length;
			state->lane_to_num[lane] = -1;
			state->active--;
			state->lane_active[lane] = false;
		}
	}
}

void BidirectionalIterativeLengthTask::UnReachableSet() const {
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

void BidirectionalIterativeLengthTask::Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                               std::vector<std::bitset<LANE_LIMIT>> &next,
                                               const std::atomic<uint32_t> *v, const std::vector<uint16_t> &e,
                                               size_t v_size, idx_t start_vertex) {
	for (auto i = 0; i < v_size; i++) {
		auto active_visit = visit[i] & state->lane_active;
		if (active_visit.any()) {
			auto start_edges = v[i].load(std::memory_order_relaxed);
			auto end_edges = v[i + 1].load(std::memory_order_relaxed);
			for (auto offset = start_edges; offset < end_edges; offset++) {
				auto n = e[offset] + start_vertex;
				next[n] |= active_visit;
			}
		}
	}
}

void BidirectionalIterativeLengthTask::RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                                  std::vector<std::bitset<LANE_LIMIT>> &next, const atomic<uint32_t> *v,
                                                  const std::vector<uint16_t> &e, size_t v_size, idx_t start_vertex) {
	if (!state->benchmark_enabled) {
		Explore(visit, next, v, e, v_size, start_vertex);
		return;
	}

	auto start_time = std::chrono::high_resolution_clock::now();
	Explore(visit, next, v, e, v_size, start_vertex);
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
	state->timing_data.emplace_back(thread_id, core_id, duration_ms, state->num_threads, v_size, e.size(),
	                                state->local_csrs.size(), state->src_depth + state->dst_depth);
}

} // namespace duckdb
