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

static constexpr idx_t CANDIDATE_WORD_BITS = 64;
static constexpr idx_t CANDIDATE_FRONTIER_GATE = 128;

static string SideToString(BidirectionalSearchSide side) {
	return side == BidirectionalSearchSide::SOURCE ? "source" : "destination";
}

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
			auto initialize_start_time = std::chrono::high_resolution_clock::now();
			state->InitializeBidirectionalLanes();
			if (state->benchmark_enabled) {
				auto initialize_end_time = std::chrono::high_resolution_clock::now();
				auto duration_ms =
				    std::chrono::duration<double, std::milli>(initialize_end_time - initialize_start_time).count();
				RecordGlobalPhaseTiming(state->current_batch_id, 0, "batch_initialize", 0, duration_ms);
			}
		}
		barrier->Wait(worker_id);

		while (state->active > 0) {
			auto side = state->expand_source_next ? BidirectionalSearchSide::SOURCE
			                                      : BidirectionalSearchSide::DESTINATION;
			ExpandSide(side);
			if (worker_id == 0) {
				state->continue_search = state->active > 0 && state->last_side_changed;
			}
			barrier->Wait(worker_id);
			if (!state->continue_search) {
				break;
			}
		}

		if (worker_id == 0) {
			auto unreachable_start_time = std::chrono::high_resolution_clock::now();
			UnReachableSet();
			if (state->benchmark_enabled) {
				auto unreachable_end_time = std::chrono::high_resolution_clock::now();
				auto duration_ms =
				    std::chrono::duration<double, std::milli>(unreachable_end_time - unreachable_start_time).count();
				RecordGlobalPhaseTiming(state->current_batch_id, state->current_step_id, "unreachable_set", 0,
				                        duration_ms);
			}
		}

		barrier->Wait(worker_id);
		if (worker_id == 0) {
			auto batch_id = state->current_batch_id;
			auto step_id = state->current_step_id;
			auto clear_start_time = std::chrono::high_resolution_clock::now();
			state->Clear();
			if (state->benchmark_enabled) {
				auto clear_end_time = std::chrono::high_resolution_clock::now();
				auto duration_ms =
				    std::chrono::duration<double, std::milli>(clear_end_time - clear_start_time).count();
				RecordGlobalPhaseTiming(batch_id, step_id, "state_clear", 0, duration_ms);
			}
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
	auto &side_frontier_size =
	    side == BidirectionalSearchSide::SOURCE ? state->src_frontier_size : state->dst_frontier_size;
	auto &frontier_vertices =
	    side == BidirectionalSearchSide::SOURCE ? state->src_frontier_vertices : state->dst_frontier_vertices;
	auto &visit1 = side == BidirectionalSearchSide::SOURCE ? state->src_visit1 : state->dst_visit1;
	auto &visit2 = side == BidirectionalSearchSide::SOURCE ? state->src_visit2 : state->dst_visit2;
	auto &visit = side_depth % 2 == 0 ? visit1 : visit2;
	auto &next = side_depth % 2 == 0 ? visit2 : visit1;
	auto batch_id = state->current_batch_id;
	auto step_id = state->current_step_id;

	auto candidate_clear_start_time = std::chrono::high_resolution_clock::now();
	auto &dirty_candidate_words = state->worker_dirty_candidate_words[worker_id];
	auto &candidate_words = state->worker_candidate_words[worker_id];
	auto cleared_candidate_words = dirty_candidate_words.size();
	for (auto word_idx : dirty_candidate_words) {
		candidate_words[word_idx] = 0;
	}
	dirty_candidate_words.clear();
	if (state->benchmark_enabled) {
		auto candidate_clear_end_time = std::chrono::high_resolution_clock::now();
		auto duration_ms =
		    std::chrono::duration<double, std::milli>(candidate_clear_end_time - candidate_clear_start_time).count();
		RecordPhaseTiming(batch_id, step_id, side, "candidate_clear", frontier_vertices.size(), 0,
		                  cleared_candidate_words * CANDIDATE_WORD_BITS, 0, 0, 0, duration_ms);
	}

	auto prepare_start_time = std::chrono::high_resolution_clock::now();
	if (worker_id == 0) {
		state->partition_counter = 0;
		state->local_csr_counter = 0;
		state->change = false;
		state->last_side_changed = false;
		state->candidate_dirty_word_count = 0;
		state->use_candidate_check =
		    frontier_vertices.size() * CANDIDATE_FRONTIER_GATE <= static_cast<idx_t>(state->v_size);
		for (auto &meet_mask : state->worker_meet_masks) {
			meet_mask.reset();
		}
		for (auto &frontier_count : state->worker_frontier_counts) {
			frontier_count = 0;
		}
		for (auto &worker_frontier_vertices : state->worker_frontier_vertices) {
			worker_frontier_vertices.clear();
		}
	}
	if (worker_id == 0 && state->benchmark_enabled) {
		auto prepare_end_time = std::chrono::high_resolution_clock::now();
		auto duration_ms =
		    std::chrono::duration<double, std::milli>(prepare_end_time - prepare_start_time).count();
		RecordPhaseTiming(batch_id, step_id, side, "prepare", frontier_vertices.size(), 0, 0, 0, 0, 0,
		                  duration_ms);
	}
	TimedBarrier(batch_id, step_id, side, "barrier_after_prepare");

	idx_t cleared_partitions = 0;
	idx_t cleared_vertices = 0;
	auto clear_start_time = std::chrono::high_resolution_clock::now();
	while (true) {
		auto partition_idx = state->partition_counter.fetch_add(1);
		if (partition_idx >= static_cast<int64_t>(partition_csrs.size())) {
			break;
		}
		auto &local_csr = partition_csrs[partition_idx];
		cleared_partitions++;
		cleared_vertices += local_csr->end_vertex - local_csr->start_vertex;
		for (auto i = local_csr->start_vertex; i < local_csr->end_vertex; i++) {
			next[i] = 0;
		}
	}
	if (state->benchmark_enabled) {
		auto clear_end_time = std::chrono::high_resolution_clock::now();
		auto duration_ms = std::chrono::duration<double, std::milli>(clear_end_time - clear_start_time).count();
		RecordPhaseTiming(batch_id, step_id, side, "clear_next", frontier_vertices.size(), cleared_partitions,
		                  cleared_vertices, 0, 0, 0, duration_ms);
	}
	TimedBarrier(batch_id, step_id, side, "barrier_after_clear");

	auto prepare_explore_start_time = std::chrono::high_resolution_clock::now();
	if (worker_id == 0) {
		state->partition_counter = 0;
	}
	if (worker_id == 0 && state->benchmark_enabled) {
		auto prepare_explore_end_time = std::chrono::high_resolution_clock::now();
		auto duration_ms =
		    std::chrono::duration<double, std::milli>(prepare_explore_end_time - prepare_explore_start_time).count();
		RecordPhaseTiming(batch_id, step_id, side, "prepare_explore", frontier_vertices.size(), 0, 0, 0, 0, 0,
		                  duration_ms);
	}
	TimedBarrier(batch_id, step_id, side, "barrier_after_prepare_explore");

	idx_t explored_partitions = 0;
	idx_t explored_edges = 0;
	auto explore_start_time = std::chrono::high_resolution_clock::now();
	while (true) {
		auto partition_idx = state->local_csr_counter.fetch_add(1);
		if (partition_idx >= static_cast<int64_t>(partition_csrs.size())) {
			break;
		}
		auto local_csr = partition_csrs[partition_idx].get();
		if (!local_csr) {
			throw InternalException("Tried to reference nullptr for LocalCSR");
		}
		explored_partitions++;
		explored_edges += RunExplore(visit, next, *local_csr, frontier_vertices);
	}
	if (state->benchmark_enabled) {
		auto explore_end_time = std::chrono::high_resolution_clock::now();
		auto duration_ms =
		    std::chrono::duration<double, std::milli>(explore_end_time - explore_start_time).count();
		RecordPhaseTiming(batch_id, step_id, side, "explore", frontier_vertices.size(), explored_partitions, 0,
		                  explored_edges, 0, 0, duration_ms);
	}
	TimedBarrier(batch_id, step_id, side, "barrier_after_explore");

	auto prepare_check_start_time = std::chrono::high_resolution_clock::now();
	if (worker_id == 0) {
		state->local_csr_counter = 0;
		state->partition_counter = 0;
		if (state->use_candidate_check) {
			for (const auto &worker_dirty_words : state->worker_dirty_candidate_words) {
				state->candidate_dirty_word_count += worker_dirty_words.size();
			}
			state->use_candidate_check =
			    state->candidate_dirty_word_count * CANDIDATE_WORD_BITS <= static_cast<idx_t>(state->v_size);
		}
	}
	if (worker_id == 0 && state->benchmark_enabled) {
		auto prepare_check_end_time = std::chrono::high_resolution_clock::now();
		auto duration_ms =
		    std::chrono::duration<double, std::milli>(prepare_check_end_time - prepare_check_start_time).count();
		RecordPhaseTiming(batch_id, step_id, side, "prepare_check", frontier_vertices.size(), 0, 0, 0, 0, 0,
		                  duration_ms);
	}
	TimedBarrier(batch_id, step_id, side, "barrier_after_prepare_check");

	idx_t checked_partitions = 0;
	idx_t checked_vertices = 0;
	auto frontier_count_before_check = state->worker_frontier_counts[worker_id];
	auto check_start_time = std::chrono::high_resolution_clock::now();
	while (true) {
		auto partition_idx = state->partition_counter.fetch_add(1);
		if (partition_idx >= static_cast<int64_t>(partition_csrs.size())) {
			break;
		}
		auto &local_csr = partition_csrs[partition_idx];
		checked_partitions++;
		if (state->use_candidate_check) {
			checked_vertices += CheckCandidateChange(side_seen, next, other_seen, local_csr);
		} else {
			checked_vertices += local_csr->end_vertex - local_csr->start_vertex;
			CheckChange(side_seen, next, other_seen, local_csr);
		}
	}
	if (state->benchmark_enabled) {
		auto check_end_time = std::chrono::high_resolution_clock::now();
		auto duration_ms = std::chrono::duration<double, std::milli>(check_end_time - check_start_time).count();
		auto new_frontier_count = state->worker_frontier_counts[worker_id] - frontier_count_before_check;
		RecordPhaseTiming(batch_id, step_id, side, "check_change", frontier_vertices.size(), checked_partitions,
		                  checked_vertices, 0, new_frontier_count, 0, duration_ms);
	}
	TimedBarrier(batch_id, step_id, side, "barrier_after_check");

	auto aggregate_start_time = std::chrono::high_resolution_clock::now();
	if (worker_id == 0) {
		std::bitset<LANE_LIMIT> found_lanes;
		idx_t frontier_size = 0;
		idx_t completed_lanes = 0;
		for (const auto &meet_mask : state->worker_meet_masks) {
			found_lanes |= meet_mask;
		}
		frontier_vertices.clear();
		for (const auto &worker_frontier_vertices : state->worker_frontier_vertices) {
			frontier_vertices.insert(frontier_vertices.end(), worker_frontier_vertices.begin(),
			                         worker_frontier_vertices.end());
		}
		side_depth++;
		state->last_side_changed = state->change;
		completed_lanes = CompleteFoundLanes(found_lanes, state->src_depth + state->dst_depth);
		if (state->active == 0) {
			state->last_side_changed = false;
			side_frontier_size = 0;
		} else {
			frontier_size = 0;
			auto write = frontier_vertices.begin();
			for (auto vertex : frontier_vertices) {
				next[vertex] &= state->lane_active;
				if (next[vertex].any()) {
					frontier_size += next[vertex].count();
					*write++ = vertex;
				}
			}
			frontier_vertices.erase(write, frontier_vertices.end());
			side_frontier_size = frontier_size;
		}
		state->expand_source_next = state->src_frontier_size <= state->dst_frontier_size;
		if (state->benchmark_enabled) {
			auto aggregate_end_time = std::chrono::high_resolution_clock::now();
			auto duration_ms =
			    std::chrono::duration<double, std::milli>(aggregate_end_time - aggregate_start_time).count();
			RecordPhaseTiming(batch_id, step_id, side, "aggregate_prune", frontier_vertices.size(), 0, 0, 0,
			                  frontier_size, completed_lanes, duration_ms);
		}
		state->current_step_id++;
	}
	TimedBarrier(batch_id, step_id, side, "barrier_after_aggregate");
}

void BidirectionalIterativeLengthTask::CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen,
                                                   std::vector<std::bitset<LANE_LIMIT>> &next,
                                                   std::vector<std::bitset<LANE_LIMIT>> &other_seen,
                                                   shared_ptr<LocalCSR> &local_csr) const {
	std::bitset<LANE_LIMIT> found_lanes;
	idx_t local_frontier_count = 0;
	bool local_change = false;
	if (state->lane_active.all()) {
		for (auto i = local_csr->start_vertex; i < local_csr->end_vertex; i++) {
			if (next[i].any()) {
				next[i] &= ~seen[i];
				if (next[i].any()) {
					found_lanes |= next[i] & other_seen[i];
					seen[i] |= next[i];
					if (state->benchmark_enabled) {
						local_frontier_count += next[i].count();
					}
					state->worker_frontier_vertices[worker_id].push_back(i);
					local_change = true;
				}
			}
		}
	} else {
		for (auto i = local_csr->start_vertex; i < local_csr->end_vertex; i++) {
			if (next[i].any()) {
				next[i] &= ~seen[i];
				next[i] &= state->lane_active;
				if (next[i].any()) {
					found_lanes |= next[i] & other_seen[i];
					seen[i] |= next[i];
					if (state->benchmark_enabled) {
						local_frontier_count += next[i].count();
					}
					state->worker_frontier_vertices[worker_id].push_back(i);
					local_change = true;
				}
			}
		}
	}

	if (local_change) {
		std::lock_guard<std::mutex> guard(state->change_lock);
		state->change = true;
	}
	state->worker_meet_masks[worker_id] |= found_lanes;
	if (state->benchmark_enabled) {
		state->worker_frontier_counts[worker_id] += local_frontier_count;
	}
}

idx_t BidirectionalIterativeLengthTask::CheckCandidateChange(std::vector<std::bitset<LANE_LIMIT>> &seen,
                                                             std::vector<std::bitset<LANE_LIMIT>> &next,
                                                             std::vector<std::bitset<LANE_LIMIT>> &other_seen,
                                                             shared_ptr<LocalCSR> &local_csr) const {
	std::bitset<LANE_LIMIT> found_lanes;
	idx_t local_frontier_count = 0;
	idx_t checked_vertices = 0;
	bool local_change = false;
	auto all_lanes_active = state->lane_active.all();

	auto start_word = local_csr->start_vertex / CANDIDATE_WORD_BITS;
	auto end_word = (local_csr->end_vertex + CANDIDATE_WORD_BITS - 1) / CANDIDATE_WORD_BITS;
	auto start_bit = local_csr->start_vertex % CANDIDATE_WORD_BITS;
	auto end_bit = local_csr->end_vertex % CANDIDATE_WORD_BITS;

	for (auto word_idx = start_word; word_idx < end_word; word_idx++) {
		uint64_t candidate_word = 0;
		for (idx_t worker = 0; worker < state->tasks_scheduled; worker++) {
			candidate_word |= state->worker_candidate_words[worker][word_idx];
		}
		if (word_idx == start_word && start_bit != 0) {
			candidate_word &= ~0ULL << start_bit;
		}
		if (word_idx + 1 == end_word && end_bit != 0) {
			candidate_word &= (1ULL << end_bit) - 1;
		}

		while (candidate_word != 0) {
			auto bit = static_cast<idx_t>(__builtin_ctzll(candidate_word));
			auto vertex = word_idx * CANDIDATE_WORD_BITS + bit;
			candidate_word &= candidate_word - 1;
			checked_vertices++;

			if (all_lanes_active) {
				next[vertex] &= ~seen[vertex];
				if (next[vertex].any()) {
					found_lanes |= next[vertex] & other_seen[vertex];
					seen[vertex] |= next[vertex];
					if (state->benchmark_enabled) {
						local_frontier_count += next[vertex].count();
					}
					state->worker_frontier_vertices[worker_id].push_back(vertex);
					local_change = true;
				}
			} else {
				next[vertex] &= ~seen[vertex];
				next[vertex] &= state->lane_active;
				if (next[vertex].any()) {
					found_lanes |= next[vertex] & other_seen[vertex];
					seen[vertex] |= next[vertex];
					if (state->benchmark_enabled) {
						local_frontier_count += next[vertex].count();
					}
					state->worker_frontier_vertices[worker_id].push_back(vertex);
					local_change = true;
				}
			}
		}
	}

	if (local_change) {
		std::lock_guard<std::mutex> guard(state->change_lock);
		state->change = true;
	}
	state->worker_meet_masks[worker_id] |= found_lanes;
	if (state->benchmark_enabled) {
		state->worker_frontier_counts[worker_id] += local_frontier_count;
	}
	return checked_vertices;
}

idx_t BidirectionalIterativeLengthTask::CompleteFoundLanes(std::bitset<LANE_LIMIT> found_lanes,
                                                          int64_t path_length) const {
	auto result_data = FlatVector::GetDataMutable<int64_t>(state->pf_results->data[0]);
	found_lanes &= state->lane_active;
	idx_t completed_lanes = 0;
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
			completed_lanes++;
		}
	}
	return completed_lanes;
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

void BidirectionalIterativeLengthTask::RecordGlobalPhaseTiming(idx_t batch_id, idx_t step_id, const string &phase,
                                                               idx_t completed_lanes, double time_ms) const {
	if (!state->benchmark_enabled) {
		return;
	}
	std::lock_guard<std::mutex> guard(state->phase_timing_lock);
	state->bidirectional_phase_timing_data.push_back(
	    {batch_id, step_id, "batch", phase, worker_id, state->num_threads, state->active, state->src_depth,
	     state->dst_depth, state->src_frontier_size, state->dst_frontier_size, 0, 0, 0, 0, 0, completed_lanes,
	     time_ms});
}

void BidirectionalIterativeLengthTask::RecordPhaseTiming(idx_t batch_id, idx_t step_id, BidirectionalSearchSide side,
                                                         const string &phase, idx_t frontier_vertices,
                                                         idx_t partitions, idx_t vertices, idx_t edges,
                                                         idx_t new_frontier_count, idx_t completed_lanes,
                                                         double time_ms) const {
	if (!state->benchmark_enabled) {
		return;
	}
	std::lock_guard<std::mutex> guard(state->phase_timing_lock);
	state->bidirectional_phase_timing_data.push_back(
	    {batch_id, step_id, SideToString(side), phase, worker_id, state->num_threads, state->active, state->src_depth,
	     state->dst_depth, state->src_frontier_size, state->dst_frontier_size, frontier_vertices, partitions, vertices,
	     edges, new_frontier_count, completed_lanes, time_ms});
}

void BidirectionalIterativeLengthTask::TimedBarrier(idx_t batch_id, idx_t step_id, BidirectionalSearchSide side,
                                                    const string &phase) {
	if (!state->benchmark_enabled) {
		state->barrier->Wait(worker_id);
		return;
	}

	auto start_time = std::chrono::high_resolution_clock::now();
	state->barrier->Wait(worker_id);
	auto end_time = std::chrono::high_resolution_clock::now();
	auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
	RecordPhaseTiming(batch_id, step_id, side, phase, 0, 0, 0, 0, 0, 0, duration_ms);
}

idx_t BidirectionalIterativeLengthTask::Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                               std::vector<std::bitset<LANE_LIMIT>> &next,
                                               const LocalCSR &local_csr,
                                               const std::vector<idx_t> &frontier_vertices) {
	idx_t explored_edges = 0;
	auto &candidate_words = state->worker_candidate_words[worker_id];
	auto &dirty_candidate_words = state->worker_dirty_candidate_words[worker_id];
	if (state->lane_active.all() && state->use_candidate_check) {
		for (const auto i : frontier_vertices) {
			if (visit[i].any()) {
				uint32_t start_edges;
				uint32_t end_edges;
				if (!local_csr.GetRowEdges(i, start_edges, end_edges)) {
					continue;
				}
				if (state->benchmark_enabled) {
					explored_edges += end_edges - start_edges;
				}
				for (auto offset = start_edges; offset < end_edges; offset++) {
					auto n = local_csr.e[offset] + local_csr.start_vertex;
					auto word_idx = n / CANDIDATE_WORD_BITS;
					auto word_mask = 1ULL << (n % CANDIDATE_WORD_BITS);
					if (candidate_words[word_idx] == 0) {
						dirty_candidate_words.push_back(word_idx);
					}
					candidate_words[word_idx] |= word_mask;
					next[n] |= visit[i];
				}
			}
		}
	} else if (state->lane_active.all()) {
		for (const auto i : frontier_vertices) {
			if (visit[i].any()) {
				uint32_t start_edges;
				uint32_t end_edges;
				if (!local_csr.GetRowEdges(i, start_edges, end_edges)) {
					continue;
				}
				if (state->benchmark_enabled) {
					explored_edges += end_edges - start_edges;
				}
				for (auto offset = start_edges; offset < end_edges; offset++) {
					auto n = local_csr.e[offset] + local_csr.start_vertex;
					next[n] |= visit[i];
				}
			}
		}
	} else if (state->use_candidate_check) {
		for (const auto i : frontier_vertices) {
			auto active_visit = visit[i] & state->lane_active;
			if (active_visit.any()) {
				uint32_t start_edges;
				uint32_t end_edges;
				if (!local_csr.GetRowEdges(i, start_edges, end_edges)) {
					continue;
				}
				if (state->benchmark_enabled) {
					explored_edges += end_edges - start_edges;
				}
				for (auto offset = start_edges; offset < end_edges; offset++) {
					auto n = local_csr.e[offset] + local_csr.start_vertex;
					auto word_idx = n / CANDIDATE_WORD_BITS;
					auto word_mask = 1ULL << (n % CANDIDATE_WORD_BITS);
					if (candidate_words[word_idx] == 0) {
						dirty_candidate_words.push_back(word_idx);
					}
					candidate_words[word_idx] |= word_mask;
					next[n] |= active_visit;
				}
			}
		}
	} else {
		for (const auto i : frontier_vertices) {
			auto active_visit = visit[i] & state->lane_active;
			if (active_visit.any()) {
				uint32_t start_edges;
				uint32_t end_edges;
				if (!local_csr.GetRowEdges(i, start_edges, end_edges)) {
					continue;
				}
				if (state->benchmark_enabled) {
					explored_edges += end_edges - start_edges;
				}
				for (auto offset = start_edges; offset < end_edges; offset++) {
					auto n = local_csr.e[offset] + local_csr.start_vertex;
					next[n] |= active_visit;
				}
			}
		}
	}
	return explored_edges;
}

idx_t BidirectionalIterativeLengthTask::RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit,
                                                  std::vector<std::bitset<LANE_LIMIT>> &next,
                                                  const LocalCSR &local_csr,
                                                  const std::vector<idx_t> &frontier_vertices) {
	if (!state->benchmark_enabled) {
		Explore(visit, next, local_csr, frontier_vertices);
		return 0;
	}

	auto start_time = std::chrono::high_resolution_clock::now();
	auto explored_edges = Explore(visit, next, local_csr, frontier_vertices);
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
	                                local_csr.e.size(), state->local_csrs.size(), state->src_depth + state->dst_depth);
	return explored_edges;
}

} // namespace duckdb
