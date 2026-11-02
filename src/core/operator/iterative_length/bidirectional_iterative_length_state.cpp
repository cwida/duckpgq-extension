#include "duckpgq/core/operator/iterative_length/bidirectional_iterative_length_state.hpp"

#include <duckpgq/core/operator/iterative_length/bidirectional_iterative_length_task.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <algorithm>
#include <fstream>
#include <mutex>

namespace duckdb {

static mutex bidirectional_phase_detail_timing_lock;

BidirectionalIterativeLengthState::BidirectionalIterativeLengthState(
    const shared_ptr<PathFindingBatch> &batch_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
    std::vector<shared_ptr<LocalCSR>> &reverse_local_csrs_, idx_t num_threads_, ClientContext &context_, int64_t vsize_)
    : BFSState(batch_, local_csrs_, num_threads_, PathFindingOperatorMode::BIDIRECTIONAL_ITERATIVE_LENGTH, context_,
               vsize_, PathFindingSettings(context_)),
      reverse_local_csrs(reverse_local_csrs_) {
	src_seen = vector<std::bitset<LANE_LIMIT>>(v_size);
	src_visit1 = vector<std::bitset<LANE_LIMIT>>(v_size);
	src_visit2 = vector<std::bitset<LANE_LIMIT>>(v_size);
	dst_seen = vector<std::bitset<LANE_LIMIT>>(v_size);
	dst_visit1 = vector<std::bitset<LANE_LIMIT>>(v_size);
	dst_visit2 = vector<std::bitset<LANE_LIMIT>>(v_size);
	worker_meet_masks = vector<std::bitset<LANE_LIMIT>>(num_threads);
	worker_changed = vector<uint8_t>(num_threads, 0);
	worker_frontier_counts = vector<idx_t>(num_threads);
	worker_frontier_vertices = vector<vector<idx_t>>(num_threads);
	candidate_word_count = (v_size + 63) / 64;
	worker_candidate_words = vector<vector<uint64_t>>(num_threads, vector<uint64_t>(candidate_word_count));
	worker_dirty_candidate_words = vector<vector<idx_t>>(num_threads);
	candidate_dirty_word_count = 0;
	use_candidate_check = true;
	current_batch_id = 0;
	current_step_id = 0;
	src_depth = 0;
	dst_depth = 0;
	src_frontier_size = 0;
	dst_frontier_size = 0;
	last_side_changed = false;
	has_more_batches = false;
	continue_search = false;
	expand_source_next = true;
}

void BidirectionalIterativeLengthState::InitializeBidirectionalLanes() {
	auto &result_validity = FlatVector::ValidityMutable(pf_results->data[0]);
	current_batch_id++;
	current_step_id = 0;
	active = 0;
	lane_active.reset();
	src_depth = 0;
	dst_depth = 0;
	last_side_changed = false;
	src_frontier_size = 0;
	dst_frontier_size = 0;
	expand_source_next = true;
	candidate_dirty_word_count = 0;
	use_candidate_check = true;
	src_frontier_vertices.clear();
	dst_frontier_vertices.clear();

	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		lane_to_num[lane] = -1;
		while (started_searches < pairs->size()) {
			auto search_num = started_searches++;
			int64_t src_pos = vdata_src.sel->get_index(search_num);
			int64_t dst_pos = vdata_dst.sel->get_index(search_num);
			if (!vdata_src.validity.RowIsValid(src_pos) || !vdata_dst.validity.RowIsValid(dst_pos)) {
				result_validity.SetInvalid(search_num);
			} else if (!IsVertexInRange(src[src_pos]) || !IsVertexInRange(dst[dst_pos])) {
				result_validity.SetInvalid(search_num);
			} else if (src[src_pos] == dst[dst_pos]) {
				pf_results->data[0].SetValue(search_num, 0);
			} else {
				src_visit1[src[src_pos]][lane] = true;
				dst_visit1[dst[dst_pos]][lane] = true;
				src_seen[src[src_pos]][lane] = true;
				dst_seen[dst[dst_pos]][lane] = true;
				src_frontier_vertices.push_back(src[src_pos]);
				dst_frontier_vertices.push_back(dst[dst_pos]);
				lane_to_num[lane] = search_num;
				lane_active[lane] = true;
				active++;
				src_frontier_size++;
				dst_frontier_size++;
				break;
			}
		}
	}
	std::sort(src_frontier_vertices.begin(), src_frontier_vertices.end());
	src_frontier_vertices.erase(std::unique(src_frontier_vertices.begin(), src_frontier_vertices.end()),
	                            src_frontier_vertices.end());
	std::sort(dst_frontier_vertices.begin(), dst_frontier_vertices.end());
	dst_frontier_vertices.erase(std::unique(dst_frontier_vertices.begin(), dst_frontier_vertices.end()),
	                            dst_frontier_vertices.end());
}

void BidirectionalIterativeLengthState::Clear() {
	src_depth = 0;
	dst_depth = 0;
	active = 0;
	change = false;
	last_side_changed = false;
	has_more_batches = false;
	continue_search = false;
	current_step_id = 0;
	src_frontier_size = 0;
	dst_frontier_size = 0;
	expand_source_next = true;
	candidate_dirty_word_count = 0;
	use_candidate_check = true;
	src_frontier_vertices.clear();
	dst_frontier_vertices.clear();
	for (idx_t worker = 0; worker < worker_dirty_candidate_words.size(); worker++) {
		for (auto word_idx : worker_dirty_candidate_words[worker]) {
			worker_candidate_words[worker][word_idx] = 0;
		}
		worker_dirty_candidate_words[worker].clear();
	}
	for (auto i = 0; i < v_size; i++) {
		src_seen[i] = 0;
		src_visit1[i] = 0;
		src_visit2[i] = 0;
		dst_seen[i] = 0;
		dst_visit1[i] = 0;
		dst_visit2[i] = 0;
	}
	for (auto &meet_mask : worker_meet_masks) {
		meet_mask.reset();
	}
	std::fill(worker_changed.begin(), worker_changed.end(), 0);
	for (auto &frontier_count : worker_frontier_counts) {
		frontier_count = 0;
	}
	for (auto &frontier_vertices : worker_frontier_vertices) {
		frontier_vertices.clear();
	}
	lane_active.reset();
}

BidirectionalIterativeLengthState::~BidirectionalIterativeLengthState() {
}

shared_ptr<Event> BidirectionalIterativeLengthState::CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op) {
	auto self = shared_ptr_cast<BFSState, BidirectionalIterativeLengthState>(shared_from_this());
	return make_shared_ptr<PhasedKernelEvent>(shared_ptr<PhasedKernel>(self), pipeline, *op);
}

//===--------------------------------------------------------------------===//
// PhasedKernel
//===--------------------------------------------------------------------===//
bool BidirectionalIterativeLengthState::BeginStep() {
	side = expand_source_next ? BidirectionalSearchSide::SOURCE : BidirectionalSearchSide::DESTINATION;
	workers[0]->PrepareSide(side);
	phase = Phase::CLEAR;
	return true;
}

bool BidirectionalIterativeLengthState::FinishBatch() {
	while (true) {
		PhaseTimer unreachable_timer(benchmark_enabled);
		UnReachableSet();
		workers[0]->RecordGlobalPhaseTiming(current_batch_id, current_step_id, "unreachable_set", 0,
		                                    unreachable_timer.ElapsedMs());
		auto batch_id = current_batch_id;
		auto step_id = current_step_id;
		PhaseTimer clear_timer(benchmark_enabled);
		Clear();
		workers[0]->RecordGlobalPhaseTiming(batch_id, step_id, "state_clear", 0, clear_timer.ElapsedMs());
		has_more_batches = started_searches < pairs->size();
		if (!has_more_batches) {
			return false;
		}
		PhaseTimer initialize_timer(benchmark_enabled);
		InitializeBidirectionalLanes();
		workers[0]->RecordGlobalPhaseTiming(current_batch_id, 0, "batch_initialize", 0, initialize_timer.ElapsedMs());
		if (active > 0) {
			return BeginStep();
		}
		// Nothing to search in this batch (every pair answered at initialization): finish it right away
	}
}

bool BidirectionalIterativeLengthState::Start() {
	phase_start_time = std::chrono::steady_clock::now();
	idx_t num_partitions = std::max(local_csrs.size(), reverse_local_csrs.size());
	worker_count = std::max<idx_t>(1, std::min<idx_t>(num_threads, std::max<idx_t>(1, num_partitions)));
	auto self = shared_ptr_cast<BFSState, BidirectionalIterativeLengthState>(shared_from_this());
	for (idx_t worker = 0; worker < worker_count; worker++) {
		workers.push_back(make_uniq<BidirectionalIterativeLengthWorker>(self, worker));
	}
	has_more_batches = started_searches < pairs->size();
	if (!has_more_batches) {
		return false;
	}
	PhaseTimer initialize_timer(benchmark_enabled);
	InitializeBidirectionalLanes();
	workers[0]->RecordGlobalPhaseTiming(current_batch_id, 0, "batch_initialize", 0, initialize_timer.ElapsedMs());
	if (active > 0) {
		return BeginStep();
	}
	return FinishBatch();
}

idx_t BidirectionalIterativeLengthState::WorkerCount() const {
	return worker_count;
}

void BidirectionalIterativeLengthState::RunPhase(idx_t worker_id) {
	switch (phase) {
	case Phase::CLEAR:
		workers[worker_id]->ClearPhase(side);
		break;
	case Phase::EXPLORE:
		workers[worker_id]->ExplorePhase(side);
		break;
	case Phase::CHECK:
		workers[worker_id]->CheckPhase(side);
		break;
	}
}

bool BidirectionalIterativeLengthState::AdvancePhase() {
	switch (phase) {
	case Phase::CLEAR:
		workers[0]->PrepareExplore(side);
		phase = Phase::EXPLORE;
		return true;
	case Phase::EXPLORE:
		workers[0]->PrepareCheck(side);
		phase = Phase::CHECK;
		return true;
	case Phase::CHECK:
		break;
	}
	for (idx_t worker = 0; worker < worker_count; worker++) {
		change |= worker_changed[worker] != 0;
	}
	workers[0]->Aggregate(side);
	// A lane not met after this step needs more than src_depth + dst_depth hops: none when that is the bound
	continue_search = active > 0 && last_side_changed && !DepthBoundReached(src_depth + dst_depth);
	if (continue_search) {
		return BeginStep();
	}
	return FinishBatch();
}

void BidirectionalIterativeLengthState::Finished() {
	if (!benchmark_enabled) {
		return;
	}
	AppendBatchPhaseTiming("bfs_batch", ElapsedMs(phase_start_time));
	auto heavy_partition_fraction = std::to_string(GetHeavyPartitionFraction(context));
	auto light_partition_multiplier = std::to_string(GetLightPartitionMultiplier(context));
	auto file_name = benchmark_output_prefix + "_bidirectional_explore_timing_" + benchmark_run_id + "_threads_" +
	                 std::to_string(num_threads) + "_" + heavy_partition_fraction + "_" + light_partition_multiplier +
	                 ".csv";
	WriteExploreTimings(file_name, timing_data);
	auto phase_detail_file_name = benchmark_output_prefix + "_bidirectional_phase_detail.csv";
	WritePhaseTimingResults(phase_detail_file_name);
}

void BidirectionalIterativeLengthState::WritePhaseTimingResults(const std::string &filename) {
	std::lock_guard<std::mutex> guard(bidirectional_phase_detail_timing_lock);
	std::ofstream file(filename, std::ios::app);
	if (file.is_open()) {
		if (file.tellp() == 0) {
			file << "Batch,RunID,Step,Side,Phase,WorkerID,ThreadCount,ActiveLanes,SrcDepth,DstDepth,SrcFrontierSize,"
			        "DstFrontierSize,FrontierVertices,Partitions,Vertices,Edges,NewFrontierCount,CompletedLanes,"
			        "Time_ms\n";
		}
		for (const auto &entry : bidirectional_phase_timing_data) {
			file << entry.batch_id << "," << benchmark_run_id << "," << entry.step_id << "," << entry.side << ","
			     << entry.phase << "," << entry.worker_id << "," << entry.thread_count << "," << entry.active_lanes
			     << "," << entry.src_depth << "," << entry.dst_depth << "," << entry.src_frontier_size << ","
			     << entry.dst_frontier_size << "," << entry.frontier_vertices << "," << entry.partitions << ","
			     << entry.vertices << "," << entry.edges << "," << entry.new_frontier_count << ","
			     << entry.completed_lanes << "," << entry.time_ms << "\n";
		}
		file.close();
	}
}

} // namespace duckdb
