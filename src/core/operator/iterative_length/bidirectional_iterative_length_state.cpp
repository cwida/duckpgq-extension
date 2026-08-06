#include "duckpgq/core/operator/iterative_length/bidirectional_iterative_length_state.hpp"

#include <duckpgq/core/operator/iterative_length/bidirectional_iterative_length_event.hpp>
#include <algorithm>
#include <fstream>

namespace duckdb {

static mutex bidirectional_phase_detail_timing_lock;

BidirectionalIterativeLengthState::BidirectionalIterativeLengthState(
    const shared_ptr<PathFindingBatch> &batch_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
    std::vector<shared_ptr<LocalCSR>> &reverse_local_csrs_, idx_t num_threads_, ClientContext &context_, int64_t vsize_)
    : BFSState(batch_, local_csrs_, num_threads_, "bidirectionaliterativelength", context_, vsize_),
      reverse_local_csrs(reverse_local_csrs_) {
	src_seen = vector<std::bitset<LANE_LIMIT>>(v_size);
	src_visit1 = vector<std::bitset<LANE_LIMIT>>(v_size);
	src_visit2 = vector<std::bitset<LANE_LIMIT>>(v_size);
	dst_seen = vector<std::bitset<LANE_LIMIT>>(v_size);
	dst_visit1 = vector<std::bitset<LANE_LIMIT>>(v_size);
	dst_visit2 = vector<std::bitset<LANE_LIMIT>>(v_size);
	worker_meet_masks = vector<std::bitset<LANE_LIMIT>>(num_threads);
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
		visit1[i] = 0;
		visit2[i] = 0;
		seen[i] = 0;
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
	for (auto &frontier_count : worker_frontier_counts) {
		frontier_count = 0;
	}
	for (auto &frontier_vertices : worker_frontier_vertices) {
		frontier_vertices.clear();
	}
	lane_active.reset();
	lane_completed.reset();
}

void BidirectionalIterativeLengthState::ScheduleBFSBatch(Pipeline &pipeline, Event &event,
                                                         const PhysicalPathFinding *op) {
	event.InsertEvent(make_shared_ptr<BidirectionalIterativeLengthEvent>(
	    shared_ptr_cast<BFSState, BidirectionalIterativeLengthState>(shared_from_this()), pipeline, *op));
}

void BidirectionalIterativeLengthState::WriteTimingResults(const std::string &filename) {
	std::ofstream file(filename);
	if (file.is_open()) {
		file << "ThreadID,CoreID,Time_ms,ThreadCount,vsize,esize,numPartitions,Iter\n";
		for (const auto &entry : timing_data) {
			file << std::get<0>(entry) << "," << std::get<1>(entry) << "," << std::get<2>(entry) << ","
			     << std::get<3>(entry) << "," << std::get<4>(entry) << "," << std::get<5>(entry) << ","
			     << std::get<6>(entry) << "," << std::get<7>(entry) << "\n";
		}
		file.close();
	}
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
			     << entry.phase << "," << entry.worker_id << "," << entry.thread_count << ","
			     << entry.active_lanes << "," << entry.src_depth << "," << entry.dst_depth << ","
			     << entry.src_frontier_size << "," << entry.dst_frontier_size << "," << entry.frontier_vertices
			     << "," << entry.partitions << "," << entry.vertices << "," << entry.edges << ","
			     << entry.new_frontier_count << "," << entry.completed_lanes << "," << entry.time_ms << "\n";
		}
		file.close();
	}
}

} // namespace duckdb
