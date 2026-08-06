#include "duckpgq/core/operator/iterative_length/push_pull_iterative_length_state.hpp"

#include <duckpgq/core/operator/iterative_length/push_pull_iterative_length_event.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>
#include <algorithm>
#include <filesystem>
#include <fstream>

namespace duckdb {

PushPullIterativeLengthState::PushPullIterativeLengthState(const shared_ptr<DataChunk> &pairs_,
                                                           std::vector<shared_ptr<LocalCSR>> &local_csrs_,
                                                           std::vector<shared_ptr<PullCSR>> &pull_local_csrs_,
                                                           idx_t num_threads_, ClientContext &context_, int64_t vsize_)
    : BFSState(pairs_, local_csrs_, num_threads_, "pushpulliterativelength", context_, vsize_),
      pull_local_csrs(pull_local_csrs_) {
	worker_timing_data.resize(num_threads_);
	phase_timing_data.resize(num_threads_);
	for (idx_t partition_idx = 0; partition_idx < pull_local_csrs.size(); partition_idx++) {
		auto vertex_count = pull_local_csrs[partition_idx]->GetVertexSize();
		for (idx_t local_start = 0; local_start < vertex_count; local_start += STANDARD_VECTOR_SIZE) {
			auto local_end = std::min<idx_t>(local_start + STANDARD_VECTOR_SIZE, vertex_count);
			pull_blocks.push_back({partition_idx, local_start, local_end});
		}
	}
	frontier_count_by_worker.resize(num_threads_, 0);
	pull_block_counter = 0;
	current_batch = 0;
	frontier_vertices = 0;
	pull_frontier_gate = std::max<idx_t>(1, static_cast<idx_t>(GetPathFindingPushPullFrontierGate(context_)));
	use_pull = false;
	continue_search = false;
	has_more_batches = false;
}

void PushPullIterativeLengthState::Clear() {
	iter = 1;
	active = 0;
	change = false;
	frontier_vertices = 0;
	use_pull = false;
	continue_search = false;
	for (auto i = 0; i < v_size; i++) {
		visit1[i] = 0;
		visit2[i] = 0;
		seen[i] = 0;
	}
	lane_completed.reset();
}

void PushPullIterativeLengthState::ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op) {
	event.InsertEvent(make_shared_ptr<PushPullIterativeLengthEvent>(
	    shared_ptr_cast<BFSState, PushPullIterativeLengthState>(shared_from_this()), pipeline, *op));
}

void PushPullIterativeLengthState::WriteTimingResults(const std::string &filename) {
	std::ofstream file(filename);
	if (file.is_open()) {
		file << "ThreadID,CoreID,Time_ms,ThreadCount,vsize,esize,numPartitions,Iter\n";
		for (const auto &worker_entries : worker_timing_data) {
			for (const auto &entry : worker_entries) {
				file << std::get<0>(entry) << "," << std::get<1>(entry) << "," << std::get<2>(entry) << ","
				     << std::get<3>(entry) << "," << std::get<4>(entry) << "," << std::get<5>(entry) << ","
				     << std::get<6>(entry) << "," << std::get<7>(entry) << "\n";
			}
		}
		file.close();
	}
}

void PushPullIterativeLengthState::WriteIterationStats(const std::string &filename) {
	std::ofstream file(filename, std::ios::app);
	if (!file.is_open()) {
		throw IOException("Could not open push/pull iteration benchmark file \"%s\"", filename);
	}
	if (!std::filesystem::exists(filename) || std::filesystem::file_size(filename) == 0) {
		file << "RunID,Batch,Iter,Mode,ActiveLanes,FrontierVertices,VertexCount,PullGate\n";
	}
	for (const auto &entry : iteration_stats) {
		file << benchmark_run_id << "," << entry.batch << "," << entry.iteration << "," << entry.mode << ","
		     << entry.active_lanes << "," << entry.frontier_vertices << "," << entry.vertex_count << ","
		     << entry.pull_gate << "\n";
	}
}

void PushPullIterativeLengthState::WritePhaseTimingResults(const std::string &filename) {
	std::ofstream file(filename, std::ios::app);
	if (!file.is_open()) {
		throw IOException("Could not open push/pull phase timing benchmark file \"%s\"", filename);
	}
	if (!std::filesystem::exists(filename) || std::filesystem::file_size(filename) == 0) {
		file << "RunID,Batch,Iter,Mode,Phase,Worker,ActiveLanes,FrontierVertices,VertexCount,PartitionCount,Vertices,"
		        "Edges,Candidates,ChangedVertices,Time_ms\n";
	}
	for (const auto &worker_entries : phase_timing_data) {
		for (const auto &entry : worker_entries) {
			file << benchmark_run_id << "," << entry.batch << "," << entry.iteration << "," << entry.mode << ","
			     << entry.phase << "," << entry.worker_id << "," << entry.active_lanes << ","
			     << entry.frontier_vertices << "," << entry.vertex_count << "," << entry.partition_count << ","
			     << entry.vertices << "," << entry.edges << "," << entry.candidates << "," << entry.changed_vertices
			     << "," << entry.time_ms << "\n";
		}
	}
}

} // namespace duckdb
