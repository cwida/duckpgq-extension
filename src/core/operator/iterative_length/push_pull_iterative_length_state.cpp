#include "duckpgq/core/operator/iterative_length/push_pull_iterative_length_state.hpp"

#include <duckpgq/core/operator/iterative_length/push_pull_iterative_length_event.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>
#include <algorithm>
#include <filesystem>
#include <fstream>

namespace duckdb {

PushPullIterativeLengthState::PushPullIterativeLengthState(const shared_ptr<DataChunk> &pairs_,
                                                           std::vector<shared_ptr<LocalCSR>> &local_csrs_,
                                                           std::vector<shared_ptr<LocalCSR>> &reverse_local_csrs_,
                                                           idx_t num_threads_, ClientContext &context_, int64_t vsize_)
    : BFSState(pairs_, local_csrs_, num_threads_, "pushpulliterativelength", context_, vsize_),
      reverse_local_csrs(reverse_local_csrs_) {
	frontier_vertices = 0;
	pull_frontier_gate = std::max<idx_t>(1, static_cast<idx_t>(GetPathFindingPushPullFrontierGate(context_)));
	use_pull = false;
}

void PushPullIterativeLengthState::Clear() {
	iter = 1;
	active = 0;
	change = false;
	frontier_vertices = 0;
	use_pull = false;
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
		for (const auto &entry : timing_data) {
			file << std::get<0>(entry) << "," << std::get<1>(entry) << "," << std::get<2>(entry) << ","
			     << std::get<3>(entry) << "," << std::get<4>(entry) << "," << std::get<5>(entry) << ","
			     << std::get<6>(entry) << "," << std::get<7>(entry) << "\n";
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
		file << "RunID,Iter,Mode,ActiveLanes,FrontierVertices,VertexCount,PullGate\n";
	}
	for (const auto &entry : iteration_stats) {
		file << benchmark_run_id << "," << entry.iteration << "," << entry.mode << "," << entry.active_lanes << ","
		     << entry.frontier_vertices << "," << entry.vertex_count << "," << entry.pull_gate << "\n";
	}
}

} // namespace duckdb
