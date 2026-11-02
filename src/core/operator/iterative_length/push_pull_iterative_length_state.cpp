#include "duckpgq/core/operator/iterative_length/push_pull_iterative_length_state.hpp"

#include <duckpgq/core/operator/iterative_length/push_pull_iterative_length_task.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>
#include <algorithm>
#include <filesystem>
#include <fstream>

namespace duckdb {

PushPullIterativeLengthState::PushPullIterativeLengthState(const shared_ptr<PathFindingBatch> &batch_,
                                                           std::vector<shared_ptr<LocalCSR>> &local_csrs_,
                                                           std::vector<shared_ptr<PullCSR>> &pull_local_csrs_,
                                                           idx_t num_threads_, ClientContext &context_, int64_t vsize_)
    : BFSState(batch_, local_csrs_, num_threads_, "pushpulliterativelength", context_, vsize_),
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
	worker_changed.resize(num_threads_, 0);
	pull_block_counter = 0;
	current_batch = 0;
	frontier_vertices = 0;
	pull_frontier_gate = std::max<idx_t>(1, static_cast<idx_t>(GetPathFindingPushPullFrontierGate(context_)));
	use_pull = false;
	continue_search = false;
	has_more_batches = false;
}

void PushPullIterativeLengthState::Clear() {
	// The working set keeps the last batch's bits: the INITIALIZE phase of the next batch clears them on the workers
	iter = 1;
	active = 0;
	change = false;
	frontier_vertices = 0;
	use_pull = false;
	continue_search = false;
}

PushPullIterativeLengthState::~PushPullIterativeLengthState() {
}

shared_ptr<Event> PushPullIterativeLengthState::CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op) {
	auto self = shared_ptr_cast<BFSState, PushPullIterativeLengthState>(shared_from_this());
	return make_shared_ptr<PhasedKernelEvent>(shared_ptr<PhasedKernel>(self), pipeline, *op);
}

//===--------------------------------------------------------------------===//
// PhasedKernel
//===--------------------------------------------------------------------===//
void PushPullIterativeLengthState::BeginIteration() {
	change = false;
	std::fill(worker_changed.begin(), worker_changed.end(), 0);
	partition_counter = 0;
	local_csr_counter = 0;
	pull_block_counter = 0;
	phase = Phase::PREPARE;
}

void PushPullIterativeLengthState::BeginBatch() {
	current_batch++;
	PhaseTimer timer(benchmark_enabled);
	AssignLanes();
	workers[0]->RecordPhaseTiming("initialize_lanes", "batch", iter, 0, pairs->size(), 0, active, 0, timer.ElapsedMs());
	phase = Phase::INITIALIZE;
}

bool PushPullIterativeLengthState::Start() {
	phase_start_time = std::chrono::steady_clock::now();
	idx_t num_partitions = std::max(local_csrs.size(), pull_local_csrs.size());
	worker_count = std::max<idx_t>(1, std::min<idx_t>(num_threads, std::max<idx_t>(1, num_partitions)));
	auto self = shared_ptr_cast<BFSState, PushPullIterativeLengthState>(shared_from_this());
	for (idx_t worker = 0; worker < worker_count; worker++) {
		workers.push_back(make_uniq<PushPullIterativeLengthWorker>(self, worker));
	}
	EnsureWorkingSet();
	has_more_batches = started_searches < pairs->size();
	if (!has_more_batches) {
		return false;
	}
	BeginBatch();
	return true;
}

idx_t PushPullIterativeLengthState::WorkerCount() const {
	return worker_count;
}

void PushPullIterativeLengthState::RunPhase(idx_t worker_id) {
	auto iteration = static_cast<idx_t>(iter);
	switch (phase) {
	case Phase::INITIALIZE:
		workers[worker_id]->InitializePhase();
		break;
	case Phase::PREPARE:
		workers[worker_id]->PreparePhase();
		break;
	case Phase::PUSH_EXPLORE:
		workers[worker_id]->PushExplore(iteration);
		break;
	case Phase::PUSH_CHECK:
		workers[worker_id]->PushCheck(iteration);
		break;
	case Phase::PULL:
		workers[worker_id]->Pull(iteration);
		break;
	}
}

bool PushPullIterativeLengthState::AdvancePhase() {
	auto iteration = static_cast<idx_t>(iter);
	switch (phase) {
	case Phase::INITIALIZE:
		MarkLaneSources();
		BeginIteration();
		return true;
	case Phase::PREPARE:
		workers[0]->DecideMode();
		phase = use_pull ? Phase::PULL : Phase::PUSH_EXPLORE;
		return true;
	case Phase::PUSH_EXPLORE:
		partition_counter = 0;
		phase = Phase::PUSH_CHECK;
		return true;
	case Phase::PUSH_CHECK:
	case Phase::PULL:
		break;
	}
	// The iteration is complete
	partition_counter = 0;
	local_csr_counter = 0;
	for (idx_t worker = 0; worker < worker_count; worker++) {
		change |= worker_changed[worker] != 0;
	}
	auto mode_name = use_pull ? "pull" : "push";
	PhaseTimer reach_timer(benchmark_enabled);
	ReachDetect();
	workers[0]->RecordPhaseTiming("reach_detect", mode_name, iteration, 0, LANE_LIMIT, 0, active, 0,
	                              reach_timer.ElapsedMs());
	continue_search = change;
	if (continue_search) {
		BeginIteration();
		return true;
	}
	PhaseTimer unreachable_timer(benchmark_enabled);
	UnReachableSet();
	workers[0]->RecordPhaseTiming("unreachable_set", "batch", iter, 0, LANE_LIMIT, 0, 0, 0,
	                              unreachable_timer.ElapsedMs());
	PhaseTimer clear_timer(benchmark_enabled);
	Clear();
	workers[0]->RecordPhaseTiming("clear_state", "batch", iter, 0, 0, 0, 0, 0, clear_timer.ElapsedMs());
	has_more_batches = started_searches < pairs->size();
	if (!has_more_batches) {
		return false;
	}
	BeginBatch();
	return true;
}

void PushPullIterativeLengthState::Finished() {
	ReleaseWorkingSet();
	if (!benchmark_enabled) {
		return;
	}
	AppendBatchPhaseTiming("bfs_batch", ElapsedMs(phase_start_time));
	auto heavy_partition_fraction = std::to_string(GetHeavyPartitionFraction(context));
	auto light_partition_multiplier = std::to_string(GetLightPartitionMultiplier(context));
	auto file_name = benchmark_output_prefix + "_pushpull_explore_timing_" + benchmark_run_id + "_threads_" +
	                 std::to_string(num_threads) + "_" + heavy_partition_fraction + "_" + light_partition_multiplier +
	                 ".csv";
	vector<ExploreTiming> timings;
	for (const auto &worker_entries : worker_timing_data) {
		timings.insert(timings.end(), worker_entries.begin(), worker_entries.end());
	}
	WriteExploreTimings(file_name, timings);
	auto iteration_stats_file_name = benchmark_output_prefix + "_pushpull_iteration_stats.csv";
	WriteIterationStats(iteration_stats_file_name);
	auto phase_detail_file_name = benchmark_output_prefix + "_pushpull_phase_detail.csv";
	WritePhaseTimingResults(phase_detail_file_name);
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
			     << entry.phase << "," << entry.worker_id << "," << entry.active_lanes << "," << entry.frontier_vertices
			     << "," << entry.vertex_count << "," << entry.partition_count << "," << entry.vertices << ","
			     << entry.edges << "," << entry.candidates << "," << entry.changed_vertices << "," << entry.time_ms
			     << "\n";
		}
	}
}

} // namespace duckdb
