#include "duckpgq/core/operator/iterative_length/push_pull_iterative_length_state.hpp"

#include <duckpgq/core/operator/iterative_length/push_pull_iterative_length_task.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <chrono>
#include <mutex>
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

PushPullIterativeLengthState::~PushPullIterativeLengthState() {
}

shared_ptr<Event> PushPullIterativeLengthState::CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op) {
	auto self = shared_ptr_cast<BFSState, PushPullIterativeLengthState>(shared_from_this());
	return make_shared_ptr<PhasedKernelEvent>(shared_ptr<PhasedKernel>(self), pipeline, *op);
}

//===--------------------------------------------------------------------===//
// PhasedKernel
//===--------------------------------------------------------------------===//
namespace {

mutex push_pull_phase_timing_lock;

size_t GetLocalCSREdgeCount(const std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	size_t edge_count = 0;
	for (const auto &local_csr : partition_csrs) {
		edge_count += local_csr->GetEdgeSize();
	}
	return edge_count;
}

void AppendBFSPhaseTiming(const PushPullIterativeLengthState &state, double time_ms) {
	auto file_name = state.benchmark_output_prefix + "_phase_timing.csv";
	lock_guard<mutex> lock(push_pull_phase_timing_lock);
	bool write_header = !std::filesystem::exists(file_name);
	std::ofstream outfile(file_name, std::ios::app);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding phase benchmark file \"%s\"", file_name);
	}
	if (write_header) {
		outfile << "Phase,RunID,ThreadCount,PairCount,VertexCount,EdgeCount,PartitionCount,Time_ms,MemoryBytes\n";
	}
	outfile << "bfs_batch," << state.benchmark_run_id << "," << state.num_threads << "," << state.pairs->size() << ","
	        << state.v_size - 2 << "," << GetLocalCSREdgeCount(state.local_csrs) << "," << state.local_csrs.size()
	        << "," << time_ms << ",0\n";
}

double ElapsedMs(std::chrono::steady_clock::time_point start) {
	return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
}

} // namespace

void PushPullIterativeLengthState::BeginIteration() {
	change = false;
	partition_counter = 0;
	local_csr_counter = 0;
	pull_block_counter = 0;
	phase = Phase::PREPARE;
}

void PushPullIterativeLengthState::BeginBatch() {
	current_batch++;
	auto start_time = std::chrono::steady_clock::now();
	InitializeLanes();
	workers[0]->RecordPhaseTiming("initialize_lanes", "batch", iter, 0, pairs->size(), 0, active, 0,
	                              ElapsedMs(start_time));
	BeginIteration();
}

bool PushPullIterativeLengthState::Start() {
	phase_start_time = std::chrono::steady_clock::now();
	idx_t num_partitions = std::max(local_csrs.size(), pull_local_csrs.size());
	worker_count = std::max<idx_t>(1, std::min<idx_t>(num_threads, std::max<idx_t>(1, num_partitions)));
	tasks_scheduled = worker_count;
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
	auto mode_name = use_pull ? "pull" : "push";
	auto reach_start = std::chrono::steady_clock::now();
	workers[0]->ReachDetect();
	workers[0]->RecordPhaseTiming("reach_detect", mode_name, iteration, 0, LANE_LIMIT, 0, active, 0,
	                              ElapsedMs(reach_start));
	continue_search = change;
	if (continue_search) {
		BeginIteration();
		return true;
	}
	auto unreachable_start = std::chrono::steady_clock::now();
	workers[0]->UnReachableSet();
	workers[0]->RecordPhaseTiming("unreachable_set", "batch", iter, 0, LANE_LIMIT, 0, 0, 0,
	                              ElapsedMs(unreachable_start));
	auto clear_start = std::chrono::steady_clock::now();
	Clear();
	workers[0]->RecordPhaseTiming("clear_state", "batch", iter, 0, static_cast<idx_t>(v_size) * 3, 0, 0, 0,
	                              ElapsedMs(clear_start));
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
	auto time_ms = ElapsedMs(phase_start_time);
	AppendBFSPhaseTiming(*this, time_ms);
	auto heavy_partition_fraction = std::to_string(GetHeavyPartitionFraction(context));
	auto light_partition_multiplier = std::to_string(GetLightPartitionMultiplier(context));
	auto file_name = benchmark_output_prefix + "_pushpull_explore_timing_" + benchmark_run_id + "_threads_" +
	                 std::to_string(num_threads) + "_" + heavy_partition_fraction + "_" + light_partition_multiplier +
	                 ".csv";
	WriteTimingResults(file_name);
	auto iteration_stats_file_name = benchmark_output_prefix + "_pushpull_iteration_stats.csv";
	WriteIterationStats(iteration_stats_file_name);
	auto phase_detail_file_name = benchmark_output_prefix + "_pushpull_phase_detail.csv";
	WritePhaseTimingResults(phase_detail_file_name);
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
			     << entry.phase << "," << entry.worker_id << "," << entry.active_lanes << "," << entry.frontier_vertices
			     << "," << entry.vertex_count << "," << entry.partition_count << "," << entry.vertices << ","
			     << entry.edges << "," << entry.candidates << "," << entry.changed_vertices << "," << entry.time_ms
			     << "\n";
		}
	}
}

} // namespace duckdb
