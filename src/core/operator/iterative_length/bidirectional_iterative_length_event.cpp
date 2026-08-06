#include "duckpgq/core/operator/iterative_length/bidirectional_iterative_length_event.hpp"

#include <duckpgq/core/option/duckpgq_option.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>

namespace duckdb {

static mutex bidirectional_bfs_phase_timing_lock;

static size_t GetLocalCSREdgeCount(const std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	size_t edge_count = 0;
	for (const auto &local_csr : partition_csrs) {
		edge_count += local_csr->GetEdgeSize();
	}
	return edge_count;
}

static void AppendBFSPhaseTiming(const BidirectionalIterativeLengthState &state, double time_ms) {
	auto file_name = state.benchmark_output_prefix + "_phase_timing.csv";
	lock_guard<mutex> lock(bidirectional_bfs_phase_timing_lock);
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

BidirectionalIterativeLengthEvent::BidirectionalIterativeLengthEvent(
    shared_ptr<BidirectionalIterativeLengthState> gbfs_state_p, Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
    : BasePipelineEvent(pipeline_p), gbfs_state(std::move(gbfs_state_p)), op(op_p) {
}

void BidirectionalIterativeLengthEvent::Schedule() {
	gbfs_state->phase_start_time = std::chrono::steady_clock::now();
	auto &context = pipeline->GetClientContext();
	vector<shared_ptr<Task>> bfs_tasks;
	idx_t num_partitions = std::max(gbfs_state->local_csrs.size(), gbfs_state->reverse_local_csrs.size());
	for (idx_t tnum = 0; tnum < std::min(gbfs_state->num_threads, num_partitions); tnum++) {
		bfs_tasks.push_back(
		    make_uniq<BidirectionalIterativeLengthTask>(shared_from_this(), context, gbfs_state, tnum, op));
		gbfs_state->tasks_scheduled++;
	}
	gbfs_state->barrier = make_uniq<Barrier>(gbfs_state->tasks_scheduled);
	SetTasks(std::move(bfs_tasks));
}

void BidirectionalIterativeLengthEvent::FinishEvent() {
	if (!gbfs_state->benchmark_enabled) {
		return;
	}

	auto phase_end_time = std::chrono::steady_clock::now();
	auto time_ms = std::chrono::duration<double, std::milli>(phase_end_time - gbfs_state->phase_start_time).count();
	AppendBFSPhaseTiming(*gbfs_state, time_ms);

	auto heavy_partition_fraction = std::to_string(GetHeavyPartitionFraction(gbfs_state->context));
	auto light_partition_multiplier = std::to_string(GetLightPartitionMultiplier(gbfs_state->context));
	auto file_name = gbfs_state->benchmark_output_prefix + "_bidirectional_explore_timing_" +
	                 gbfs_state->benchmark_run_id + "_threads_" + std::to_string(gbfs_state->num_threads) + "_" +
	                 heavy_partition_fraction + "_" + light_partition_multiplier + ".csv";
	gbfs_state->WriteTimingResults(file_name);

	auto phase_detail_file_name = gbfs_state->benchmark_output_prefix + "_bidirectional_phase_detail.csv";
	gbfs_state->WritePhaseTimingResults(phase_detail_file_name);
}

} // namespace duckdb
