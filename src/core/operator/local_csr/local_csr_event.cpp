#include "duckpgq/core/operator/local_csr/local_csr_event.hpp"

#include <duckpgq/core/option/duckpgq_option.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>

namespace duckdb {

static mutex local_csr_phase_timing_lock;

static double ElapsedMs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end) {
	return std::chrono::duration<double, std::milli>(end - start).count();
}

static size_t GetLocalCSREdgeCount(const std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	size_t edge_count = 0;
	for (const auto &local_csr : partition_csrs) {
		edge_count += local_csr->GetEdgeSize();
	}
	return edge_count;
}

static size_t GetLocalCSRMemory(const std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	size_t memory = 0;
	for (const auto &local_csr : partition_csrs) {
		if (local_csr->v) {
			memory += local_csr->v_array_size * sizeof(std::atomic<uint32_t>);
		}
		memory += local_csr->source_vertices.capacity() * sizeof(uint32_t);
		memory += local_csr->row_offsets.capacity() * sizeof(uint32_t);
		memory += local_csr->e.capacity() * sizeof(uint16_t);
		for (const auto &segment : local_csr->segments) {
			memory += segment.source_vertices.capacity() * sizeof(uint32_t);
			memory += segment.row_offsets.capacity() * sizeof(uint32_t);
			memory += segment.edges.capacity() * sizeof(uint16_t);
		}
	}
	return memory;
}

static size_t GetPullCSREdgeCount(const std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) {
	size_t edge_count = 0;
	for (const auto &pull_csr : pull_partition_csrs) {
		edge_count += pull_csr->GetEdgeSize();
	}
	return edge_count;
}

static size_t GetPullCSRMemory(const std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) {
	size_t memory = 0;
	for (const auto &pull_csr : pull_partition_csrs) {
		memory += pull_csr->offsets_size * sizeof(std::atomic<uint32_t>);
		memory += pull_csr->predecessors.capacity() * sizeof(uint32_t);
	}
	return memory;
}

static void AppendPhaseTiming(const LocalCSRState &state, const string &phase,
                              const std::vector<shared_ptr<LocalCSR>> &partition_csrs, double time_ms) {
	if (!state.benchmark_enabled) {
		return;
	}

	auto file_name = state.benchmark_output_prefix + "_phase_timing.csv";
	lock_guard<mutex> lock(local_csr_phase_timing_lock);
	bool write_header = !std::filesystem::exists(file_name);
	std::ofstream outfile(file_name, std::ios::app);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding phase benchmark file \"%s\"", file_name);
	}
	if (write_header) {
		outfile << "Phase,RunID,ThreadCount,PairCount,VertexCount,EdgeCount,PartitionCount,Time_ms,MemoryBytes\n";
	}
	auto vertex_count = state.vsize - 2;
	outfile << phase << "," << state.benchmark_run_id << "," << state.num_threads << ",0," << vertex_count << ","
	        << GetLocalCSREdgeCount(partition_csrs) << "," << partition_csrs.size() << "," << time_ms << ","
	        << GetLocalCSRMemory(partition_csrs) << "\n";
}

static void AppendPullPhaseTiming(const LocalCSRState &state, double time_ms) {
	if (!state.benchmark_enabled) {
		return;
	}

	auto file_name = state.benchmark_output_prefix + "_phase_timing.csv";
	lock_guard<mutex> lock(local_csr_phase_timing_lock);
	bool write_header = !std::filesystem::exists(file_name);
	std::ofstream outfile(file_name, std::ios::app);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding phase benchmark file \"%s\"", file_name);
	}
	if (write_header) {
		outfile << "Phase,RunID,ThreadCount,PairCount,VertexCount,EdgeCount,PartitionCount,Time_ms,MemoryBytes\n";
	}
	auto vertex_count = state.vsize - 2;
	outfile << "local_csr_pull," << state.benchmark_run_id << "," << state.num_threads << ",0," << vertex_count << ","
	        << GetPullCSREdgeCount(state.pull_partition_csrs) << "," << state.pull_partition_csrs.size() << ","
	        << time_ms << "," << GetPullCSRMemory(state.pull_partition_csrs) << "\n";
}

static void AppendSubphaseTimings(const LocalCSRState &state) {
	for (const auto &timing : state.subphase_timings) {
		auto &partition_csrs = timing.reverse ? state.reverse_partition_csrs : state.partition_csrs;
		auto phase = string("local_csr_") + (timing.reverse ? "reverse_" : "forward_") + timing.phase;
		AppendPhaseTiming(state, phase, partition_csrs, timing.time_ms);
	}
}

static void WritePartitionStats(const LocalCSRState &state, ClientContext &context,
                                const std::vector<shared_ptr<LocalCSR>> &partition_csrs, const string &direction) {
	if (partition_csrs.empty()) {
		return;
	}

	auto heavy_partition_fraction = std::to_string(GetHeavyPartitionFraction(context));
	auto light_partition_multiplier = std::to_string(GetLightPartitionMultiplier(context));

	auto suffix = direction == "forward" ? "" : "_" + direction;
	auto file_name = state.benchmark_output_prefix + suffix + "_partition_stats_" + state.benchmark_run_id + "_" +
	                 std::to_string(state.vsize - 2) + "_vertices_mphl_" + heavy_partition_fraction + "_" +
	                 light_partition_multiplier + ".csv";
	std::ofstream outfile(file_name);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding partition benchmark file \"%s\"", file_name);
	}
	outfile << "PartitionID,StartVertex,EndVertex,VertexCount,EdgeCount,EdgePerVertex,VertexMemBytes,EdgeMemBytes,"
	           "TotalMemBytes\n";

	idx_t partition_id = 0;
	for (const auto &local_csr : partition_csrs) {
		auto vertex_count = local_csr->GetVertexSize();
		auto edge_count = local_csr->GetEdgeSize();
		double edge_per_vertex = vertex_count > 0 ? static_cast<double>(edge_count) / vertex_count : 0.0;

		size_t vertex_mem = 0;
		if (local_csr->v) {
			vertex_mem += local_csr->v_array_size * sizeof(std::atomic<uint32_t>);
		}
		vertex_mem += local_csr->source_vertices.capacity() * sizeof(uint32_t);
		vertex_mem += local_csr->row_offsets.capacity() * sizeof(uint32_t);
		size_t edge_mem = local_csr->e.capacity() * sizeof(uint16_t);
		for (const auto &segment : local_csr->segments) {
			vertex_mem += segment.source_vertices.capacity() * sizeof(uint32_t);
			vertex_mem += segment.row_offsets.capacity() * sizeof(uint32_t);
			edge_mem += segment.edges.capacity() * sizeof(uint16_t);
		}
		size_t total_mem = vertex_mem + edge_mem;

		outfile << partition_id << "," << local_csr->start_vertex << "," << local_csr->end_vertex << "," << vertex_count
		        << "," << edge_count << "," << edge_per_vertex << "," << vertex_mem << "," << edge_mem << ","
		        << total_mem << "\n";

		partition_id++;
	}
}

static void WritePullPartitionStats(const LocalCSRState &state, ClientContext &context) {
	if (state.pull_partition_csrs.empty()) {
		return;
	}

	auto heavy_partition_fraction = std::to_string(GetHeavyPartitionFraction(context));
	auto light_partition_multiplier = std::to_string(GetLightPartitionMultiplier(context));

	auto file_name = state.benchmark_output_prefix + "_pull_partition_stats_" + state.benchmark_run_id + "_" +
	                 std::to_string(state.vsize - 2) + "_vertices_mphl_" + heavy_partition_fraction + "_" +
	                 light_partition_multiplier + ".csv";
	std::ofstream outfile(file_name);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding partition benchmark file \"%s\"", file_name);
	}
	outfile << "PartitionID,StartVertex,EndVertex,VertexCount,EdgeCount,EdgePerVertex,VertexMemBytes,EdgeMemBytes,"
	           "TotalMemBytes\n";

	idx_t partition_id = 0;
	for (const auto &pull_csr : state.pull_partition_csrs) {
		auto vertex_count = pull_csr->GetVertexSize();
		auto edge_count = pull_csr->GetEdgeSize();
		double edge_per_vertex = vertex_count > 0 ? static_cast<double>(edge_count) / vertex_count : 0.0;

		size_t vertex_mem = pull_csr->offsets_size * sizeof(std::atomic<uint32_t>);
		size_t edge_mem = pull_csr->predecessors.capacity() * sizeof(uint32_t);
		size_t total_mem = vertex_mem + edge_mem;

		outfile << partition_id << "," << pull_csr->start_vertex << "," << pull_csr->end_vertex << "," << vertex_count
		        << "," << edge_count << "," << edge_per_vertex << "," << vertex_mem << "," << edge_mem << ","
		        << total_mem << "\n";

		partition_id++;
	}
}

//===--------------------------------------------------------------------===//
// LocalCSRBuildKernel
//===--------------------------------------------------------------------===//
static void RecordSubphase(LocalCSRState &state, bool reverse, const string &phase,
                           std::chrono::steady_clock::time_point start) {
	if (!state.benchmark_enabled) {
		return;
	}
	state.subphase_timings.push_back({reverse, phase, ElapsedMs(start, std::chrono::steady_clock::now())});
}

LocalCSRBuildKernel::LocalCSRBuildKernel(shared_ptr<LocalCSRState> state_p) : state(std::move(state_p)) {
}

void LocalCSRBuildKernel::AddStep(std::function<void()> before, std::function<void(LocalCSRWorker &)> parallel,
                                  std::function<void()> after) {
	steps.push_back({std::move(before), std::move(parallel), std::move(after)});
}

//! Statistics, partitions, edge counts, running sums, distribution (and sparse rows), as the barrier version did
void LocalCSRBuildKernel::PlanDenseBuild(bool reverse) {
	auto st = state;
	auto &statistics = reverse ? st->reverse_statistics_chunks : st->statistics_chunks;
	auto &partitions = reverse ? st->reverse_partition_csrs : st->partition_csrs;
	auto &start_time = reverse ? st->reverse_start_time : st->forward_start_time;
	auto &end_time = reverse ? st->reverse_end_time : st->forward_end_time;
	auto record = [this, st, reverse](const string &phase) {
		RecordSubphase(*st, reverse, phase, subphase_start);
	};
	AddStep(
	    [this, st, &statistics, &partitions, &start_time]() {
		    std::fill(statistics.begin(), statistics.end(), 0);
		    partitions.clear();
		    st->partition_index = 0;
		    start_time = std::chrono::steady_clock::now();
		    subphase_start = start_time;
	    },
	    [reverse, &statistics](LocalCSRWorker &worker) { worker.CreateStatistics(reverse, statistics); },
	    [record]() { record("statistics"); });
	AddStep(
	    [this, &statistics, &partitions, record]() {
		    subphase_start = std::chrono::steady_clock::now();
		    workers[0]->DeterminePartitions(statistics, partitions);
		    record("determine_partitions");
		    subphase_start = std::chrono::steady_clock::now();
	    },
	    [reverse, &partitions](LocalCSRWorker &worker) { worker.CountOutgoingEdgesPerPartition(reverse, partitions); },
	    [this, st, record]() {
		    record("count_edges");
		    st->partition_index = 0;
		    subphase_start = std::chrono::steady_clock::now();
	    });
	AddStep(
	    nullptr, [&partitions](LocalCSRWorker &worker) { worker.CreateRunningSum(partitions); },
	    [this, record]() {
		    record("running_sum");
		    subphase_start = std::chrono::steady_clock::now();
	    });
	AddStep([this, &partitions]() { workers[0]->PrepareDistributeEdges(partitions); },
	        [reverse, &partitions](LocalCSRWorker &worker) { worker.DistributeEdges(reverse, partitions); },
	        [record]() { record("distribute_edges"); });
	if (st->finalize_sparse_rows) {
		AddStep(
		    [this, st]() {
			    st->partition_index = 0;
			    subphase_start = std::chrono::steady_clock::now();
		    },
		    [&partitions](LocalCSRWorker &worker) { worker.FinalizeSparseRows(partitions); },
		    [record]() { record("finalize_sparse_rows"); });
	}
	auto previous_after = steps.back().after;
	steps.back().after = [previous_after, &end_time]() {
		if (previous_after) {
			previous_after();
		}
		end_time = std::chrono::steady_clock::now();
	};
}

//! Statistics, partitions, per-worker edge buffers, merge
void LocalCSRBuildKernel::PlanSparseForwardBuild() {
	auto st = state;
	auto record = [this, st](const string &phase) {
		RecordSubphase(*st, false, phase, subphase_start);
	};
	AddStep(
	    [this, st]() {
		    std::fill(st->statistics_chunks.begin(), st->statistics_chunks.end(), 0);
		    st->partition_csrs.clear();
		    st->forward_build_buffers.clear();
		    st->partition_index = 0;
		    st->forward_start_time = std::chrono::steady_clock::now();
		    subphase_start = st->forward_start_time;
	    },
	    [st](LocalCSRWorker &worker) { worker.CreateStatistics(false, st->statistics_chunks); },
	    [record]() { record("statistics"); });
	AddStep(
	    [this, st, record]() {
		    subphase_start = std::chrono::steady_clock::now();
		    workers[0]->DeterminePartitions(st->statistics_chunks, st->partition_csrs, false);
		    st->forward_build_buffers.resize(st->tasks_scheduled);
		    for (auto &worker_buffers : st->forward_build_buffers) {
			    worker_buffers.resize(st->partition_csrs.size());
		    }
		    record("determine_partitions");
		    subphase_start = std::chrono::steady_clock::now();
	    },
	    [st](LocalCSRWorker &worker) { worker.BufferForwardEdges(st->partition_csrs); },
	    [this, st, record]() {
		    st->partition_index = 0;
		    record("buffer_edges");
		    subphase_start = std::chrono::steady_clock::now();
	    });
	AddStep(
	    nullptr, [st](LocalCSRWorker &worker) { worker.MergeForwardBuffers(st->partition_csrs); },
	    [st, record]() {
		    st->forward_build_buffers.clear();
		    st->forward_end_time = std::chrono::steady_clock::now();
		    record("merge_buffers");
	    });
}

void LocalCSRBuildKernel::PlanPullBuild() {
	auto st = state;
	AddStep(
	    [this, st]() {
		    st->pull_partition_csrs.clear();
		    st->partition_index = 0;
		    st->pull_start_time = std::chrono::steady_clock::now();
		    workers[0]->DeterminePullPartitions(st->pull_partition_csrs);
	    },
	    [st](LocalCSRWorker &worker) { worker.CountIncomingEdgesPerPullPartition(st->pull_partition_csrs); },
	    [st]() { st->partition_index = 0; });
	AddStep(
	    nullptr, [st](LocalCSRWorker &worker) { worker.CreatePullRunningSum(st->pull_partition_csrs); }, nullptr);
	AddStep([this, st]() { workers[0]->PrepareDistributePullEdges(st->pull_partition_csrs); },
	        [st](LocalCSRWorker &worker) { worker.DistributePullEdges(st->pull_partition_csrs); },
	        [st]() { st->pull_end_time = std::chrono::steady_clock::now(); });
}

bool LocalCSRBuildKernel::Start() {
	auto worker_count = std::max<idx_t>(1, state->num_threads);
	state->num_threads = worker_count;
	state->tasks_scheduled = worker_count;
	for (idx_t worker = 0; worker < worker_count; worker++) {
		workers.push_back(make_uniq<LocalCSRWorker>(state, worker));
	}
	if (state->build_forward_csr) {
		if (state->streaming_endpoint_input) {
			// Serial: the streaming segments are promoted to partitions as they are
			state->partition_csrs.clear();
			state->forward_start_time = std::chrono::steady_clock::now();
			workers[0]->PromoteStreamingEndpointBuffers(state->partition_csrs);
			state->forward_build_buffers.clear();
			state->forward_end_time = std::chrono::steady_clock::now();
			RecordSubphase(*state, false, "streaming_promote", state->forward_start_time);
		} else if (state->finalize_sparse_rows) {
			PlanSparseForwardBuild();
		} else {
			PlanDenseBuild(false);
		}
	}
	if (state->build_reverse_csr) {
		if (state->streaming_endpoint_input) {
			throw InternalException("Streaming endpoint CSR cannot build reverse input");
		}
		PlanDenseBuild(true);
	}
	if (state->build_pull_csr) {
		PlanPullBuild();
	}
	if (steps.empty()) {
		return false;
	}
	if (steps[0].before) {
		steps[0].before();
	}
	return true;
}

idx_t LocalCSRBuildKernel::WorkerCount() const {
	return workers.size();
}

void LocalCSRBuildKernel::RunPhase(idx_t worker_id) {
	steps[step].parallel(*workers[worker_id]);
}

bool LocalCSRBuildKernel::AdvancePhase() {
	if (steps[step].after) {
		steps[step].after();
	}
	step++;
	if (step >= steps.size()) {
		return false;
	}
	if (steps[step].before) {
		steps[step].before();
	}
	return true;
}

void LocalCSRBuildKernel::Finished() {
	// Assume at least one partition exists
	D_ASSERT(!state->partition_csrs.empty() || !state->reverse_partition_csrs.empty());

	if (state->build_forward_csr) {
		std::sort(state->partition_csrs.begin(), state->partition_csrs.end(),
		          [](const shared_ptr<LocalCSR> &a, const shared_ptr<LocalCSR> &b) {
			          return a->GetEdgeSize() > b->GetEdgeSize(); // Sort by edge count
		          });
	}
	if (state->build_reverse_csr) {
		std::sort(state->reverse_partition_csrs.begin(), state->reverse_partition_csrs.end(),
		          [](const shared_ptr<LocalCSR> &a, const shared_ptr<LocalCSR> &b) {
			          return a->GetEdgeSize() > b->GetEdgeSize(); // Sort by edge count
		          });
	}
	std::sort(state->pull_partition_csrs.begin(), state->pull_partition_csrs.end(),
	          [](const shared_ptr<PullCSR> &a, const shared_ptr<PullCSR> &b) {
		          return a->GetEdgeSize() > b->GetEdgeSize(); // Sort by edge count
	          });

	if (!state->benchmark_enabled) {
		return;
	}

	if (state->build_forward_csr) {
		AppendPhaseTiming(*state, "local_csr_forward", state->partition_csrs,
		                  ElapsedMs(state->forward_start_time, state->forward_end_time));
		WritePartitionStats(*state, state->context, state->partition_csrs, "forward");
	}
	if (state->build_reverse_csr) {
		AppendPhaseTiming(*state, "local_csr_reverse", state->reverse_partition_csrs,
		                  ElapsedMs(state->reverse_start_time, state->reverse_end_time));
		WritePartitionStats(*state, state->context, state->reverse_partition_csrs, "reverse");
	}
	if (state->build_pull_csr) {
		AppendPullPhaseTiming(*state, ElapsedMs(state->pull_start_time, state->pull_end_time));
		WritePullPartitionStats(*state, state->context);
	}
	AppendSubphaseTimings(*state);
}

} // namespace duckdb
