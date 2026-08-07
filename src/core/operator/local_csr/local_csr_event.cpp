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
	auto vertex_count = state.global_csr->vsize - 2;
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
	auto vertex_count = state.global_csr->vsize - 2;
	outfile << "local_csr_pull," << state.benchmark_run_id << "," << state.num_threads << ",0," << vertex_count << ","
	        << GetPullCSREdgeCount(state.pull_partition_csrs) << "," << state.pull_partition_csrs.size() << ","
	        << time_ms << "," << GetPullCSRMemory(state.pull_partition_csrs) << "\n";
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
	                 std::to_string(state.global_csr->vsize - 2) + "_vertices_mphl_" + heavy_partition_fraction + "_" +
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
	                 std::to_string(state.global_csr->vsize - 2) + "_vertices_mphl_" + heavy_partition_fraction + "_" +
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

LocalCSREvent::LocalCSREvent(shared_ptr<LocalCSRState> local_csr_state_p, Pipeline &pipeline_p,
                             const PhysicalPathFinding &op_p, ClientContext &context_p)
    : BasePipelineEvent(pipeline_p), local_csr_state(std::move(local_csr_state_p)), op(op_p), context(context_p) {
}

void LocalCSREvent::Schedule() {
	auto &context = pipeline->GetClientContext();
	vector<shared_ptr<Task>> csr_tasks;
	for (idx_t tnum = 0; tnum < local_csr_state->num_threads; tnum++) {
		csr_tasks.push_back(make_uniq<LocalCSRTask>(shared_from_this(), context, local_csr_state, tnum, op));
		local_csr_state->tasks_scheduled++;
	}
	local_csr_state->barrier = make_uniq<Barrier>(local_csr_state->tasks_scheduled);
	SetTasks(std::move(csr_tasks));
}

void LocalCSREvent::FinishEvent() {
	// Assume at least one partition exists
	D_ASSERT(!local_csr_state->partition_csrs.empty() || !local_csr_state->reverse_partition_csrs.empty());

	if (local_csr_state->build_forward_csr) {
		std::sort(local_csr_state->partition_csrs.begin(), local_csr_state->partition_csrs.end(),
		          [](const shared_ptr<LocalCSR> &a, const shared_ptr<LocalCSR> &b) {
			          return a->GetEdgeSize() > b->GetEdgeSize(); // Sort by edge count
		          });
	}
	if (local_csr_state->build_reverse_csr) {
		std::sort(local_csr_state->reverse_partition_csrs.begin(), local_csr_state->reverse_partition_csrs.end(),
		          [](const shared_ptr<LocalCSR> &a, const shared_ptr<LocalCSR> &b) {
			          return a->GetEdgeSize() > b->GetEdgeSize(); // Sort by edge count
		          });
	}
	std::sort(local_csr_state->pull_partition_csrs.begin(), local_csr_state->pull_partition_csrs.end(),
	          [](const shared_ptr<PullCSR> &a, const shared_ptr<PullCSR> &b) {
		          return a->GetEdgeSize() > b->GetEdgeSize(); // Sort by edge count
	          });

	if (!local_csr_state->benchmark_enabled) {
		return;
	}

	if (local_csr_state->build_forward_csr) {
		AppendPhaseTiming(*local_csr_state, "local_csr_forward", local_csr_state->partition_csrs,
		                  ElapsedMs(local_csr_state->forward_start_time, local_csr_state->forward_end_time));
		WritePartitionStats(*local_csr_state, context, local_csr_state->partition_csrs, "forward");
	}
	if (local_csr_state->build_reverse_csr) {
		AppendPhaseTiming(*local_csr_state, "local_csr_reverse", local_csr_state->reverse_partition_csrs,
		                  ElapsedMs(local_csr_state->reverse_start_time, local_csr_state->reverse_end_time));
		WritePartitionStats(*local_csr_state, context, local_csr_state->reverse_partition_csrs, "reverse");
	}
	if (local_csr_state->build_pull_csr) {
		AppendPullPhaseTiming(*local_csr_state,
		                      ElapsedMs(local_csr_state->pull_start_time, local_csr_state->pull_end_time));
		WritePullPartitionStats(*local_csr_state, context);
	}
}

} // namespace duckdb
