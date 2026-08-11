#include "duckpgq/core/operator/local_csr/local_csr_task.hpp"
#include <duckdb/parallel/event.hpp>
#include <duckpgq/core/operator/local_csr/local_csr_state.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

namespace duckdb {

namespace {

static void RecordLocalCSRSubphase(LocalCSRState &state, bool reverse, const string &phase,
                                   std::chrono::steady_clock::time_point start,
                                   std::chrono::steady_clock::time_point end) {
	if (!state.benchmark_enabled) {
		return;
	}
	auto time_ms = std::chrono::duration<double, std::milli>(end - start).count();
	state.subphase_timings.push_back({reverse, phase, time_ms});
}

} // namespace

LocalCSRTask::LocalCSRTask(shared_ptr<Event> event_p, ClientContext &context, shared_ptr<LocalCSRState> &state,
                           idx_t worker_id_p, const PhysicalOperator &op_p)
    : ExecutorTask(context, std::move(event_p), op_p), local_csr_state(state), worker_id(worker_id_p) {
}

TaskExecutionResult LocalCSRTask::ExecuteTask(TaskExecutionMode mode) {
	if (local_csr_state->build_forward_csr) {
		BuildLocalCSRs(false);
	}
	if (local_csr_state->build_reverse_csr) {
		BuildLocalCSRs(true);
	}
	if (local_csr_state->build_pull_csr) {
		BuildPullCSRs();
	}
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

void LocalCSRTask::BuildLocalCSRs(bool reverse) {
	if (local_csr_state->streaming_endpoint_input) {
		if (reverse) {
			throw InternalException("Streaming endpoint CSR cannot build reverse input");
		}
		BuildStreamingEndpointCSRs();
		return;
	}
	if (!reverse && local_csr_state->finalize_sparse_rows) {
		BuildSparseForwardCSRs();
		return;
	}

	auto &barrier = local_csr_state->barrier;
	auto &statistics_chunks = reverse ? local_csr_state->reverse_statistics_chunks : local_csr_state->statistics_chunks;
	auto &partition_csrs = reverse ? local_csr_state->reverse_partition_csrs : local_csr_state->partition_csrs;

	if (worker_id == 0) {
		std::fill(statistics_chunks.begin(), statistics_chunks.end(), 0);
		partition_csrs.clear();
		local_csr_state->partition_index = 0;
		if (reverse) {
			local_csr_state->reverse_start_time = std::chrono::steady_clock::now();
		} else {
			local_csr_state->forward_start_time = std::chrono::steady_clock::now();
		}
	}
	barrier->Wait(worker_id);
	auto subphase_start = std::chrono::steady_clock::now();
	CreateStatistics(reverse, statistics_chunks); // Phase 1
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		RecordLocalCSRSubphase(*local_csr_state, reverse, "statistics", subphase_start,
		                       std::chrono::steady_clock::now());
	}
	subphase_start = std::chrono::steady_clock::now();
	if (worker_id == 0) {
		DeterminePartitions(statistics_chunks, partition_csrs); // Phase 2
	}
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		RecordLocalCSRSubphase(*local_csr_state, reverse, "determine_partitions", subphase_start,
		                       std::chrono::steady_clock::now());
	}
	subphase_start = std::chrono::steady_clock::now();
	CountOutgoingEdgesPerPartition(reverse, partition_csrs); // Phase 3
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		RecordLocalCSRSubphase(*local_csr_state, reverse, "count_edges", subphase_start,
		                       std::chrono::steady_clock::now());
	}
	if (worker_id == 0) {
		local_csr_state->partition_index = 0;
	}
	barrier->Wait(worker_id);
	subphase_start = std::chrono::steady_clock::now();
	CreateRunningSum(partition_csrs); // Phase 4
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		RecordLocalCSRSubphase(*local_csr_state, reverse, "running_sum", subphase_start,
		                       std::chrono::steady_clock::now());
	}
	subphase_start = std::chrono::steady_clock::now();
	DistributeEdges(reverse, partition_csrs); // Phase 5
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		RecordLocalCSRSubphase(*local_csr_state, reverse, "distribute_edges", subphase_start,
		                       std::chrono::steady_clock::now());
	}
	if (local_csr_state->finalize_sparse_rows) {
		if (worker_id == 0) {
			local_csr_state->partition_index = 0;
		}
		barrier->Wait(worker_id);
		subphase_start = std::chrono::steady_clock::now();
		FinalizeSparseRows(partition_csrs);
		barrier->Wait(worker_id);
		if (worker_id == 0) {
			RecordLocalCSRSubphase(*local_csr_state, reverse, "finalize_sparse_rows", subphase_start,
			                       std::chrono::steady_clock::now());
		}
	}
	if (worker_id == 0) {
		if (reverse) {
			local_csr_state->reverse_end_time = std::chrono::steady_clock::now();
		} else {
			local_csr_state->forward_end_time = std::chrono::steady_clock::now();
		}
	}
	barrier->Wait(worker_id);
}

void LocalCSRTask::BuildStreamingEndpointCSRs() {
	auto &barrier = local_csr_state->barrier;
	auto &partition_csrs = local_csr_state->partition_csrs;
	if (worker_id == 0) {
		partition_csrs.clear();
		local_csr_state->forward_start_time = std::chrono::steady_clock::now();
		PromoteStreamingEndpointBuffers(partition_csrs);
	}
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		local_csr_state->forward_build_buffers.clear();
		local_csr_state->forward_end_time = std::chrono::steady_clock::now();
		RecordLocalCSRSubphase(*local_csr_state, false, "streaming_promote",
		                       local_csr_state->forward_start_time,
		                       local_csr_state->forward_end_time);
	}
	barrier->Wait(worker_id);
}

void LocalCSRTask::PromoteStreamingEndpointBuffers(std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	idx_t partition_count = 0;
	for (const auto &run : local_csr_state->forward_build_buffers) {
		partition_count = std::max<idx_t>(partition_count, run.size());
	}
	partition_csrs.reserve(partition_count);
	for (idx_t partition_idx = 0; partition_idx < partition_count; partition_idx++) {
		auto start_vertex = partition_idx * local_csr_state->streaming_partition_width;
		auto end_vertex = std::min(start_vertex + local_csr_state->streaming_partition_width,
		                           local_csr_state->vsize);
		auto target = make_shared_ptr<LocalCSR>(start_vertex, end_vertex, local_csr_state->vsize, false);
		target->initialized_e = true;
		target->sparse_rows_initialized = true;
		partition_csrs.push_back(std::move(target));
	}

	for (auto &run : local_csr_state->forward_build_buffers) {
		for (idx_t partition_idx = 0; partition_idx < run.size(); partition_idx++) {
			auto &buffer = run[partition_idx];
			if (buffer.destinations.empty()) {
				continue;
			}
			LocalCSRSegment segment;
			segment.source_vertices = std::move(buffer.source_vertices);
			segment.row_offsets = std::move(buffer.row_offsets);
			segment.edges = std::move(buffer.destinations);
			partition_csrs[partition_idx]->segments.push_back(std::move(segment));
		}
	}
}

void LocalCSRTask::BuildSparseForwardCSRs() {
	auto &barrier = local_csr_state->barrier;
	auto &statistics_chunks = local_csr_state->statistics_chunks;
	auto &partition_csrs = local_csr_state->partition_csrs;

	if (worker_id == 0) {
		std::fill(statistics_chunks.begin(), statistics_chunks.end(), 0);
		partition_csrs.clear();
		local_csr_state->forward_build_buffers.clear();
		local_csr_state->partition_index = 0;
		local_csr_state->forward_start_time = std::chrono::steady_clock::now();
	}
	barrier->Wait(worker_id);

	auto subphase_start = std::chrono::steady_clock::now();
	CreateStatistics(false, statistics_chunks);
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		RecordLocalCSRSubphase(*local_csr_state, false, "statistics", subphase_start, std::chrono::steady_clock::now());
	}

	subphase_start = std::chrono::steady_clock::now();
	if (worker_id == 0) {
		DeterminePartitions(statistics_chunks, partition_csrs, false);
		local_csr_state->forward_build_buffers.resize(local_csr_state->tasks_scheduled);
		for (auto &worker_buffers : local_csr_state->forward_build_buffers) {
			worker_buffers.resize(partition_csrs.size());
		}
	}
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		RecordLocalCSRSubphase(*local_csr_state, false, "determine_partitions", subphase_start,
		                       std::chrono::steady_clock::now());
	}

	subphase_start = std::chrono::steady_clock::now();
	BufferForwardEdges(partition_csrs);
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		local_csr_state->partition_index = 0;
		RecordLocalCSRSubphase(*local_csr_state, false, "buffer_edges", subphase_start,
		                       std::chrono::steady_clock::now());
	}
	barrier->Wait(worker_id);

	subphase_start = std::chrono::steady_clock::now();
	MergeForwardBuffers(partition_csrs);
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		local_csr_state->forward_build_buffers.clear();
		local_csr_state->forward_end_time = std::chrono::steady_clock::now();
		RecordLocalCSRSubphase(*local_csr_state, false, "merge_buffers", subphase_start,
		                       local_csr_state->forward_end_time);
	}
	barrier->Wait(worker_id);
}

void LocalCSRTask::BufferForwardEdges(std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	auto &global_csr = *local_csr_state->global_csr;
	auto &worker_buffers = local_csr_state->forward_build_buffers[worker_id];
	const idx_t vertex_count = global_csr.vsize - 2;
	const idx_t vertices_per_worker =
	    (vertex_count + local_csr_state->tasks_scheduled - 1) / local_csr_state->tasks_scheduled;
	const idx_t src_start = worker_id * vertices_per_worker;
	const idx_t src_end = std::min(src_start + vertices_per_worker, vertex_count);

	for (idx_t src = src_start; src < src_end; src++) {
		const idx_t edge_start = global_csr.v[src];
		const idx_t edge_end = global_csr.v[src + 1];
		for (idx_t edge_idx = edge_start; edge_idx < edge_end; edge_idx++) {
			const idx_t dst = global_csr.e[edge_idx];
			const idx_t partition_idx = GetPartitionForVertex(dst, partition_csrs);
			auto &partition = *partition_csrs[partition_idx];
			worker_buffers[partition_idx].Append(src, dst - partition.start_vertex);
		}
	}
}

void LocalCSRTask::MergeForwardBuffers(std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	while (true) {
		const idx_t partition_idx = local_csr_state->partition_index.fetch_add(1);
		if (partition_idx >= partition_csrs.size()) {
			break;
		}

		auto &target = *partition_csrs[partition_idx];
		idx_t source_count = 0;
		idx_t edge_count = 0;
		for (auto &worker_buffers : local_csr_state->forward_build_buffers) {
			auto &source = worker_buffers[partition_idx];
			source_count += source.source_vertices.size();
			edge_count += source.destinations.size();
		}

		target.source_vertices.reserve(source_count);
		target.row_offsets.reserve(source_count + 1);
		target.e.reserve(edge_count);
		for (auto &worker_buffers : local_csr_state->forward_build_buffers) {
			auto &source = worker_buffers[partition_idx];
			const auto edge_base = target.e.size();
			target.source_vertices.insert(target.source_vertices.end(), source.source_vertices.begin(),
			                              source.source_vertices.end());
			for (auto offset : source.row_offsets) {
				target.row_offsets.push_back(NumericCast<uint32_t>(edge_base + offset));
			}
			target.e.insert(target.e.end(), source.destinations.begin(), source.destinations.end());
		}
		target.row_offsets.push_back(NumericCast<uint32_t>(target.e.size()));
		target.initialized_e = true;
		target.sparse_rows_initialized = true;
	}
}

void LocalCSRTask::BuildPullCSRs() {
	auto &barrier = local_csr_state->barrier;
	auto &pull_partition_csrs = local_csr_state->pull_partition_csrs;

	if (worker_id == 0) {
		pull_partition_csrs.clear();
		local_csr_state->partition_index = 0;
		local_csr_state->pull_start_time = std::chrono::steady_clock::now();
		DeterminePullPartitions(pull_partition_csrs);
	}
	barrier->Wait(worker_id);
	CountIncomingEdgesPerPullPartition(pull_partition_csrs);
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		local_csr_state->partition_index = 0;
	}
	barrier->Wait(worker_id);
	CreatePullRunningSum(pull_partition_csrs);
	barrier->Wait(worker_id);
	DistributePullEdges(pull_partition_csrs);
	barrier->Wait(worker_id);
	if (worker_id == 0) {
		local_csr_state->pull_end_time = std::chrono::steady_clock::now();
	}
	barrier->Wait(worker_id);
}

void LocalCSRTask::DistributeEdges(bool reverse, std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	auto &v = local_csr_state->global_csr->v;
	auto &e = local_csr_state->global_csr->e;
	idx_t total_vertices = local_csr_state->global_csr->vsize - 1;

	// One-time setup: resize edge buffers and initialize write offsets
	if (worker_id == 0) {
		for (auto &csr_ptr : partition_csrs) {
			auto &csr = *csr_ptr;
			auto edge_count = csr.v[csr.v_array_size - 1].load(std::memory_order_relaxed);
			csr.e.resize(edge_count);
			csr.initialized_e = true;
		}
	}

	// Wait for all threads to finish setup
	local_csr_state->barrier->Wait(worker_id);

	// Determine per-thread vertex range
	idx_t vertices_per_worker =
	    (total_vertices + local_csr_state->tasks_scheduled - 1) / local_csr_state->tasks_scheduled;
	idx_t src_start = worker_id * vertices_per_worker;
	idx_t src_end = std::min(src_start + vertices_per_worker, total_vertices);

	for (idx_t src = src_start; src < src_end; src++) {
		for (idx_t i = v[src]; i < v[src + 1]; i++) {
			idx_t dst = e[i];
			idx_t local_src = reverse ? dst : src;
			idx_t local_dst = reverse ? src : dst;
			idx_t p = GetPartitionForVertex(local_dst, partition_csrs);
			auto &csr = *partition_csrs[p];
			auto &offset = csr.v[local_src + 1];
			idx_t pos = offset.fetch_add(1);
			csr.e[pos] = local_dst - csr.start_vertex;
		}
	}
}

void LocalCSRTask::FinalizeSparseRows(std::vector<shared_ptr<LocalCSR>> &partition_csrs) const {
	while (true) {
		idx_t partition_idx = local_csr_state->partition_index.fetch_add(1);
		if (partition_idx >= partition_csrs.size()) {
			break;
		}
		partition_csrs[partition_idx]->FinalizeSparseRows();
	}
}

void LocalCSRTask::DistributePullEdges(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) {
	auto &v = local_csr_state->global_csr->v;
	auto &e = local_csr_state->global_csr->e;
	idx_t total_vertices = local_csr_state->global_csr->vsize - 1;

	if (worker_id == 0) {
		for (auto &pull_csr_ptr : pull_partition_csrs) {
			auto &pull_csr = *pull_csr_ptr;
			auto edge_count = pull_csr.offsets[pull_csr.offsets_size - 1].load(std::memory_order_relaxed);
			pull_csr.predecessors.resize(edge_count);
		}
	}
	local_csr_state->barrier->Wait(worker_id);

	idx_t vertices_per_worker =
	    (total_vertices + local_csr_state->tasks_scheduled - 1) / local_csr_state->tasks_scheduled;
	idx_t src_start = worker_id * vertices_per_worker;
	idx_t src_end = std::min(src_start + vertices_per_worker, total_vertices);

	for (idx_t src = src_start; src < src_end; src++) {
		for (idx_t i = v[src]; i < v[src + 1]; i++) {
			idx_t dst = e[i];
			idx_t p = GetPullPartitionForVertex(dst, pull_partition_csrs);
			auto &pull_csr = *pull_partition_csrs[p];
			idx_t local_dst = dst - pull_csr.start_vertex;
			auto &offset = pull_csr.offsets[local_dst + 1];
			idx_t pos = offset.fetch_add(1);
			pull_csr.predecessors[pos] = static_cast<uint32_t>(src);
		}
	}
}

void LocalCSRTask::CreateRunningSum(std::vector<shared_ptr<LocalCSR>> &partition_csrs) const {
	while (true) {
		idx_t i = local_csr_state->partition_index.fetch_add(1);
		if (i >= partition_csrs.size()) {
			break;
		}

		auto &v = partition_csrs[i]->v;
		auto v_array_size = partition_csrs[i]->v_array_size;
		int64_t sum = 0;
		for (idx_t i = 0; i < v_array_size; i++) {
			auto current = v[i].load(std::memory_order_relaxed);
			v[i].store(sum, std::memory_order_relaxed);
			sum += current;
		}
	}
}

void LocalCSRTask::CreatePullRunningSum(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) const {
	while (true) {
		idx_t partition_idx = local_csr_state->partition_index.fetch_add(1);
		if (partition_idx >= pull_partition_csrs.size()) {
			break;
		}

		auto &offsets = pull_partition_csrs[partition_idx]->offsets;
		auto offsets_size = pull_partition_csrs[partition_idx]->offsets_size;
		uint64_t sum = 0;
		for (idx_t offset_idx = 0; offset_idx < offsets_size; offset_idx++) {
			auto current = offsets[offset_idx].load(std::memory_order_relaxed);
			offsets[offset_idx].store(static_cast<uint32_t>(sum), std::memory_order_relaxed);
			sum += current;
		}
	}
}

idx_t LocalCSRTask::GetPartitionForVertex(idx_t vertex, std::vector<shared_ptr<LocalCSR>> &partition_csrs) const {
	idx_t left = 0;
	idx_t right = partition_csrs.size();
	while (left < right) {
		const idx_t mid = left + (right - left) / 2;
		auto &csr = *partition_csrs[mid];
		if (vertex < csr.start_vertex) {
			right = mid;
		} else if (vertex >= csr.end_vertex) {
			left = mid + 1;
		} else {
			return mid;
		}
	}
	throw OutOfRangeException("Vertex %llu not found in any partition", vertex);
}

idx_t LocalCSRTask::GetPullPartitionForVertex(idx_t vertex,
                                              std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) const {
	idx_t left = 0;
	idx_t right = pull_partition_csrs.size();
	while (left < right) {
		idx_t mid = left + (right - left) / 2;
		auto &pull_csr = *pull_partition_csrs[mid];
		if (vertex < pull_csr.start_vertex) {
			right = mid;
		} else if (vertex >= pull_csr.end_vertex) {
			left = mid + 1;
		} else {
			return mid;
		}
	}
	throw OutOfRangeException("Vertex %llu not found in any pull partition", vertex);
}

void LocalCSRTask::CountOutgoingEdgesPerPartition(bool reverse, std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	auto &v = local_csr_state->global_csr->v;
	auto &e = local_csr_state->global_csr->e;
	if (!reverse) {
		idx_t total_edges = e.size();

		// Determine work range for this worker
		idx_t edges_per_worker = (total_edges + local_csr_state->num_threads - 1) / local_csr_state->num_threads;
		idx_t start_edge = worker_id * edges_per_worker;
		idx_t end_edge = std::min(start_edge + edges_per_worker, total_edges);

		for (idx_t src = 0; src + 1 < local_csr_state->global_csr->vsize; src++) {
			idx_t start = v[src];
			idx_t end = v[src + 1];
			if (start >= end_edge || end <= start_edge) {
				continue; // skip vertices outside this worker's edge range
			}

			idx_t local_start = std::max(start, start_edge);
			idx_t local_end = std::min(end, end_edge);
			for (idx_t i = local_start; i < local_end; i++) {
				idx_t dst = e[i];
				idx_t p = GetPartitionForVertex(dst, partition_csrs); // Map global vertex ID to partition index
				auto &csr = *partition_csrs[p];
				// Map global src to local src in partition
				csr.v[src + 1]++;
			}
		}
		return;
	}

	idx_t total_vertices = local_csr_state->global_csr->vsize - 1;
	idx_t vertices_per_worker = (total_vertices + local_csr_state->num_threads - 1) / local_csr_state->num_threads;
	idx_t src_start = worker_id * vertices_per_worker;
	idx_t src_end = std::min(src_start + vertices_per_worker, total_vertices);
	for (idx_t src = src_start; src < src_end; src++) {
		idx_t start = v[src];
		idx_t end = v[src + 1];
		for (idx_t i = start; i < end; i++) {
			idx_t dst = e[i];
			idx_t local_src = reverse ? dst : src;
			idx_t local_dst = reverse ? src : dst;
			idx_t p = GetPartitionForVertex(local_dst, partition_csrs);
			auto &csr = *partition_csrs[p];
			csr.v[local_src + 1]++;
		}
	}
}

void LocalCSRTask::CountIncomingEdgesPerPullPartition(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) {
	auto &v = local_csr_state->global_csr->v;
	auto &e = local_csr_state->global_csr->e;
	idx_t total_edges = e.size();

	idx_t edges_per_worker = (total_edges + local_csr_state->tasks_scheduled - 1) / local_csr_state->tasks_scheduled;
	idx_t start_edge = worker_id * edges_per_worker;
	idx_t end_edge = std::min(start_edge + edges_per_worker, total_edges);

	for (idx_t src = 0; src + 1 < local_csr_state->global_csr->vsize; src++) {
		idx_t start = v[src];
		idx_t end = v[src + 1];
		if (start >= end_edge || end <= start_edge) {
			continue;
		}

		idx_t local_start = std::max(start, start_edge);
		idx_t local_end = std::min(end, end_edge);
		for (idx_t i = local_start; i < local_end; i++) {
			idx_t dst = e[i];
			idx_t p = GetPullPartitionForVertex(dst, pull_partition_csrs);
			auto &pull_csr = *pull_partition_csrs[p];
			pull_csr.offsets[dst - pull_csr.start_vertex + 1]++;
		}
	}
}

void LocalCSRTask::DeterminePartitions(std::vector<int64_t> &statistics_chunks,
                                       std::vector<shared_ptr<LocalCSR>> &partition_csrs,
                                       bool initialize_vertex_arrays) const {
	const idx_t max_vertex = local_csr_state->vsize;

	// Get edge histogram across 256 chunks
	const auto &edge_histogram = statistics_chunks; // e.g., vector<idx_t> of size 256
	D_ASSERT(edge_histogram.size() == BUCKET_COUNT);

	// Total edge count
	idx_t total_edges = 0;
	for (auto count : edge_histogram) {
		total_edges += count;
	}

	// Split into heavy and light partitions
	idx_t heavy_partition_count = local_csr_state->num_threads;
	idx_t light_partition_count = local_csr_state->num_threads * GetLightPartitionMultiplier(local_csr_state->context);

	idx_t heavy_edge_budget = static_cast<idx_t>(total_edges * GetHeavyPartitionFraction(local_csr_state->context));
	idx_t light_edge_budget = total_edges - heavy_edge_budget;

	idx_t heavy_target_per_partition = heavy_edge_budget / heavy_partition_count;
	idx_t light_target_per_partition = light_edge_budget / light_partition_count;

	idx_t current_chunk = 0;
	idx_t chunk_size = (max_vertex + BUCKET_COUNT - 1) / BUCKET_COUNT;

	partition_csrs.clear();

	auto create_partition = [&](idx_t start_chunk, idx_t end_chunk) {
		idx_t start_vertex = start_chunk * chunk_size;
		idx_t end_vertex = std::min(end_chunk * chunk_size, max_vertex);

		// If the vertex range exceeds UINT16_MAX, split into multiple subpartitions
		while ((end_vertex - start_vertex) > UINT16_MAX) {
			idx_t mid_vertex = start_vertex + UINT16_MAX;
			auto csr = make_shared_ptr<LocalCSR>(start_vertex, mid_vertex, max_vertex, initialize_vertex_arrays);
			partition_csrs.push_back(csr);
			start_vertex = mid_vertex;
		}

		// Final partition covering the remainder
		if (start_vertex < end_vertex) {
			auto csr = make_shared_ptr<LocalCSR>(start_vertex, end_vertex, max_vertex, initialize_vertex_arrays);
			partition_csrs.push_back(csr);
		}
	};

	// Create heavy partitions
	for (idx_t i = 0; i < heavy_partition_count && current_chunk < BUCKET_COUNT; i++) {
		idx_t edge_sum = 0;
		idx_t start_chunk = current_chunk;

		while (current_chunk < BUCKET_COUNT && edge_sum < heavy_target_per_partition) {
			edge_sum += edge_histogram[current_chunk];
			current_chunk++;
		}

		create_partition(start_chunk, current_chunk);
	}

	// Create light partitions
	for (idx_t i = 0; i < light_partition_count && current_chunk < BUCKET_COUNT; i++) {
		idx_t edge_sum = 0;
		idx_t start_chunk = current_chunk;

		while (current_chunk < BUCKET_COUNT && edge_sum < light_target_per_partition) {
			edge_sum += edge_histogram[current_chunk];
			current_chunk++;
		}

		create_partition(start_chunk, current_chunk);
	}

	// If there are leftover chunks (in case of rounding), wrap them up
	if (current_chunk < BUCKET_COUNT) {
		create_partition(current_chunk, BUCKET_COUNT);
	}
}

void LocalCSRTask::DeterminePullPartitions(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) const {
	for (const auto &local_csr : local_csr_state->partition_csrs) {
		pull_partition_csrs.push_back(make_shared_ptr<PullCSR>(local_csr->start_vertex, local_csr->end_vertex));
	}
}

void LocalCSRTask::CreateStatistics(bool reverse, std::vector<int64_t> &statistics_chunks) const {
	// References to CSR data
	auto &v = local_csr_state->global_csr->v;
	auto &e = local_csr_state->global_csr->e;
	idx_t total_edges = e.size();
	idx_t max_col = local_csr_state->global_csr->vsize; // max column index is #vertices

	// Determine work range for this worker
	idx_t edges_per_worker = (total_edges + local_csr_state->tasks_scheduled - 1) / local_csr_state->tasks_scheduled;
	idx_t start_edge = worker_id * edges_per_worker;
	idx_t end_edge = std::min(start_edge + edges_per_worker, total_edges);

	// Temporary local histogram to reduce contention
	std::vector<int64_t> local_chunks(BUCKET_COUNT, 0);

	// Compute shift so that max_col fits in BUCKET_COUNT buckets
	idx_t bucket_bits = __builtin_ctz(BUCKET_COUNT); // log2(BUCKET_COUNT)
	idx_t bucket_shift = 0;
	while ((1ULL << (bucket_shift + bucket_bits)) < max_col) {
		bucket_shift++;
	}

	if (reverse) {
		idx_t total_vertices = local_csr_state->global_csr->vsize - 1;
		idx_t vertices_per_worker =
		    (total_vertices + local_csr_state->tasks_scheduled - 1) / local_csr_state->tasks_scheduled;
		idx_t src_start = worker_id * vertices_per_worker;
		idx_t src_end = std::min(src_start + vertices_per_worker, total_vertices);
		for (idx_t src = src_start; src < src_end; src++) {
			idx_t bucket = (src >> bucket_shift) & BUCKET_MASK;
			local_chunks[bucket] += v[src + 1] - v[src];
		}
	} else {
		// Bucket edges by destination vertex using bitshift and mask
		for (idx_t i = start_edge; i < end_edge; i++) {
			idx_t bucket = (e[i] >> bucket_shift) & BUCKET_MASK;
			local_chunks[bucket]++;
		}
	}

	// Merge local result into global histogram (atomic add)
	for (idx_t i = 0; i < BUCKET_COUNT; i++) {
		__atomic_fetch_add(&statistics_chunks[i], local_chunks[i], __ATOMIC_RELAXED);
	}
}

} // namespace duckdb
