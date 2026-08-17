#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"
#include "duckpgq/core/utils/duckpgq_barrier.hpp"

#include <chrono>

namespace duckdb {

struct Partition {
	idx_t start_bucket;
	idx_t end_bucket; // exclusive
};

struct LocalCSRSubphaseTiming {
	bool reverse;
	string phase;
	double time_ms;
};

struct LocalCSRBuildPartition {
	vector<uint32_t> source_vertices;
	vector<uint32_t> row_offsets;
	vector<uint16_t> destinations;

	void Append(idx_t source, idx_t destination) {
		if (source_vertices.empty() || source_vertices.back() != source) {
			source_vertices.push_back(NumericCast<uint32_t>(source));
			row_offsets.push_back(NumericCast<uint32_t>(destinations.size()));
		}
		destinations.push_back(NumericCast<uint16_t>(destination));
	}

	void Finalize() {
		if (!source_vertices.empty() && row_offsets.size() == source_vertices.size()) {
			row_offsets.push_back(NumericCast<uint32_t>(destinations.size()));
		}
	}
};

class LocalCSRState {
public:
	LocalCSRState(ClientContext &context_p, CSR *csr, idx_t num_threads_p);
	LocalCSRState(ClientContext &context_p,
	              std::vector<std::vector<LocalCSRBuildPartition>> &&streaming_build_buffers_p, idx_t vertex_count_p,
	              idx_t edge_count_p, idx_t partition_width_p, idx_t num_threads_p);

public:
	CSR *global_csr;
	idx_t vsize;
	idx_t edge_count;
	idx_t streaming_partition_width = 0;
	bool streaming_endpoint_input = false;
	ClientContext &context;

	idx_t num_threads;
	idx_t tasks_scheduled;

	unique_ptr<Barrier> barrier;

	std::vector<int64_t> statistics_chunks;
	std::vector<int64_t> reverse_statistics_chunks;
	std::vector<shared_ptr<LocalCSR>> partition_csrs;
	std::vector<shared_ptr<LocalCSR>> reverse_partition_csrs;
	std::vector<shared_ptr<PullCSR>> pull_partition_csrs;
	std::vector<std::vector<LocalCSRBuildPartition>> forward_build_buffers;
	std::vector<LocalCSRSubphaseTiming> subphase_timings;
	std::atomic<idx_t> partition_index;
	bool build_forward_csr;
	bool build_reverse_csr;
	bool build_pull_csr;
	bool finalize_sparse_rows;
	bool benchmark_enabled;
	bool loaded_from_cache = false;
	bool published_to_cache = false;
	bool rebuild_timing_started = false;
	uint64_t expected_persistence_generation = 0;
	string cache_key;
	string benchmark_output_prefix;
	string benchmark_run_id;
	std::chrono::steady_clock::time_point forward_start_time;
	std::chrono::steady_clock::time_point forward_end_time;
	std::chrono::steady_clock::time_point reverse_start_time;
	std::chrono::steady_clock::time_point reverse_end_time;
	std::chrono::steady_clock::time_point pull_start_time;
	std::chrono::steady_clock::time_point pull_end_time;
	std::chrono::steady_clock::time_point rebuild_start_time;
};

} // namespace duckdb
