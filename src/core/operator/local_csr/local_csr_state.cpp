#include "duckpgq/core/operator/local_csr/local_csr_state.hpp"

#include <duckpgq/core/operator/bfs_benchmark.hpp>
#include <duckpgq/core/operator/local_csr/local_csr_event.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

namespace duckdb {

LocalCSRState::LocalCSRState(ClientContext &context_p, CSR *csr_p, idx_t num_threads_p)
    : context(context_p), num_threads(num_threads_p), statistics_chunks(BUCKET_COUNT, 0),
      reverse_statistics_chunks(BUCKET_COUNT, 0) {
	global_csr = csr_p;
	vsize = csr_p->vsize;
	edge_count = csr_p->e.size();
	partition_index = 0;
	build_forward_csr = true;
	build_reverse_csr = GetPathFindingBuildReverseCSR(context);
	build_pull_csr = false;
	finalize_sparse_rows = true;
	benchmark_enabled = GetPathFindingBenchmarkOption(context);
	benchmark_output_prefix = GetPathFindingBenchmarkPrefix(context);
	benchmark_run_id = CreateBenchmarkRunId();
}

LocalCSRState::LocalCSRState(ClientContext &context_p,
	                         std::vector<std::vector<LocalCSRBuildPartition>> &&streaming_build_buffers_p,
	                         idx_t vertex_count_p, idx_t edge_count_p, idx_t partition_width_p, idx_t num_threads_p)
	    : global_csr(nullptr), vsize(vertex_count_p + 2), edge_count(edge_count_p),
	      streaming_partition_width(partition_width_p), streaming_endpoint_input(true), context(context_p),
	      num_threads(num_threads_p), statistics_chunks(BUCKET_COUNT, 0), reverse_statistics_chunks(BUCKET_COUNT, 0),
	      forward_build_buffers(std::move(streaming_build_buffers_p)) {
	partition_index = 0;
	build_forward_csr = true;
	build_reverse_csr = false;
	build_pull_csr = false;
	finalize_sparse_rows = true;
	benchmark_enabled = GetPathFindingBenchmarkOption(context);
	benchmark_output_prefix = GetPathFindingBenchmarkPrefix(context);
	benchmark_run_id = CreateBenchmarkRunId();
}

} // namespace duckdb
