#include "duckpgq/core/operator/local_csr/local_csr_state.hpp"

#include <duckpgq/core/operator/local_csr/local_csr_event.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

#include <chrono>
#include <iomanip>
#include <sstream>

namespace duckdb {

static string CreateLocalCSRBenchmarkRunId() {
	auto now = std::chrono::system_clock::now();
	auto time_t_now = std::chrono::system_clock::to_time_t(now);
	std::stringstream ss;
	ss << std::put_time(std::localtime(&time_t_now), "%Y-%m-%d_%H-%M-%S");
	return ss.str();
}

LocalCSRState::LocalCSRState(ClientContext &context_p, CSR *csr_p, idx_t num_threads_p)
    : context(context_p), num_threads(num_threads_p), statistics_chunks(BUCKET_COUNT, 0),
      reverse_statistics_chunks(BUCKET_COUNT, 0) {
	global_csr = csr_p;
	tasks_scheduled = 0;
	partition_index = 0;
	build_reverse_csr = GetPathFindingBuildReverseCSR(context);
	build_pull_csr = false;
	finalize_sparse_rows = true;
	benchmark_enabled = GetPathFindingBenchmarkOption(context);
	benchmark_output_prefix = GetPathFindingBenchmarkPrefix(context);
	benchmark_run_id = CreateLocalCSRBenchmarkRunId();
}

} // namespace duckdb
