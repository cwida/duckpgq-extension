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

class LocalCSRState {
public:
	LocalCSRState(ClientContext &context_p, CSR *csr, idx_t num_threads_p);

public:
	CSR *global_csr;
	ClientContext &context;

	idx_t num_threads;
	idx_t tasks_scheduled;

	unique_ptr<Barrier> barrier;

	std::vector<int64_t> statistics_chunks;
	std::vector<int64_t> reverse_statistics_chunks;
	std::vector<shared_ptr<LocalCSR>> partition_csrs;
	std::vector<shared_ptr<LocalCSR>> reverse_partition_csrs;
	std::vector<shared_ptr<PullCSR>> pull_partition_csrs;
	std::atomic<idx_t> partition_index;
	bool build_reverse_csr;
	bool build_pull_csr;
	bool finalize_sparse_rows;
	bool benchmark_enabled;
	string benchmark_output_prefix;
	string benchmark_run_id;
	std::chrono::steady_clock::time_point forward_start_time;
	std::chrono::steady_clock::time_point forward_end_time;
	std::chrono::steady_clock::time_point reverse_start_time;
	std::chrono::steady_clock::time_point reverse_end_time;
	std::chrono::steady_clock::time_point pull_start_time;
	std::chrono::steady_clock::time_point pull_end_time;
};

} // namespace duckdb
