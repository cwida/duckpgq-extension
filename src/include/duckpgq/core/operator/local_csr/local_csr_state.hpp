#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"
#include "duckpgq/core/utils/duckpgq_barrier.hpp"

namespace duckpgq {

namespace core {

struct Partition {
  idx_t start_bucket;
  idx_t end_bucket; // exclusive
};

class LocalCSRState {
public:
  LocalCSRState(ClientContext &context_p, CSR *csr, idx_t num_threads_p);

public:
  CSR* global_csr;
  ClientContext &context;

  idx_t num_threads;
  idx_t tasks_scheduled;

  unique_ptr<Barrier> barrier;

  std::vector<int64_t> statistics_chunks;
  std::vector<shared_ptr<LocalCSR>> partition_csrs;
  std::atomic<idx_t> partition_index;
};

} // namespace core

} // namespace duckpgq
