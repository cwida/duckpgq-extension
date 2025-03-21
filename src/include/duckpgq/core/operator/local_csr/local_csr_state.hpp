#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

namespace duckpgq {

namespace core {
class LocalCSRState {
public:
  LocalCSRState(ClientContext &context_p, CSR* csr);

public:
  CSR* global_csr;
  ClientContext &context;
  std::vector<std::pair<idx_t, idx_t>> partition_ranges;
  std::vector<shared_ptr<LocalCSR>> local_csrs;
  std::vector<shared_ptr<LocalCSR>> local_csrs_to_partition;
};

} // namespace core

} // namespace duckpgq
