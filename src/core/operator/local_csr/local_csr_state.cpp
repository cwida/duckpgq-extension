#include "duckpgq/core/operator/local_csr/local_csr_state.hpp"

#include <duckpgq/core/operator/local_csr/local_csr_event.hpp>

namespace duckpgq {

namespace core {

LocalCSRState::LocalCSRState(ClientContext &context_p, CSR* csr_p) : context(context_p) {
  global_csr = csr_p;
  local_csrs.clear();
  partition_ranges.clear();
  shared_ptr<LocalCSR> initial_local_csr;
  for (const auto v_: global_csr->v) {
    initial_local_csr->v.push_back(v_);
  }
  for (const auto e_: global_csr->e) {
    initial_local_csr->e.push_back(e_);
  }
  initial_local_csr->start_vertex = 0;
  initial_local_csr->end_vertex = initial_local_csr->GetVertexSize();
  local_csrs_to_partition.push_back(initial_local_csr);
}




} // namespace core

} // namespace duckpgq