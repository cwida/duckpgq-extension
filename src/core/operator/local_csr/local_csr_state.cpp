#include "duckpgq/core/operator/local_csr/local_csr_state.hpp"

#include <duckpgq/core/operator/local_csr/local_csr_event.hpp>

namespace duckpgq {

namespace core {

LocalCSRState::LocalCSRState(ClientContext &context_p, CSR *csr_p,
                             idx_t num_threads_p)
    : context(context_p), num_threads(num_threads_p),
      statistics_chunks(BUCKET_COUNT, 0) {
  global_csr = csr_p;
  tasks_scheduled = 0;
  partition_index = 0;
}

} // namespace core

} // namespace duckpgq