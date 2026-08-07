#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"

namespace duckdb {

void ExecuteIterativeLengthBatch(IterativeLengthState &state, idx_t worker_id);

} // namespace duckdb
