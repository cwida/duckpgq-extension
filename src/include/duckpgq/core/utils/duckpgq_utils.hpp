#pragma once
#include "duckpgq_state.hpp"

namespace duckpgq {

namespace core {

#define LANE_LIMIT 512
#define VISIT_SIZE_DIVISOR 2

// Function to get DuckPGQState from ClientContext
DuckPGQState *GetDuckPGQState(ClientContext &context);

} // namespace core

} // namespace duckpgq
