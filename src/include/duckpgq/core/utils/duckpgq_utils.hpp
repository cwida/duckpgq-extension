#pragma once
#include "duckpgq_extension.hpp"

namespace duckdb {

// Function to get DuckPGQState from ClientContext
DuckPGQState *GetDuckPGQState(ClientContext &context);

} // namespace duckdb

