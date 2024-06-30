#include "duckpgq/utils/duckpgq_utils.hpp"

namespace duckdb {
// Function to get DuckPGQState from ClientContext
DuckPGQState * GetDuckPGQState(ClientContext &context) {
  auto lookup = context.registered_state.find("duckpgq");
  if (lookup == context.registered_state.end()) {
    throw Exception(ExceptionType::INVALID, "Registered DuckPGQ state not found");
  }
  return dynamic_cast<DuckPGQState*>(lookup->second.get());
}

} // namespace duckdb
