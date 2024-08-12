#include "duckpgq/core/utils/duckpgq_utils.hpp"
#include "duckpgq/common.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"

#include "duckpgq/core/functions/table/describe_property_graph.hpp"
#include "duckpgq/core/functions/table/drop_property_graph.hpp"

namespace duckpgq {

namespace core {
// Function to get DuckPGQState from ClientContext
DuckPGQState * GetDuckPGQState(ClientContext &context) {
  auto lookup = context.registered_state.find("duckpgq");
  if (lookup == context.registered_state.end()) {
    throw Exception(ExceptionType::INVALID, "Registered DuckPGQ state not found");
  }
  return dynamic_cast<DuckPGQState*>(lookup->second.get());
}
} // namespace core
} // namespace duckpgq