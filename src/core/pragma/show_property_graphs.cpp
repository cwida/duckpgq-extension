#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckpgq/core/pragma/duckpgq_pragma.hpp>

namespace duckpgq {

namespace core {

static string PragmaShowPropertyGraphs(ClientContext &context,
                                       const FunctionParameters &parameters) {
  return "SELECT DISTINCT property_graph from __duckpgq_internal";
}

void CorePGQPragma::RegisterShowPropertyGraphs(DatabaseInstance &instance) {
  // Define the pragma function
  auto pragma_func = PragmaFunction::PragmaCall(
      "show_property_graphs",   // Name of the pragma
      PragmaShowPropertyGraphs, // Query substitution function
      {}                        // Parameter types (mail_limit is an integer)
  );

  // Register the pragma function
  ExtensionUtil::RegisterFunction(instance, pragma_func);
}

} // namespace core

} // namespace duckpgq
