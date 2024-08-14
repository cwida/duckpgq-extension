
#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"

#include <duckpgq/core/functions/scalar.hpp>

namespace duckpgq {

namespace core {

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterShortestPathScalarFunction(
    DatabaseInstance &db) {

  ExtensionUtil::RegisterFunction(
  db, ScalarFunction("shortestpathoperator", {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
                                              LogicalType::LIST(LogicalType::BIGINT), ShortestPathOperatorFunction,
                                              ShortestPathOperatorData::ShortestPathOperatorBind));
}


} // namespace core

} // namespace duckpgq