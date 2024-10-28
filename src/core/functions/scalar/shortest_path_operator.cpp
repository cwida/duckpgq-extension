
#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"

#include <duckpgq/core/functions/scalar.hpp>

namespace duckpgq {

namespace core {

static void ShortestPathOperatorFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  throw NotImplementedException("ShortestPathOperatorFunction not implemented, should have gone to the operator instead.");
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterShortestPathOperatorScalarFunction(
    DatabaseInstance &db) {

  ExtensionUtil::RegisterFunction(
  db, ScalarFunction("shortestpathoperator", {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
                                              LogicalType::LIST(LogicalType::BIGINT), ShortestPathOperatorFunction,
                                              ShortestPathOperatorData::ShortestPathOperatorBind));
}


} // namespace core

} // namespace duckpgq