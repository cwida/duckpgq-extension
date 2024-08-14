
#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"

#include <duckpgq/core/functions/scalar.hpp>

namespace duckpgq {

namespace core {

static void IterativeLengthOperatorFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  throw NotImplementedException("IterativeLengthOperatorFunction not implemented, should have gone to the operator instead.");
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterIterativeLengthOperatorScalarFunction(DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(
  db, ScalarFunction("iterativelengthoperator", {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
                                              LogicalType::BIGINT, IterativeLengthOperatorFunction,
                                              ShortestPathOperatorData::ShortestPathOperatorBind));
}

} // namespace core

} // namespace duckpgq