
#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"

#include <duckpgq/core/functions/scalar.hpp>

namespace duckdb {

static void ShortestPathOperatorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw NotImplementedException(
	    "ShortestPathOperatorFunction not implemented, should have gone to the operator instead.");
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterShortestPathOperatorScalarFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(ScalarFunction("shortestpathoperator",
	                                       {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                       LogicalType::LIST(LogicalType::BIGINT), ShortestPathOperatorFunction,
	                                       ShortestPathOperatorData::ShortestPathOperatorBind));
}

} // namespace duckdb
