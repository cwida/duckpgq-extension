
#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"

#include <duckpgq/core/functions/scalar.hpp>

namespace duckdb {

static void IterativeLengthOperatorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw NotImplementedException(
	    "IterativeLengthOperatorFunction not implemented, should have gone to the operator instead.");
}

static void BidirectionalIterativeLengthOperatorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw NotImplementedException(
	    "BidirectionalIterativeLengthOperatorFunction not implemented, should have gone to the operator instead.");
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterIterativeLengthOperatorScalarFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(ScalarFunction(
	    "iterativelengthoperator", {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::BIGINT,
	    IterativeLengthOperatorFunction, ShortestPathOperatorData::ShortestPathOperatorBind));
}

void CoreScalarFunctions::RegisterBidirectionalIterativeLengthOperatorScalarFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(ScalarFunction("bidirectionaliterativelengthoperator",
	                                       {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                       LogicalType::BIGINT, BidirectionalIterativeLengthOperatorFunction,
	                                       ShortestPathOperatorData::ShortestPathOperatorBind));
}

} // namespace duckdb
