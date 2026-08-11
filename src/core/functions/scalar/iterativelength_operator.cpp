
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

static void PushPullIterativeLengthOperatorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw NotImplementedException(
	    "PushPullIterativeLengthOperatorFunction not implemented, should have gone to the operator instead.");
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterIterativeLengthOperatorScalarFunction(ExtensionLoader &loader) {
	ScalarFunctionSet functions("iterativelengthoperator");
	functions.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::BIGINT, IterativeLengthOperatorFunction,
	                                     ShortestPathOperatorData::ShortestPathOperatorBind));
	functions.AddFunction(ScalarFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}, LogicalType::BIGINT,
	    IterativeLengthOperatorFunction, ShortestPathOperatorData::ShortestPathOperatorBind));
	functions.AddFunction(ScalarFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
	    LogicalType::BIGINT, IterativeLengthOperatorFunction, ShortestPathOperatorData::ShortestPathOperatorBind));
	functions.AddFunction(ScalarFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	     LogicalType::BIGINT, LogicalType::VARCHAR},
	    LogicalType::BIGINT, IterativeLengthOperatorFunction, ShortestPathOperatorData::ShortestPathOperatorBind));
	auto endpoint_type = LogicalType::STRUCT({{"src", LogicalType::BIGINT}, {"dst", LogicalType::BIGINT}});
	functions.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, endpoint_type, endpoint_type,
	                                      LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
	                                     LogicalType::BIGINT, IterativeLengthOperatorFunction,
	                                     ShortestPathOperatorData::ShortestPathOperatorBind));
	loader.RegisterFunction(functions);
}

void CoreScalarFunctions::RegisterPushPullIterativeLengthOperatorScalarFunction(ExtensionLoader &loader) {
	ScalarFunctionSet functions("pushpulliterativelengthoperator");
	functions.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::BIGINT, PushPullIterativeLengthOperatorFunction,
	                                     ShortestPathOperatorData::ShortestPathOperatorBind));
	functions.AddFunction(ScalarFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}, LogicalType::BIGINT,
	    PushPullIterativeLengthOperatorFunction, ShortestPathOperatorData::ShortestPathOperatorBind));
	loader.RegisterFunction(functions);
}

void CoreScalarFunctions::RegisterBidirectionalIterativeLengthOperatorScalarFunction(ExtensionLoader &loader) {
	ScalarFunctionSet functions("bidirectionaliterativelengthoperator");
	functions.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::BIGINT, BidirectionalIterativeLengthOperatorFunction,
	                                     ShortestPathOperatorData::ShortestPathOperatorBind));
	functions.AddFunction(ScalarFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}, LogicalType::BIGINT,
	    BidirectionalIterativeLengthOperatorFunction, ShortestPathOperatorData::ShortestPathOperatorBind));
	loader.RegisterFunction(functions);
}

} // namespace duckdb
