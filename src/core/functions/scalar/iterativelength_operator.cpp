
#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"

#include <duckpgq/core/functions/scalar.hpp>

namespace duckdb {

static ScalarFunction PathFindingMarkerFunction(vector<LogicalType> arguments, scalar_function_t function) {
	auto result = ScalarFunction(std::move(arguments), LogicalType::BIGINT, std::move(function),
	                             ShortestPathOperatorData::ShortestPathOperatorBind);
	// The optimizer must replace this marker with the physical operator before execution.
	result.SetStability(FunctionStability::VOLATILE);
	return result;
}

[[noreturn]] static void ThrowMarkerReachedExecution(const char *function_name) {
	throw NotImplementedException("%s is a planning marker that the path-finding optimizer rule did not replace: this "
	                              "query shape is not supported by the path-finding operator. Set "
	                              "experimental_path_finding_operator = false to use the SQL fallback.",
	                              function_name);
}

static void IterativeLengthOperatorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ThrowMarkerReachedExecution("iterativelengthoperator");
}

static void BidirectionalIterativeLengthOperatorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ThrowMarkerReachedExecution("bidirectionaliterativelengthoperator");
}

static void PushPullIterativeLengthOperatorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	ThrowMarkerReachedExecution("pushpulliterativelengthoperator");
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterIterativeLengthOperatorScalarFunction(ExtensionLoader &loader) {
	ScalarFunctionSet functions("iterativelengthoperator");
	functions.AddFunction(PathFindingMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT}, IterativeLengthOperatorFunction));
	functions.AddFunction(PathFindingMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
	    IterativeLengthOperatorFunction));
	functions.AddFunction(PathFindingMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
	    IterativeLengthOperatorFunction));
	functions.AddFunction(PathFindingMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	     LogicalType::BIGINT, LogicalType::VARCHAR},
	    IterativeLengthOperatorFunction));
	auto endpoint_type = LogicalType::STRUCT({{"src", LogicalType::BIGINT}, {"dst", LogicalType::BIGINT}});
	functions.AddFunction(PathFindingMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, endpoint_type, LogicalType::BIGINT, LogicalType::BIGINT,
	     LogicalType::VARCHAR},
	    IterativeLengthOperatorFunction));
	functions.AddFunction(PathFindingMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, endpoint_type, endpoint_type, LogicalType::BIGINT,
	     LogicalType::BIGINT, LogicalType::VARCHAR},
	    IterativeLengthOperatorFunction));
	loader.RegisterFunction(functions);
}

void CoreScalarFunctions::RegisterPushPullIterativeLengthOperatorScalarFunction(ExtensionLoader &loader) {
	ScalarFunctionSet functions("pushpulliterativelengthoperator");
	functions.AddFunction(PathFindingMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT}, PushPullIterativeLengthOperatorFunction));
	functions.AddFunction(PathFindingMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
	    PushPullIterativeLengthOperatorFunction));
	loader.RegisterFunction(functions);
}

void CoreScalarFunctions::RegisterBidirectionalIterativeLengthOperatorScalarFunction(ExtensionLoader &loader) {
	ScalarFunctionSet functions("bidirectionaliterativelengthoperator");
	functions.AddFunction(PathFindingMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT}, BidirectionalIterativeLengthOperatorFunction));
	functions.AddFunction(PathFindingMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR},
	    BidirectionalIterativeLengthOperatorFunction));
	loader.RegisterFunction(functions);
}

} // namespace duckdb
