#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"

#include <duckpgq/core/functions/scalar.hpp>

namespace duckdb {

static void ReachOperatorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw NotImplementedException("reachoperator is a planning marker that the path-finding optimizer rule did not "
	                              "replace: this query shape is not supported by the path-finding operator.");
}

LogicalType ReachOperatorResultType() {
	// [dst, len] as a list rather than a struct: DuckDB's unused-columns optimizer pulls struct field extraction up
	// through projections and its statistics pass then crashes on the marker's column; lists are left alone
	return LogicalType::LIST(LogicalType::BIGINT);
}

//! Unknown statistics of the result type
static unique_ptr<BaseStatistics> ReachOperatorStatistics(ClientContext &context, FunctionStatisticsInput &input) {
	return BaseStatistics::CreateUnknown(ReachOperatorResultType()).ToUnique();
}

static ScalarFunction ReachMarkerFunction(vector<LogicalType> arguments) {
	auto result = ScalarFunction(std::move(arguments), ReachOperatorResultType(), ReachOperatorFunction,
	                             ShortestPathOperatorData::ShortestPathOperatorBind);
	result.SetStability(FunctionStability::VOLATILE);
	result.SetStatisticsCallback(ReachOperatorStatistics);
	return result;
}

//! reachoperator(src, src, ...) has the argument forms of iterativelengthoperator (the second argument is ignored
//! and conventionally repeats the source) and yields one row per vertex reachable from the source: a list
//! [dst, len] with the vertex (key over an index, row id otherwise) and its hop distance, the source itself at 0.
void CoreScalarFunctions::RegisterReachOperatorScalarFunction(ExtensionLoader &loader) {
	ScalarFunctionSet functions("reachoperator");
	// Cache-only form over a CSR index: (src, src, vertex_count, edge_count, cache_key[, max_depth])
	functions.AddFunction(ReachMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}));
	functions.AddFunction(ReachMarkerFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                           LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BIGINT}));
	// Buffered endpoint form: (src, src, {src, dst}, vertex_count, edge_count, cache_key[, max_depth])
	auto endpoint_type = LogicalType::STRUCT({{"src", LogicalType::BIGINT}, {"dst", LogicalType::BIGINT}});
	functions.AddFunction(ReachMarkerFunction({LogicalType::BIGINT, LogicalType::BIGINT, endpoint_type,
	                                           LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}));
	functions.AddFunction(ReachMarkerFunction({LogicalType::BIGINT, LogicalType::BIGINT, endpoint_type,
	                                           LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR,
	                                           LogicalType::BIGINT}));
	loader.RegisterFunction(functions);
}

} // namespace duckdb
