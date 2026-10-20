
#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"

#include <duckpgq/core/functions/scalar.hpp>

namespace duckdb {

static void ShortestPathOperatorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw NotImplementedException("shortestpathoperator is a planning marker that the path-finding optimizer rule did "
	                              "not replace: this query shape is not supported by the path-finding operator. Set "
	                              "experimental_path_finding_operator = false to use the SQL fallback.");
}

static ScalarFunction ShortestPathMarkerFunction(vector<LogicalType> arguments) {
	auto result = ScalarFunction(std::move(arguments), LogicalType::LIST(LogicalType::BIGINT),
	                             ShortestPathOperatorFunction, ShortestPathOperatorData::ShortestPathOperatorBind);
	// The optimizer must replace this marker with the physical operator before execution.
	result.SetStability(FunctionStability::VOLATILE);
	return result;
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterShortestPathOperatorScalarFunction(ExtensionLoader &loader) {
	ScalarFunctionSet functions("shortestpathoperator");
	// Legacy global-CSR form: (src, dst, csr_id)
	functions.AddFunction(ShortestPathMarkerFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT}));
	// Cache-only form over a CSR index: (src, dst, vertex_count, edge_count, cache_key[, max_depth])
	functions.AddFunction(ShortestPathMarkerFunction(
	    {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}));
	functions.AddFunction(ShortestPathMarkerFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                                  LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BIGINT}));
	// Buffered endpoint form with edge row ids: (src, dst, {src, dst, id}, vertex_count, edge_count, cache_key[,
	// max_depth])
	auto endpoint_type =
	    LogicalType::STRUCT({{"src", LogicalType::BIGINT}, {"dst", LogicalType::BIGINT}, {"id", LogicalType::BIGINT}});
	functions.AddFunction(ShortestPathMarkerFunction({LogicalType::BIGINT, LogicalType::BIGINT, endpoint_type,
	                                                  LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR}));
	functions.AddFunction(
	    ShortestPathMarkerFunction({LogicalType::BIGINT, LogicalType::BIGINT, endpoint_type, LogicalType::BIGINT,
	                                LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BIGINT}));
	loader.RegisterFunction(functions);
}

} // namespace duckdb
