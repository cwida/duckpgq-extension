#include "duckpgq/core/functions/scalar.hpp"
#include "duckpgq/core/utils/duckpgq_utils.hpp"

namespace duckdb {

//! Returns the decisions the most recent path-finding operator of this connection took (orientation, scheduling,
//! CSR source, ...) as key=value pairs separated by semicolons. Benchmark harnesses record it next to the timing.
static void PathFindingStrategyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto strategy = GetDuckPGQState(state.GetContext())->GetPathFindingStrategy();
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value(strategy));
}

void CoreScalarFunctions::RegisterPathFindingStrategyScalarFunction(ExtensionLoader &loader) {
	auto function = ScalarFunction("duckpgq_path_finding_strategy", {}, LogicalType::VARCHAR,
	                               PathFindingStrategyFunction);
	function.SetStability(FunctionStability::VOLATILE);
	loader.RegisterFunction(function);
}

} // namespace duckdb
