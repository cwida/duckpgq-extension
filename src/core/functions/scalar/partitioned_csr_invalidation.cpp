#include "duckpgq/core/functions/scalar.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/core/operator/partitioned_csr_persistence.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <chrono>

namespace duckdb {

static void PartitionedCSRInvalidationFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto start_time = std::chrono::steady_clock::now();
	UnifiedVectorFormat keys;
	args.data[0].ToUnifiedFormat(keys);
	auto key_data = UnifiedVectorFormat::GetData<string_t>(keys);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<bool>(result);
	for (idx_t row_idx = 0; row_idx < args.size(); row_idx++) {
		auto key_idx = keys.sel->get_index(row_idx);
		if (!keys.validity.RowIsValid(key_idx)) {
			FlatVector::SetNull(result, row_idx, true);
			continue;
		}
		MarkPartitionedCSRInvalidated(state.GetContext(), key_data[key_idx].GetString());
		result_data[row_idx] = false;
	}
	auto end_time = std::chrono::steady_clock::now();
	auto invalidation_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
	AppendOperatorPhaseTiming(state.GetContext(), "partitioned_csr_invalidation", 1, args.size(), args.size(), 0,
	                          invalidation_ms, 0);
}

void CoreScalarFunctions::RegisterPartitionedCSRInvalidationScalarFunction(ExtensionLoader &loader) {
	auto function = ScalarFunction("duckpgq_mark_csr_invalid", {LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                               PartitionedCSRInvalidationFunction);
	function.SetStability(FunctionStability::VOLATILE);
	loader.RegisterFunction(function);
}

} // namespace duckdb
