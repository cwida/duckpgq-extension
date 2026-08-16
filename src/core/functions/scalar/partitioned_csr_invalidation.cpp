#include "duckpgq/core/functions/scalar.hpp"
#include "duckpgq/core/operator/partitioned_csr_persistence.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

static void PartitionedCSRInvalidationFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
}

void CoreScalarFunctions::RegisterPartitionedCSRInvalidationScalarFunction(ExtensionLoader &loader) {
	auto function = ScalarFunction("duckpgq_mark_csr_invalid", {LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                               PartitionedCSRInvalidationFunction);
	function.SetStability(FunctionStability::VOLATILE);
	loader.RegisterFunction(function);
}

} // namespace duckdb
