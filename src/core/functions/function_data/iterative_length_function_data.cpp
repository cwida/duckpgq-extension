#include "duckpgq/core/functions/function_data/iterative_length_function_data.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckpgq/common.hpp"

#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckdb {

unique_ptr<FunctionData> IterativeLengthFunctionData::Copy() const {
	return make_uniq<IterativeLengthFunctionData>(context, csr_id);
}

bool IterativeLengthFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<IterativeLengthFunctionData>();
	return other.csr_id == csr_id;
}

unique_ptr<FunctionData> IterativeLengthFunctionData::IterativeLengthBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("Id must be constant.");
	}

	int32_t csr_id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]).GetValue<int32_t>();
	auto duckpgq_state = GetDuckPGQState(context);
	duckpgq_state->csr_to_delete.insert(csr_id);

	return make_uniq<IterativeLengthFunctionData>(context, csr_id);
}

} // namespace duckdb
