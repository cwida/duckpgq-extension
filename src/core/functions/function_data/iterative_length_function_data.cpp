#include "duckpgq/core/functions/function_data/iterative_length_function_data.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckpgq/common.hpp"

#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {

unique_ptr<FunctionData> IterativeLengthFunctionData::Copy() const {
  return make_uniq<IterativeLengthFunctionData>(context, csr_id);
}

bool IterativeLengthFunctionData::Equals(const FunctionData &other_p) const {
  auto &other = (const IterativeLengthFunctionData &)other_p;
  return other.csr_id == csr_id;
}

unique_ptr<FunctionData> IterativeLengthFunctionData::IterativeLengthBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }

  int32_t csr_id = ExpressionExecutor::EvaluateScalar(context, *arguments[0])
                       .GetValue<int32_t>();
  auto duckpgq_state = GetDuckPGQState(context);
  duckpgq_state->csr_to_delete.insert(csr_id);


  return make_uniq<IterativeLengthFunctionData>(context, csr_id);
}

} // namespace core

} // namespace duckpgq