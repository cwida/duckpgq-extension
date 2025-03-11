#include "duckpgq/core/functions/function_data/weakly_connected_component_function_data.hpp"

#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {

WeaklyConnectedComponentFunctionData::WeaklyConnectedComponentFunctionData(
    ClientContext &context, int32_t csr_id)
    : context(context), csr_id(csr_id) {
  state_converged = false; // Initialize state
  state_initialized = false;
}

unique_ptr<FunctionData>
WeaklyConnectedComponentFunctionData::WeaklyConnectedComponentBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }

  int32_t csr_id = ExpressionExecutor::EvaluateScalar(context, *arguments[0])
                       .GetValue<int32_t>();
  auto duckpgq_state = GetDuckPGQState(context);
  duckpgq_state->csr_to_delete.insert(csr_id);

  return make_uniq<WeaklyConnectedComponentFunctionData>(context, csr_id);
}

unique_ptr<FunctionData> WeaklyConnectedComponentFunctionData::Copy() const {
  auto result = make_uniq<WeaklyConnectedComponentFunctionData>(context, csr_id);
  return std::move(result);
}

bool WeaklyConnectedComponentFunctionData::Equals(
    const FunctionData &other_p) const {
  auto &other = (const WeaklyConnectedComponentFunctionData &)other_p;
  if (csr_id != other.csr_id) {
    return false;
  }
  if (state_converged != other.state_converged) {
    return false;
  }
  return true;
}

} // namespace core

} // namespace duckpgq
