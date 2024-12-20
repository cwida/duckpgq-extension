#include "duckpgq/core/functions/function_data/triangle_counting_function_data.hpp"

namespace duckpgq {

namespace core {

TriangleCountingFunctionData::TriangleCountingFunctionData(
    ClientContext &context, int32_t csr_id)
    : context(context), csr_id(csr_id) {

}

unique_ptr<FunctionData>
TriangleCountingFunctionData::TriangleCountingBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }

  int32_t csr_id = ExpressionExecutor::EvaluateScalar(context, *arguments[0])
                       .GetValue<int32_t>();

  return make_uniq<TriangleCountingFunctionData>(context, csr_id);
}

unique_ptr<FunctionData> TriangleCountingFunctionData::Copy() const {
  auto result = make_uniq<TriangleCountingFunctionData>(context, csr_id);
  return std::move(result);
}
bool TriangleCountingFunctionData::Equals(
    const FunctionData &other_p) const {
  auto &other = (const TriangleCountingFunctionData &)other_p;
  if (csr_id != other.csr_id) {
    return false;
  }

  return true;
}

} // namespace core

} // namespace duckpgq