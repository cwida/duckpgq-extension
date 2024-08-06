#include "duckpgq/core/functions/function_data/local_clustering_coefficient_function_data.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckpgq {

namespace core {

LocalClusteringCoefficientFunctionData::LocalClusteringCoefficientFunctionData(
    ClientContext &context, int32_t csr_id)
    : context(context), csr_id(csr_id) {

}

unique_ptr<FunctionData>
LocalClusteringCoefficientFunctionData::LocalClusteringCoefficientBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }

  int32_t csr_id = ExpressionExecutor::EvaluateScalar(context, *arguments[0])
                       .GetValue<int32_t>();

  return make_uniq<LocalClusteringCoefficientFunctionData>(context, csr_id);
}

unique_ptr<FunctionData> LocalClusteringCoefficientFunctionData::Copy() const {
  return make_uniq<LocalClusteringCoefficientFunctionData>(context, csr_id);
}

bool LocalClusteringCoefficientFunctionData::Equals(const FunctionData &other_p) const {
  auto &other = (const LocalClusteringCoefficientFunctionData &)other_p;
  return other.csr_id == csr_id;
}

} // namespace core

} // namespace duckpgq