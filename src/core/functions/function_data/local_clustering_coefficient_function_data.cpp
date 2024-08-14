#include "duckpgq/core/functions/function_data/local_clustering_coefficient_function_data.hpp"
#include <duckpgq/core/utils/compressed_sparse_row.hpp>

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
  auto csr_id = GetCSRId(arguments[0], context);

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