#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"
#include <duckpgq/core/utils/compressed_sparse_row.hpp>

namespace duckpgq {

namespace core {

unique_ptr<FunctionData>
ShortestPathOperatorData::ShortestPathOperatorBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
  return make_uniq<ShortestPathOperatorData>(context);
}
unique_ptr<FunctionData> ShortestPathOperatorData::Copy() const {
  return make_uniq<ShortestPathOperatorData>(context);
}

bool ShortestPathOperatorData::Equals(const FunctionData &other_p) const {
  return true;
}

} // namespace core

} // namespace duckpgq