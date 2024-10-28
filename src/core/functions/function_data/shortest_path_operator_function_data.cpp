#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"
#include <duckpgq/core/utils/compressed_sparse_row.hpp>

namespace duckpgq {

namespace core {

unique_ptr<FunctionData>
ShortestPathOperatorData::ShortestPathOperatorBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
//  string table_to_scan = ExpressionExecutor::EvaluateScalar(context, *arguments[2]).GetValue<string>();
  return make_uniq<ShortestPathOperatorData>(context, "");
}
unique_ptr<FunctionData> ShortestPathOperatorData::Copy() const {
  return make_uniq<ShortestPathOperatorData>(context, table_to_scan);
}

bool ShortestPathOperatorData::Equals(const FunctionData &other_p) const {
  auto &other = (const ShortestPathOperatorData &)other_p;
  return other.table_to_scan == table_to_scan;
}

} // namespace core

} // namespace duckpgq