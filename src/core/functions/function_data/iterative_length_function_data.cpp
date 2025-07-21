#include "duckpgq/core/functions/function_data/iterative_length_function_data.hpp"
#include "duckpgq/common.hpp"

#include <duckpgq/core/utils/compressed_sparse_row.hpp>

namespace duckpgq {

namespace core {

unique_ptr<FunctionData> IterativeLengthFunctionData::Copy() const {
  return make_uniq<IterativeLengthFunctionData>(context, table_to_scan, csr_id);
}

bool IterativeLengthFunctionData::Equals(const FunctionData &other_p) const {
  auto &other = (const IterativeLengthFunctionData &)other_p;
  return other.csr_id == csr_id;
}


unique_ptr<FunctionData> IterativeLengthFunctionData::IterativeLengthBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
  if (arguments.size() == 3) {
    string table_to_scan = ExpressionExecutor::EvaluateScalar(context, *arguments[2]).GetValue<string>();
    return make_uniq<IterativeLengthFunctionData>(context, table_to_scan, 0);
  }
 auto csr_id = GetCSRId(arguments[0], context);

  return make_uniq<IterativeLengthFunctionData>(context, "", csr_id);
}

} // namespace core

} // namespace duckpgq