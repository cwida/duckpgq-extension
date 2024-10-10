#include "duckpgq/core/functions/function_data/csr_function_data.hpp"
#include <duckpgq/core/utils/compressed_sparse_row.hpp>

namespace duckpgq {

namespace core {

CSRFunctionData::CSRFunctionData(ClientContext &context, int32_t id,
                                 LogicalType weight_type)
    : context(context), id(id), weight_type(std::move(weight_type)) {}

unique_ptr<FunctionData> CSRFunctionData::Copy() const {
  return make_uniq<CSRFunctionData>(context, id, weight_type);
}

bool CSRFunctionData::Equals(const FunctionData &other_p) const {
  auto &other = (const CSRFunctionData &)other_p;
  return id == other.id && weight_type == other.weight_type;
}


unique_ptr<FunctionData>
CSRFunctionData::CSRVertexBind(ClientContext &context,
                               ScalarFunction &bound_function,
                               vector<unique_ptr<Expression>> &arguments) {
  auto csr_id = GetCSRId(arguments[0], context);
  if (arguments.size() == 4) {
    auto logical_type = LogicalType::SQLNULL;
    return make_uniq<CSRFunctionData>(context, csr_id, logical_type);
  }
  return make_uniq<CSRFunctionData>(context, csr_id, arguments[3]->return_type);
}

unique_ptr<FunctionData>
CSRFunctionData::CSREdgeBind(ClientContext &context,
                             ScalarFunction &bound_function,
                             vector<unique_ptr<Expression>> &arguments) {
  auto csr_id = GetCSRId(arguments[0], context);
  if (arguments.size() == 8) {
    return make_uniq<CSRFunctionData>(context, csr_id,
                                      arguments[7]->return_type);
  }
  auto logical_type = LogicalType::SQLNULL;
  return make_uniq<CSRFunctionData>(context, csr_id, logical_type);
}

unique_ptr<FunctionData>
CSRFunctionData::CSRBind(ClientContext &context, ScalarFunction &bound_function,
                         vector<unique_ptr<Expression>> &arguments) {
  auto csr_id = GetCSRId(arguments[0], context);
  return make_uniq<CSRFunctionData>(context, csr_id, LogicalType::BOOLEAN);
}

} // namespace core

} // namespace duckpgq
