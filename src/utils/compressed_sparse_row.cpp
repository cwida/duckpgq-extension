#include "duckpgq/utils/compressed_sparse_row.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {
string CSR::ToString() const {
  string result;
  if (initialized_v) {
    result += "v: ";
    for (size_t i = 0; i < vsize; i++) {
      result += std::to_string(v[i].load()) + " ";
    }
  }
  result += "\n";
  if (initialized_e) {
    result += "e: ";
    for (size_t i = 0; i < e.size(); i++) {
      result += std::to_string(e[i]) + " ";
    }
  }
  result += "\n";
  if (initialized_w) {
    result += "w: ";
    for (size_t i = 0; i < w.size(); i++) {
      result += std::to_string(w[i]) + " ";
    }
  }
  return result;
}

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
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }

  Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
  if (arguments.size() == 4) {
    auto logical_type = LogicalType::SQLNULL;
    return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                      logical_type);
  }
  return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                      arguments[3]->return_type);

}

unique_ptr<FunctionData>
CSRFunctionData::CSREdgeBind(ClientContext &context,
                             ScalarFunction &bound_function,
                             vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }
  Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
  if (arguments.size() == 7) {
    return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                      arguments[6]->return_type);
  } else {
    auto logical_type = LogicalType::SQLNULL;
    return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                      logical_type);
  }
}

unique_ptr<FunctionData>
CSRFunctionData::CSRBind(ClientContext &context, ScalarFunction &bound_function,
                         vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }
  Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
  return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                    LogicalType::BOOLEAN);
}
} // namespace duckdb