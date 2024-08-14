#pragma once

#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

struct CSRFunctionData : FunctionData {
  CSRFunctionData(ClientContext &context, int32_t id, LogicalType weight_type);
  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
  static unique_ptr<FunctionData>
  CSRVertexBind(ClientContext &context, ScalarFunction &bound_function,
                vector<unique_ptr<Expression>> &arguments);
  static unique_ptr<FunctionData>
  CSREdgeBind(ClientContext &context, ScalarFunction &bound_function,
              vector<unique_ptr<Expression>> &arguments);
  static unique_ptr<FunctionData>
  CSRBind(ClientContext &context, ScalarFunction &bound_function,
          vector<unique_ptr<Expression>> &arguments);

  ClientContext &context;
  const int32_t id;
  const LogicalType weight_type;
};

} // namespace core

} // namespace duckpgq