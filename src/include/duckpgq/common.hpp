//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {


struct IterativeLengthFunctionData : public FunctionData {
public:
  ClientContext &context;
  int32_t csr_id;

  IterativeLengthFunctionData(ClientContext &context, int32_t csr_id)
      : context(context), csr_id(csr_id) {}
  static unique_ptr<FunctionData>
  IterativeLengthBind(ClientContext &context, ScalarFunction &bound_function,
                      vector<unique_ptr<Expression>> &arguments);

  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
};

struct CheapestPathLengthFunctionData : public FunctionData {
  ClientContext &context;
  int32_t csr_id;

  CheapestPathLengthFunctionData(ClientContext &context, int32_t csr_id)
      : context(context), csr_id(csr_id) {}
  static unique_ptr<FunctionData>
  CheapestPathLengthBind(ClientContext &context, ScalarFunction &bound_function,
                         vector<unique_ptr<Expression>> &arguments);

  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
};

} // namespace duckdb
