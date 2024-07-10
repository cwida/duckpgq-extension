//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/function_data/iterative_length_function_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/main/client_context.hpp"

namespace duckdb {
struct IterativeLengthFunctionData final : FunctionData {
  ClientContext &context;
  string table_to_scan;
  int32_t csr_id;

  IterativeLengthFunctionData(ClientContext &context, string table_to_scan, int32_t csr_id)
      : context(context), table_to_scan(table_to_scan), csr_id(csr_id) {}
  static unique_ptr<FunctionData>
  IterativeLengthBind(ClientContext &context, ScalarFunction &bound_function,
                      vector<unique_ptr<Expression>> &arguments);

  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
};

}