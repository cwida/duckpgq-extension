//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/main/client_context.hpp"
#include "duckpgq/common.hpp"


namespace duckpgq {

namespace core {

struct ShortestPathOperatorData final : FunctionData {
  ClientContext &context;
  string table_to_scan;
  int32_t csr_id;

  ShortestPathOperatorData(ClientContext &context, string table_to_scan, int32_t csr_id)
      : context(context), table_to_scan(std::move(table_to_scan)), csr_id(csr_id) {}
  static unique_ptr<FunctionData>
  ShortestPathOperatorBind(ClientContext &context, ScalarFunction &bound_function,
                      vector<unique_ptr<Expression>> &arguments);

  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
};

} // namespace core

} // namespace duckpgq