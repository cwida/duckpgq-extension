//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/functions/function_data/weakly_connected_component_function_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/main/client_context.hpp"
#include "duckpgq/common.hpp"

namespace duckpgq {
namespace core {
struct WeaklyConnectedComponentFunctionData final : FunctionData {
  ClientContext &context;
  int32_t csr_id;
  vector<int64_t> componentId;
  std::mutex component_lock;
  bool component_id_initialized; // if componentId is initialized
  WeaklyConnectedComponentFunctionData(ClientContext &context, int32_t csr_id);
  WeaklyConnectedComponentFunctionData(ClientContext &context, int32_t csr_id,
                                       const vector<int64_t> &componentId);
  static unique_ptr<FunctionData>
  WeaklyConnectedComponentBind(ClientContext &context,
                               ScalarFunction &bound_function,
                               vector<unique_ptr<Expression>> &arguments);

  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
};

} // namespace core

} // namespace duckpgq