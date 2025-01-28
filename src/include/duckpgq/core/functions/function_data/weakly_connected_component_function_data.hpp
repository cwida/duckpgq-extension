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
  std::mutex wcc_lock;
  std::mutex initialize_lock;
  bool state_converged;
  bool state_initialized;
  vector<int64_t> forest;


  WeaklyConnectedComponentFunctionData(ClientContext &context, int32_t csr_id);

  static unique_ptr<FunctionData>
  WeaklyConnectedComponentBind(ClientContext &context,
                               ScalarFunction &bound_function,
                               vector<unique_ptr<Expression>> &arguments);

  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
};

} // namespace core

} // namespace duckpgq
