#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/duckpgq_functions.hpp"
#include <duckpgq_extension.hpp>

namespace duckdb {

static void DeleteCsrFunction(DataChunk &args, ExpressionState &state,
                              Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (CSRFunctionData &)*func_expr.bind_info;

  auto duckpgq_state_entry = info.context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == info.context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());

  int flag = duckpgq_state->csr_list.erase(info.id);
  result.SetVectorType(VectorType::CONSTANT_VECTOR);
  auto result_data = ConstantVector::GetData<bool>(result);
  result_data[0] = (flag == 1);
}

CreateScalarFunctionInfo DuckPGQFunctions::GetDeleteCsrFunction() {
  ScalarFunctionSet set("delete_csr");

  set.AddFunction(ScalarFunction("delete_csr", {LogicalType::INTEGER},
                                 LogicalType::BOOLEAN, DeleteCsrFunction,
                                 CSRFunctionData::CSRBind));

  return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
