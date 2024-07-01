#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/duckpgq_functions.hpp"
#include <duckpgq/utils/duckpgq_utils.hpp>
#include <duckpgq_extension.hpp>

namespace duckdb {

static void DeleteCsrFunction(DataChunk &args, ExpressionState &state,
                              Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (CSRFunctionData &)*func_expr.bind_info;

  auto duckpgq_state = GetDuckPGQState(info.context);


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
