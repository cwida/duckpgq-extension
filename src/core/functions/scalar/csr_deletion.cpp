#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include <duckpgq/core/functions/scalar.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <duckpgq_extension.hpp>

namespace duckpgq {

namespace core {

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

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterCSRDeletionScalarFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(
      db,
      ScalarFunction("delete_csr", {LogicalType::INTEGER}, LogicalType::BOOLEAN,
                     DeleteCsrFunction, CSRFunctionData::CSRBind));
}

} // namespace core

} // namespace duckpgq
