#include <duckpgq_extension.hpp>
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/duckpgq_functions.hpp"

namespace duckdb {

enum class CSRWType : int32_t {
  // possible weight types of a csr
  UNWEIGHTED,   //! unweighted
  INTWEIGHT,    //! integer
  DOUBLEWEIGHT, //! double
};

static void GetCsrWTypeFunction(DataChunk &args, ExpressionState &state,
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
  result.SetVectorType(VectorType::CONSTANT_VECTOR);
  auto result_data = ConstantVector::GetData<int32_t>(result);
  auto csr = duckpgq_state->GetCSR(info.id);
  int32_t flag;
  if (!csr->initialized_w) {
    flag = (int32_t)CSRWType::UNWEIGHTED;
  } else if (csr->w.size()) {
    flag = (int32_t)CSRWType::INTWEIGHT;
  } else if (csr->w_double.size()) {
    flag = (int32_t)CSRWType::DOUBLEWEIGHT;
  } else {
    throw InternalException("Corrupted weight vector");
  }
  result_data[0] = flag;
}

CreateScalarFunctionInfo DuckPGQFunctions::GetGetCsrWTypeFunction() {
  ScalarFunctionSet set("csr_get_w_type");

  set.AddFunction(ScalarFunction("csr_get_w_type", {LogicalType::INTEGER},
                                 LogicalType::INTEGER, GetCsrWTypeFunction,
                                 CSRFunctionData::CSRBind));

  return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
