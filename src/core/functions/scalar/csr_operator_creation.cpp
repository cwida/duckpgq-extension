#include "duckpgq/common.hpp"
#include <duckpgq/core/functions/function_data/csr_operator_function_data.hpp>
#include <duckpgq/core/functions/scalar.hpp>
#include "duckdb/function/scalar_function.hpp"

namespace duckpgq {

namespace core {

static void CreateCSROperatorFunction(DataChunk &args, ExpressionState &state,
                                    Vector &result) {
  throw NotImplementedException("CSR operator creation function not implemented, should have gone to operator instead.");
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterCSROperatorCreationScalarFunctions(DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(db, ScalarFunction("csr_operator", {LogicalType::INTEGER, LogicalType::BIGINT,
                                  LogicalType::BIGINT, LogicalType::BIGINT,
                                  LogicalType::BIGINT, LogicalType::BIGINT,
                                  LogicalType::BIGINT},
                                  LogicalType::INTEGER, CreateCSROperatorFunction,
                                 CSROperatorFunctionData::CSRBind));
}

} // namespace core

} // namespace duckpgq