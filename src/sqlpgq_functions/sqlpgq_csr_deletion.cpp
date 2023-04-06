#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "sqlpgq_common.hpp"
#include "sqlpgq_functions.hpp"

#include <chrono>
#include <math.h>
#include <mutex>

namespace duckdb {

static void DeleteCsrFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (CSRFunctionData &)*func_expr.bind_info;

	auto sqlpgq_state_entry = info.context.registered_state.find("sqlpgq");
	if (sqlpgq_state_entry == info.context.registered_state.end()) {
		//! Wondering how you can get here if the extension wasn't loaded, but leaving this check in anyways
		throw MissingExtensionException("The SQL/PGQ extension has not been loaded");
	}
	auto sqlpgq_state = reinterpret_cast<SQLPGQContext *>(sqlpgq_state_entry->second.get());

	int flag = sqlpgq_state->csr_list.erase(info.id);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<bool>(result);
	result_data[0] = (flag == 1);
}

CreateScalarFunctionInfo SQLPGQFunctions::GetDeleteCsrFunction() {
	ScalarFunctionSet set("delete_csr");

	set.AddFunction(ScalarFunction("delete_csr", {LogicalType::INTEGER}, LogicalType::BOOLEAN, DeleteCsrFunction,
	                               CSRFunctionData::CSRBind));

	return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
