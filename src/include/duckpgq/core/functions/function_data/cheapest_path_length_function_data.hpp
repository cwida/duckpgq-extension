//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/functions/function_data/cheapest_path_length_function_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckpgq/common.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {



struct CheapestPathLengthFunctionData final : FunctionData {
	ClientContext &context;
	int32_t csr_id;

	CheapestPathLengthFunctionData(ClientContext &context, int32_t csr_id) : context(context), csr_id(csr_id) {
	}
	static unique_ptr<FunctionData> CheapestPathLengthBind(ClientContext &context, ScalarFunction &bound_function,
	                                                       vector<unique_ptr<Expression>> &arguments);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};


} // namespace duckdb
