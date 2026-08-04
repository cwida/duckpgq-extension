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

namespace duckdb {

struct ShortestPathOperatorData final : FunctionData {
	ClientContext &context;

	ShortestPathOperatorData(ClientContext &context) : context(context) {
	}
	static unique_ptr<FunctionData> ShortestPathOperatorBind(BindScalarFunctionInput &input);

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

} // namespace duckdb
