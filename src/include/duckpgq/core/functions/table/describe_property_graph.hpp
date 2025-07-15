//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/tablefunctions/describe_property_graph.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckpgq/common.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckpgq {

namespace core {

class DescribePropertyGraphFunction : public TableFunction {
public:
	DescribePropertyGraphFunction() {
		name = "describe_property_graph";
		bind = DescribePropertyGraphBind;
		init_global = DescribePropertyGraphInit;
		function = DescribePropertyGraphFunc;
	}

	struct DescribePropertyGraphBindData : public TableFunctionData {
		explicit DescribePropertyGraphBindData(CreatePropertyGraphInfo *pg_info) : describe_pg_info(pg_info) {
		}
		CreatePropertyGraphInfo *describe_pg_info;
	};

	struct DescribePropertyGraphGlobalData : public GlobalTableFunctionState {
		DescribePropertyGraphGlobalData() = default;
		bool done = false;
	};

	static unique_ptr<FunctionData> DescribePropertyGraphBind(ClientContext &context, TableFunctionBindInput &input,
	                                                          vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> DescribePropertyGraphInit(ClientContext &context,
	                                                                      TableFunctionInitInput &input);

	static void DescribePropertyGraphFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
};

} // namespace core

} // namespace duckpgq
