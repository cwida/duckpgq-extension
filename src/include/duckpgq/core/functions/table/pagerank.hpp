//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/functions/table/pagerank.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckpgq/common.hpp"

namespace duckpgq {
namespace core {

class PageRankFunction : public TableFunction {
public:
	PageRankFunction() {
		name = "pagerank";
		arguments = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
		bind_replace = PageRankBindReplace;
	}

	static unique_ptr<TableRef> PageRankBindReplace(ClientContext &context, TableFunctionBindInput &input);
};

struct PageRankData : TableFunctionData {
	static unique_ptr<FunctionData> PageRankBind(ClientContext &context, TableFunctionBindInput &input,
	                                             vector<LogicalType> &return_types, vector<string> &names) {
		auto result = make_uniq<PageRankData>();
		result->pg_name = StringValue::Get(input.inputs[0]);
		result->node_table = StringValue::Get(input.inputs[1]);
		result->edge_table = StringValue::Get(input.inputs[2]);
		return_types.emplace_back(LogicalType::BIGINT);
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("rowid");
		names.emplace_back("pagerank");
		return std::move(result);
	}

	string pg_name;
	string node_table;
	string edge_table;
};

struct PageRankScanState : GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto result = make_uniq<PageRankScanState>();
		return std::move(result);
	}

	bool finished = false;
};

} // namespace core
} // namespace duckpgq
