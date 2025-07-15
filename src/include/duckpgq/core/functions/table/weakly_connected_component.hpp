//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/functions/table/weakly_connected_component.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckpgq/common.hpp"

namespace duckpgq {
namespace core {

class WeaklyConnectedComponentFunction : public TableFunction {
public:
	WeaklyConnectedComponentFunction() {
		name = "weakly_connected_component";
		arguments = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
		bind_replace = WeaklyConnectedComponentBindReplace;
	}

	static unique_ptr<TableRef> WeaklyConnectedComponentBindReplace(ClientContext &context,
	                                                                TableFunctionBindInput &input);
};

struct WeaklyConnectedComponentData : TableFunctionData {
	static unique_ptr<FunctionData> WeaklyConnectedComponentBind(ClientContext &context, TableFunctionBindInput &input,
	                                                             vector<LogicalType> &return_types,
	                                                             vector<string> &names) {
		auto result = make_uniq<WeaklyConnectedComponentData>();
		result->pg_name = StringValue::Get(input.inputs[0]);
		result->node_table = StringValue::Get(input.inputs[1]);
		result->edge_table = StringValue::Get(input.inputs[2]);
		return_types.emplace_back(LogicalType::BIGINT);
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("rowid");
		names.emplace_back("componentId");
		return std::move(result);
	}

	string pg_name;
	string node_table;
	string edge_table;
};

struct WeaklyConnectedComponentScanState : GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto result = make_uniq<WeaklyConnectedComponentScanState>();
		return std::move(result);
	}

	bool finished = false;
};

} // namespace core
} // namespace duckpgq
