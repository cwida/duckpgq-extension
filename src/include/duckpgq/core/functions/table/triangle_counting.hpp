//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/functions/table/triangle_counting.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckpgq/common.hpp"

namespace duckpgq {
namespace core {

class TriangleCountingFunction : public TableFunction {
public:
  TriangleCountingFunction() {
    name = "triangle_counting";
    arguments = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                 LogicalType::VARCHAR};
    bind_replace = TriangleCountingBindReplace;
  }

  static unique_ptr<TableRef>
  TriangleCountingBindReplace(ClientContext &context,
                                      TableFunctionBindInput &input);
};

struct TriangleCountingData : TableFunctionData {
  static unique_ptr<FunctionData> TriangleCountingBind(
      ClientContext &context, TableFunctionBindInput &input,
      vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<TriangleCountingData>();
    result->pg_name = StringValue::Get(input.inputs[0]);
    result->node_table = StringValue::Get(input.inputs[1]);
    result->edge_table = StringValue::Get(input.inputs[2]);
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("triangleCount");
    return std::move(result);
  }

  string pg_name;
  string node_table;
  string edge_table;
};

struct TriangleCountingScanState : GlobalTableFunctionState {
  static unique_ptr<GlobalTableFunctionState>
  Init(ClientContext &context, TableFunctionInitInput &input) {
    auto result = make_uniq<TriangleCountingScanState>();
    return std::move(result);
  }

  bool finished = false;
};

} // namespace core
} // namespace duckpgq