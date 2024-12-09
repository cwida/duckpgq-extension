#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckpgq {

namespace core {

class LocalClusteringCoefficientFunction : public TableFunction {
public:
  LocalClusteringCoefficientFunction() {
    name = "local_clustering_coefficient";
    arguments = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                 LogicalType::VARCHAR};
    bind_replace = LocalClusteringCoefficientBindReplace;
  }

  static unique_ptr<TableRef>
  LocalClusteringCoefficientBindReplace(ClientContext &context,
                                        TableFunctionBindInput &input);
};

struct LocalClusteringCoefficientData : TableFunctionData {
  static unique_ptr<FunctionData> LocalClusteringCoefficientBind(
      ClientContext &context, TableFunctionBindInput &input,
      vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<LocalClusteringCoefficientData>();
    result->pg_name = StringValue::Get(input.inputs[0]);
    result->node_table = StringValue::Get(input.inputs[1]);
    result->edge_table = StringValue::Get(input.inputs[2]);
    return_types.emplace_back(LogicalType::BIGINT);
    return_types.emplace_back(LogicalType::FLOAT);
    names.emplace_back("rowid");
    names.emplace_back("local_clustering_coefficient");
    return std::move(result);
  }

  string pg_name;
  string node_table;
  string edge_table;
};

struct LocalClusteringCoefficientScanState : GlobalTableFunctionState {
  static unique_ptr<GlobalTableFunctionState>
  Init(ClientContext &context, TableFunctionInitInput &input) {
    auto result = make_uniq<LocalClusteringCoefficientScanState>();
    return std::move(result);
  }

  bool finished = false;
};

} // namespace core

} // namespace duckpgq
