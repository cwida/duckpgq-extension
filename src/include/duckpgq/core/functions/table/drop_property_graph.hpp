//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/tablefunctions/drop_property_graph.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckpgq/common.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"

#include <duckdb/parser/parsed_data/drop_property_graph_info.hpp>

namespace duckpgq {

namespace core {


class DropPropertyGraphFunction : public TableFunction {
public:
  DropPropertyGraphFunction() {
    name = "drop_property_graph";
    bind = DropPropertyGraphBind;
    init_global = DropPropertyGraphInit;
    function = DropPropertyGraphFunc;
  }

  struct DropPropertyGraphBindData : TableFunctionData {
    explicit DropPropertyGraphBindData(DropPropertyGraphInfo *pg_info)
        : drop_pg_info(pg_info) {}

    DropPropertyGraphInfo *drop_pg_info;
  };

  struct DropPropertyGraphGlobalData : GlobalTableFunctionState {
    DropPropertyGraphGlobalData() = default;
  };

  static unique_ptr<FunctionData>
  DropPropertyGraphBind(ClientContext &context, TableFunctionBindInput &input,
                        vector<LogicalType> &return_types,
                        vector<string> &names);

  static unique_ptr<GlobalTableFunctionState>
  DropPropertyGraphInit(ClientContext &context, TableFunctionInitInput &input);

  static void DropPropertyGraphFunc(ClientContext &context,
                                    TableFunctionInput &data_p,
                                    DataChunk &output);
};

} // namespace core

} // namespace duckpgq
