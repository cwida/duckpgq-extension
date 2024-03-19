//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/tablefunctions/create_property_graph.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class CreatePropertyGraphFunction : public TableFunction {
public:
  CreatePropertyGraphFunction() {
    name = "create_property_graph";
    bind = CreatePropertyGraphBind;
    init_global = CreatePropertyGraphInit;
    function = CreatePropertyGraphFunc;
  }

  struct CreatePropertyGraphBindData : TableFunctionData {
    explicit CreatePropertyGraphBindData(CreatePropertyGraphInfo *pg_info)
        : create_pg_info(pg_info) {}

    CreatePropertyGraphInfo *create_pg_info;
  };

  struct CreatePropertyGraphGlobalData : GlobalTableFunctionState {
    CreatePropertyGraphGlobalData() = default;
  };

  static void
  CheckPropertyGraphTableLabels(const shared_ptr<PropertyGraphTable> &pg_table,
                                TableCatalogEntry &table);

  static void
  CheckPropertyGraphTableColumns(const shared_ptr<PropertyGraphTable> &pg_table,
                                 TableCatalogEntry &table);

  static unique_ptr<FunctionData>
  CreatePropertyGraphBind(ClientContext &context, TableFunctionBindInput &input,
                          vector<LogicalType> &return_types,
                          vector<string> &names);

  static unique_ptr<GlobalTableFunctionState>
  CreatePropertyGraphInit(ClientContext &context,
                          TableFunctionInitInput &input);

  static void CreatePropertyGraphFunc(ClientContext &context,
                                      TableFunctionInput &data_p,
                                      DataChunk &output);
};

} // namespace duckdb