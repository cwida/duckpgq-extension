//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/tablefunctions/create_property_graph.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
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

  struct CreatePropertyGraphBindData : public TableFunctionData {
    explicit CreatePropertyGraphBindData(CreatePropertyGraphInfo *pg_info)
        : create_pg_info(pg_info) {}

    CreatePropertyGraphInfo *create_pg_info;
  };

  struct CreatePropertyGraphGlobalData : public GlobalTableFunctionState {
    CreatePropertyGraphGlobalData() = default;
  };

  static void
  CheckPropertyGraphTableLabels(shared_ptr<PropertyGraphTable> &pg_table,
                                TableCatalogEntry &table);

  static void
  CheckPropertyGraphTableColumns(shared_ptr<PropertyGraphTable> &pg_table,
                                 TableCatalogEntry &table);

  static duckdb::unique_ptr<FunctionData>
  CreatePropertyGraphBind(ClientContext &context, TableFunctionBindInput &input,
                          vector<LogicalType> &return_types,
                          vector<string> &names);

  static duckdb::unique_ptr<GlobalTableFunctionState>
  CreatePropertyGraphInit(ClientContext &context,
                          TableFunctionInitInput &input);

  static void CreatePropertyGraphFunc(ClientContext &context,
                                      TableFunctionInput &data_p,
                                      DataChunk &output);
};

} // namespace duckdb