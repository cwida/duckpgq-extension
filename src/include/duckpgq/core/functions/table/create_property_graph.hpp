//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/tablefunctions/create_property_graph.hpp
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
  CheckPropertyGraphTableLabels(const shared_ptr<PropertyGraphTable> &pg_table,
                                optional_ptr<TableCatalogEntry> &table);

  static void
  CheckPropertyGraphTableColumns(const shared_ptr<PropertyGraphTable> &pg_table,
                                 optional_ptr<TableCatalogEntry> &table);

  static reference<TableCatalogEntry> GetTableCatalogEntry(ClientContext &context,
    shared_ptr<PropertyGraphTable> &pg_table);

  static unique_ptr<FunctionData>
  CreatePropertyGraphBind(ClientContext &context, TableFunctionBindInput &input,
                          vector<LogicalType> &return_types,
                          vector<string> &names);

  static void
  ValidateVertexTableRegistration(shared_ptr<PropertyGraphTable> &pg_table,
                                  const case_insensitive_set_t &v_table_names);

  static void ValidatePrimaryKeyInTable(ClientContext &context,
                                        shared_ptr<PropertyGraphTable> &pg_table,
                                        const vector<string> &pk_columns);

  static void
  ValidateKeys(shared_ptr<PropertyGraphTable> &edge_table,
               const string &reference, const string &key_type,
               vector<string> &pk_columns, vector<string> &fk_columns,
               const vector<unique_ptr<Constraint>> &table_constraints);

  static void
  ValidateForeignKeyColumns(shared_ptr<PropertyGraphTable> &edge_table,
                            const vector<string> &fk_columns,
                            optional_ptr<TableCatalogEntry> &table);

  static unique_ptr<GlobalTableFunctionState>
  CreatePropertyGraphInit(ClientContext &context,
                          TableFunctionInitInput &input);

  static void CreatePropertyGraphFunc(ClientContext &context,
                                      TableFunctionInput &data_p,
                                      DataChunk &output);
};

} // namespace core

} // namespace duckpgq