//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/tablefunctions/summarize_property_graph.hpp
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

class SummarizePropertyGraphFunction : public TableFunction {
public:
  SummarizePropertyGraphFunction() {
    name = "summarize_property_graph";
    arguments.push_back(LogicalType::VARCHAR);
    bind_replace = SummarizePropertyGraphBindReplace;
  }

  struct SummarizePropertyGraphBindData : public TableFunctionData {
    explicit SummarizePropertyGraphBindData(CreatePropertyGraphInfo *pg_info)
        : summarize_pg_info(pg_info) {}
    CreatePropertyGraphInfo *summarize_pg_info;
  };

  struct SummarizePropertyGraphGlobalData : public GlobalTableFunctionState {
    SummarizePropertyGraphGlobalData() = default;
    bool done = false;
  };

  static unique_ptr<GlobalTableFunctionState>
  SummarizePropertyGraphInit(ClientContext &context,
                             TableFunctionInitInput &input);

  static unique_ptr<SubqueryRef> CreateGroupBySubquery(shared_ptr<PropertyGraphTable> &pg_table, bool is_in_degree, string degree_column);
  static unique_ptr<ParsedExpression> GetDegreeStatistics(string aggregate_function, bool is_in_degree);
  static unique_ptr<CommonTableExpressionInfo> CreateDegreeStatisticsCTE(shared_ptr<PropertyGraphTable> &pg_table, string degree_column, bool is_in_degree);
  static unique_ptr<ParsedExpression> GetIsolatedNodes(shared_ptr<PropertyGraphTable> &pg_table, string alias, bool is_source);
  static unique_ptr<ParsedExpression> GetDistinctCount(shared_ptr<PropertyGraphTable> &pg_table, string alias, bool is_source);

  static unique_ptr<CommonTableExpressionInfo> CreateVertexTableCTE(shared_ptr<PropertyGraphTable> &vertex_table);
  static unique_ptr<CommonTableExpressionInfo> CreateEdgeTableCTE(shared_ptr<PropertyGraphTable> &edge_table);

  static unique_ptr<TableRef> HandleSingleVertexTable(shared_ptr<PropertyGraphTable> &vertex_table, string stat_table_alias);
  static unique_ptr<TableRef> SummarizePropertyGraphBindReplace(ClientContext &context,
                                        TableFunctionBindInput &input);
};

} // namespace core

} // namespace duckpgq