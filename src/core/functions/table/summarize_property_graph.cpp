#include "duckpgq/core/functions/table/summarize_property_graph.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include <duckpgq/core/functions/table.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"

namespace duckpgq {
namespace core {

unique_ptr<ParsedExpression> GetTableNameConstantExpression(PropertyGraphTable &pg_table) {
  auto table_name_column = make_uniq<ConstantExpression>(Value(pg_table.table_name));
  table_name_column->alias = "table_name";
  return table_name_column;
}


unique_ptr<CommonTableExpressionInfo> SummarizePropertyGraphFunction::CreateVertexTableCTE(shared_ptr<PropertyGraphTable> &vertex_table) {
  auto cte_info = make_uniq<CommonTableExpressionInfo>();
  auto select_statement = make_uniq<SelectStatement>();
  auto select_node = make_uniq<SelectNode>();
  select_node->select_list.push_back(GetTableNameConstantExpression(*vertex_table));
  select_node->select_list.push_back(make_uniq<ConstantExpression>(Value())); // source table name (NULL for vertex tables)
  select_node->select_list.push_back(make_uniq<ConstantExpression>(Value())); // destination table name (NULL for vertex tables)

  select_node->from_table = make_uniq<BaseTableRef>(TableDescription(vertex_table->catalog_name, vertex_table->schema_name, vertex_table->table_name));
  select_statement->node = std::move(select_node);
  cte_info->query = std::move(select_statement);
  return cte_info;
}

unique_ptr<TableRef> SummarizePropertyGraphFunction::SummarizePropertyGraphBindReplace(
  ClientContext &context,
  TableFunctionBindInput &bind_input) {
  auto duckpgq_state = GetDuckPGQState(context);

  string property_graph = bind_input.inputs[0].GetValue<string>();
  auto pg_info = duckpgq_state->GetPropertyGraph(property_graph);
  auto select_node = make_uniq<SelectNode>();
  for (auto &vertex_table : pg_info->vertex_tables) {
    select_node->cte_map.map.insert(vertex_table->table_name + "_stats", CreateVertexTableCTE(vertex_table));
    auto base_table_ref = make_uniq<BaseTableRef>();
    base_table_ref->table_name = vertex_table->table_name + "_stats";
    select_node->from_table = std::move(base_table_ref);
  }

  select_node->select_list.push_back(make_uniq<StarExpression>());

  auto select_stmt = make_uniq<SelectStatement>();
  select_stmt->node = std::move(select_node);
  auto subquery = make_uniq<SubqueryRef>(std::move(select_stmt));
  subquery->Print();


  return std::move(subquery);
}



//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterSummarizePropertyGraphTableFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(db, SummarizePropertyGraphFunction());
}

} // namespace core

} // namespace duckpgq
