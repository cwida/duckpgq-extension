#include "duckpgq/core/functions/table/summarize_property_graph.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include <duckpgq/core/functions/table.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckdb {

unique_ptr<ParsedExpression> GetTableNameConstantExpression(const string &table_name, const string &alias) {
	auto table_name_column = make_uniq<ConstantExpression>(Value(table_name));
	table_name_column->alias = alias;
	return table_name_column;
}

unique_ptr<ParsedExpression> GetFunctionExpression(const string &aggregate_function, const string &alias,
                                                   bool is_in_degree, const Value &value = nullptr) {
	vector<unique_ptr<ParsedExpression>> max_children;
	max_children.push_back(make_uniq<ColumnRefExpression>(is_in_degree ? "in_degree" : "out_degree"));
	if (!value.IsNull()) {
		max_children.push_back(make_uniq<ConstantExpression>(value));
	}
	auto agg_function = make_uniq<FunctionExpression>(aggregate_function, std::move(max_children));
	agg_function->alias = alias + (is_in_degree ? "_in_degree" : "_out_degree");
	return agg_function;
}

unique_ptr<ParsedExpression> GetConstantNullExpressionWithAlias(const string &alias) {
	auto result = make_uniq<ConstantExpression>(Value());
	result->alias = alias;
	return result;
}

unique_ptr<ParsedExpression> IsVertexTableConstantExpression(bool is_vertex_table, const string &alias) {
	auto result = make_uniq<ConstantExpression>(Value(is_vertex_table));
	result->alias = alias;
	return result;
}
unique_ptr<ParsedExpression> GetTableCount(const string &alias) {
	vector<unique_ptr<ParsedExpression>> children;
	auto aggregate_function = make_uniq<FunctionExpression>("count_star", std::move(children));
	aggregate_function->alias = alias;
	return std::move(aggregate_function);
}

unique_ptr<ParsedExpression>
SummarizePropertyGraphFunction::GetDistinctCount(const shared_ptr<PropertyGraphTable> &pg_table, const string &alias,
                                                 bool is_source) {
	auto result = make_uniq<SubqueryExpression>();
	result->subquery_type = SubqueryType::SCALAR;
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_node->from_table =
	    make_uniq<BaseTableRef>(TableDescription(pg_table->catalog_name, pg_table->schema_name, pg_table->table_name));
	vector<unique_ptr<ParsedExpression>> count_children;
	auto column_to_count = is_source ? pg_table->source_fk[0] : pg_table->destination_fk[0];
	count_children.push_back(make_uniq<ColumnRefExpression>(column_to_count, pg_table->table_name));
	auto count_expression = make_uniq<FunctionExpression>("count", std::move(count_children));
	count_expression->distinct = true;
	select_node->select_list.push_back(std::move(count_expression));
	select_statement->node = std::move(select_node);
	result->subquery = std::move(select_statement);
	result->alias = alias;
	return result;
}

unique_ptr<ParsedExpression> SummarizePropertyGraphFunction::GetIsolatedNodes(shared_ptr<PropertyGraphTable> &pg_table,
                                                                              const string &alias, bool is_source) {
	auto result = make_uniq<SubqueryExpression>();
	result->subquery_type = SubqueryType::SCALAR;
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	vector<unique_ptr<ParsedExpression>> count_children;
	string table_reference = is_source ? pg_table->source_reference : pg_table->destination_reference;
	string table_schema = is_source ? pg_table->source_schema : pg_table->destination_schema;
	string table_catalog = is_source ? pg_table->source_catalog : pg_table->destination_catalog;
	string pk_reference = is_source ? pg_table->source_pk[0] : pg_table->destination_pk[0];
	string fk_reference = is_source ? pg_table->source_fk[0] : pg_table->destination_fk[0];
	count_children.push_back(make_uniq<ColumnRefExpression>(pk_reference, table_reference));
	select_node->select_list.push_back(make_uniq<FunctionExpression>("count", std::move(count_children)));

	auto join_ref = make_uniq<JoinRef>();
	join_ref->type = JoinType::LEFT;
	auto source_table_ref = make_uniq<BaseTableRef>(TableDescription(table_catalog, table_schema, table_reference));
	auto edge_table_ref =
	    make_uniq<BaseTableRef>(TableDescription(pg_table->catalog_name, pg_table->schema_name, pg_table->table_name));
	join_ref->left = std::move(source_table_ref);
	join_ref->right = std::move(edge_table_ref);

	join_ref->condition = make_uniq<ComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>(pk_reference, table_reference),
	    make_uniq<ColumnRefExpression>(fk_reference, pg_table->table_name));

	select_node->from_table = std::move(join_ref);

	vector<unique_ptr<ParsedExpression>> operator_children;
	operator_children.push_back(make_uniq<ColumnRefExpression>(fk_reference, pg_table->table_name));
	auto operator_expression =
	    make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, std::move(operator_children));
	select_node->where_clause = std::move(operator_expression);

	select_statement->node = std::move(select_node);
	result->subquery = std::move(select_statement);
	result->alias = alias;
	return result;
}

unique_ptr<SubqueryRef>
SummarizePropertyGraphFunction::CreateGroupBySubquery(const shared_ptr<PropertyGraphTable> &pg_table, bool is_in_degree,
                                                      const string &degree_column) {
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<ColumnRefExpression>(degree_column));
	vector<unique_ptr<ParsedExpression>> children;
	auto count_star_expression = make_uniq<FunctionExpression>("count_star", std::move(children));
	count_star_expression->alias = is_in_degree ? "in_degree" : "out_degree";
	select_node->select_list.push_back(std::move(count_star_expression));
	select_node->from_table =
	    make_uniq<BaseTableRef>(TableDescription(pg_table->catalog_name, pg_table->schema_name, pg_table->table_name));
	GroupByNode grouping_node;
	grouping_node.group_expressions.push_back(make_uniq<ColumnRefExpression>(degree_column));
	grouping_node.grouping_sets.push_back({0});
	select_node->groups = std::move(grouping_node);
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	return make_uniq<SubqueryRef>(std::move(select_statement));
}

unique_ptr<CommonTableExpressionInfo>
SummarizePropertyGraphFunction::CreateDegreeStatisticsCTE(const shared_ptr<PropertyGraphTable> &pg_table,
                                                          const string &degree_column, bool is_in_degree) {
	auto cte_info = make_uniq<CommonTableExpressionInfo>();
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(GetFunctionExpression("avg", "avg", is_in_degree));
	select_node->select_list.push_back(GetFunctionExpression("min", "min", is_in_degree));
	select_node->select_list.push_back(GetFunctionExpression("max", "max", is_in_degree));
	select_node->select_list.push_back(
	    GetFunctionExpression("approx_quantile", "q25", is_in_degree, Value::FLOAT(0.25)));
	select_node->select_list.push_back(
	    GetFunctionExpression("approx_quantile", "q50", is_in_degree, Value::FLOAT(0.5)));
	select_node->select_list.push_back(
	    GetFunctionExpression("approx_quantile", "q75", is_in_degree, Value::FLOAT(0.75)));
	select_node->from_table = CreateGroupBySubquery(pg_table, is_in_degree, degree_column);
	select_statement->node = std::move(select_node);
	cte_info->query = std::move(select_statement);
	// todo(dtenwolde) make this CTE materialized
	// cte_info->materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
	return cte_info;
}

unique_ptr<ParsedExpression> SummarizePropertyGraphFunction::GetDegreeStatistics(const string &aggregate_function,
                                                                                 bool is_in_degree) {
	auto result = make_uniq<SubqueryExpression>();
	result->subquery_type = SubqueryType::SCALAR;
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(
	    make_uniq<ColumnRefExpression>(aggregate_function + "_" + (is_in_degree ? "in_degree" : "out_degree")));
	auto cte_table_ref = make_uniq<BaseTableRef>();
	cte_table_ref->table_name = is_in_degree ? "in_degrees" : "out_degrees";
	select_node->from_table = std::move(cte_table_ref);
	select_statement->node = std::move(select_node);
	result->subquery = std::move(select_statement);
	result->alias = aggregate_function + "_" + (is_in_degree ? "in_degree" : "out_degree");
	return result;
}

unique_ptr<CommonTableExpressionInfo>
SummarizePropertyGraphFunction::CreateVertexTableCTE(const shared_ptr<PropertyGraphTable> &vertex_table) {
	auto cte_info = make_uniq<CommonTableExpressionInfo>();
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(GetTableNameConstantExpression(vertex_table->table_name, "table_name"));
	select_node->select_list.push_back(IsVertexTableConstantExpression(true, "is_vertex_table"));
	select_node->select_list.push_back(
	    GetConstantNullExpressionWithAlias("source_table")); // source table name (NULL for vertex tables)
	select_node->select_list.push_back(
	    GetConstantNullExpressionWithAlias("destination_table")); // destination table name (NULL for vertex tables)
	select_node->select_list.push_back(GetTableCount("vertex_count"));
	select_node->select_list.push_back(
	    GetConstantNullExpressionWithAlias("edge_count")); // edge_count (NULL for vertex tables)
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("unique_source_count"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("unique_destination_count"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("isolated_sources"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("isolated_destinations"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("avg_in_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("min_in_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("max_in_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("q25_in_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("q50_in_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("q75_in_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("avg_out_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("min_out_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("max_out_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("q25_out_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("q50_out_degree"));
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("q75_out_degree"));
	select_node->from_table = make_uniq<BaseTableRef>(
	    TableDescription(vertex_table->catalog_name, vertex_table->schema_name, vertex_table->table_name));
	select_statement->node = std::move(select_node);
	cte_info->query = std::move(select_statement);
	return cte_info;
}

unique_ptr<CommonTableExpressionInfo>
SummarizePropertyGraphFunction::CreateEdgeTableCTE(shared_ptr<PropertyGraphTable> &edge_table) {
	auto cte_info = make_uniq<CommonTableExpressionInfo>();
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_node->cte_map.map.insert("in_degrees",
	                                CreateDegreeStatisticsCTE(edge_table, edge_table->destination_fk[0], true));
	select_node->cte_map.map.insert("out_degrees",
	                                CreateDegreeStatisticsCTE(edge_table, edge_table->source_fk[0], false));

	select_node->select_list.push_back(GetTableNameConstantExpression(edge_table->table_name, "table_name"));
	select_node->select_list.push_back(IsVertexTableConstantExpression(false, "is_vertex_table"));
	select_node->select_list.push_back(
	    GetTableNameConstantExpression(edge_table->source_reference,
	                                   "source_table")); // source table name (NULL for vertex tables)
	select_node->select_list.push_back(
	    GetTableNameConstantExpression(edge_table->destination_reference,
	                                   "destination_table")); // destination table name (NULL for vertex tables)
	select_node->select_list.push_back(GetConstantNullExpressionWithAlias("vertex_count"));
	select_node->select_list.push_back(GetTableCount("edge_count"));

	select_node->select_list.push_back(GetDistinctCount(edge_table, "unique_source_count", true));
	select_node->select_list.push_back(GetDistinctCount(edge_table, "unique_destination_count", false));

	select_node->select_list.push_back(GetIsolatedNodes(edge_table, "isolated_sources", true));
	select_node->select_list.push_back(GetIsolatedNodes(edge_table, "isolated_destinations", false));

	select_node->select_list.push_back(GetDegreeStatistics("avg", true));
	select_node->select_list.push_back(GetDegreeStatistics("min", true));
	select_node->select_list.push_back(GetDegreeStatistics("max", true));
	select_node->select_list.push_back(GetDegreeStatistics("q25", true));
	select_node->select_list.push_back(GetDegreeStatistics("q50", true));
	select_node->select_list.push_back(GetDegreeStatistics("q75", true));

	select_node->select_list.push_back(GetDegreeStatistics("avg", false));
	select_node->select_list.push_back(GetDegreeStatistics("min", false));
	select_node->select_list.push_back(GetDegreeStatistics("max", false));
	select_node->select_list.push_back(GetDegreeStatistics("q25", false));
	select_node->select_list.push_back(GetDegreeStatistics("q50", false));
	select_node->select_list.push_back(GetDegreeStatistics("q75", false));

	select_node->from_table = make_uniq<BaseTableRef>(
	    TableDescription(edge_table->catalog_name, edge_table->schema_name, edge_table->table_name));
	select_statement->node = std::move(select_node);
	cte_info->query = std::move(select_statement);
	return cte_info;
}

unique_ptr<TableRef>
SummarizePropertyGraphFunction::HandleSingleVertexTable(const shared_ptr<PropertyGraphTable> &vertex_table,
                                                        const string &stat_table_alias) {
	auto select_node = make_uniq<SelectNode>();
	select_node->cte_map.map.insert(stat_table_alias, CreateVertexTableCTE(vertex_table));
	auto base_table_ref = make_uniq<BaseTableRef>();
	base_table_ref->table_name = stat_table_alias;
	select_node->from_table = std::move(base_table_ref);
	select_node->select_list.push_back(make_uniq<StarExpression>());
	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(select_node);
	auto subquery = make_uniq<SubqueryRef>(std::move(select_stmt));
	return std::move(subquery);
}

void AddToUnionNode(unique_ptr<SetOperationNode> &final_union_node, unique_ptr<SelectNode> &inner_select_node) {
	final_union_node->children.push_back(std::move(inner_select_node));
}

unique_ptr<SelectNode> CreateInnerSelectStatNode(const string &stat_table_alias) {
	auto inner_select_node = make_uniq<SelectNode>();
	inner_select_node->select_list.push_back(make_uniq<StarExpression>());
	auto base_table_ref = make_uniq<BaseTableRef>();
	base_table_ref->table_name = stat_table_alias;
	inner_select_node->from_table = std::move(base_table_ref);
	return inner_select_node;
}

unique_ptr<TableRef>
SummarizePropertyGraphFunction::SummarizePropertyGraphBindReplace(ClientContext &context,
                                                                  TableFunctionBindInput &bind_input) {
	auto duckpgq_state = GetDuckPGQState(context);

	string property_graph = bind_input.inputs[0].GetValue<string>();
	auto pg_info = duckpgq_state->GetPropertyGraph(property_graph);

	if (pg_info->vertex_tables.size() == 1 && pg_info->edge_tables.empty()) {
		// Special case where we don't want to create a union across the different
		// tables
		string stat_table_alias = pg_info->vertex_tables[0]->table_name + "_stats";
		return HandleSingleVertexTable(pg_info->vertex_tables[0], stat_table_alias);
	}

	auto final_union_node = make_uniq<SetOperationNode>();
	final_union_node->setop_type = SetOperationType::UNION;
	final_union_node->setop_all = true;
	for (auto &vertex_table : pg_info->vertex_tables) {
		string stat_table_alias = vertex_table->table_name + "_stats";
		auto inner_select_node = CreateInnerSelectStatNode(stat_table_alias);
		inner_select_node->cte_map.map.insert(stat_table_alias, CreateVertexTableCTE(vertex_table));
		AddToUnionNode(final_union_node, inner_select_node);
	}
	for (auto &edge_table : pg_info->edge_tables) {
		string stat_table_alias = edge_table->source_reference + "_" + edge_table->table_name + "_" +
		                          edge_table->destination_reference + "_stats";
		auto inner_select_node = CreateInnerSelectStatNode(stat_table_alias);
		inner_select_node->cte_map.map.insert(stat_table_alias, CreateEdgeTableCTE(edge_table));
		AddToUnionNode(final_union_node, inner_select_node);
	}

	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(final_union_node);
	auto subquery = make_uniq<SubqueryRef>(std::move(select_stmt));
	return std::move(subquery);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterSummarizePropertyGraphTableFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(SummarizePropertyGraphFunction());
}

} // namespace duckdb
