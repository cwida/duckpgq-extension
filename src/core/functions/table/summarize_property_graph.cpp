#include "duckpgq/core/functions/table/summarize_property_graph.hpp"
#include "duckpgq/core/utils/duckpgq_sql.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include <duckpgq/core/functions/table.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckdb {

static string PGQSQLNullAlias(const string &alias) {
	return "NULL AS " + DuckPGQSQL::Identifier(alias);
}

static string PGQSQLStringAlias(const string &value, const string &alias) {
	return DuckPGQSQL::StringLiteral(value) + " AS " + DuckPGQSQL::Identifier(alias);
}

static string PGQSQLBooleanAlias(bool value, const string &alias) {
	return string(value ? "true" : "false") + " AS " + DuckPGQSQL::Identifier(alias);
}

static string PGQSQLCountStarAlias(const string &alias) {
	return "count(*) AS " + DuckPGQSQL::Identifier(alias);
}

static string PGQSQLDegreeColumn(bool is_in_degree) {
	return is_in_degree ? "in_degree" : "out_degree";
}

static string PGQSQLDegreeStatisticColumn(const string &statistic_name, bool is_in_degree) {
	return statistic_name + "_" + PGQSQLDegreeColumn(is_in_degree);
}

static string PGQSQLDegreeStatisticExpression(const string &aggregate_function, const string &statistic_name,
                                              bool is_in_degree, const string &argument = "") {
	auto degree_column = PGQSQLDegreeColumn(is_in_degree);
	auto statistic_column = PGQSQLDegreeStatisticColumn(statistic_name, is_in_degree);
	if (argument.empty()) {
		return aggregate_function + "(" + DuckPGQSQL::Identifier(degree_column) + ") AS " +
		       DuckPGQSQL::Identifier(statistic_column);
	}
	return aggregate_function + "(" + DuckPGQSQL::Identifier(degree_column) + ", " + argument + ") AS " +
	       DuckPGQSQL::Identifier(statistic_column);
}

static string PGQSQLDegreeStatisticExpression(const string &aggregate_function, bool is_in_degree,
                                              const string &argument = "") {
	return PGQSQLDegreeStatisticExpression(aggregate_function, aggregate_function, is_in_degree, argument);
}

static string PGQSQLDegreeStatisticsCTE(const shared_ptr<PropertyGraphTable> &pg_table, const string &degree_column,
                                        bool is_in_degree) {
	std::ostringstream query;
	query << "SELECT " << PGQSQLDegreeStatisticExpression("avg", is_in_degree) << ", "
	      << PGQSQLDegreeStatisticExpression("min", is_in_degree) << ", "
	      << PGQSQLDegreeStatisticExpression("max", is_in_degree) << ", "
	      << PGQSQLDegreeStatisticExpression("approx_quantile", "q25", is_in_degree, "0.25") << ", "
	      << PGQSQLDegreeStatisticExpression("approx_quantile", "q50", is_in_degree, "0.5") << ", "
	      << PGQSQLDegreeStatisticExpression("approx_quantile", "q75", is_in_degree, "0.75") << " FROM (SELECT "
	      << DuckPGQSQL::Identifier(degree_column) << ", count(*) AS "
	      << DuckPGQSQL::Identifier(PGQSQLDegreeColumn(is_in_degree)) << " FROM "
	      << DuckPGQSQL::TableRef(*pg_table, "degree_source") << " GROUP BY " << DuckPGQSQL::Identifier(degree_column)
	      << ") AS degree_groups";
	return query.str();
}

static string PGQSQLDistinctCount(const shared_ptr<PropertyGraphTable> &pg_table, bool is_source) {
	auto column_to_count = is_source ? pg_table->source_fk[0] : pg_table->destination_fk[0];
	std::ostringstream query;
	query << "(SELECT count(DISTINCT " << DuckPGQSQL::Column(column_to_count, "edge_count") << ") FROM "
	      << DuckPGQSQL::TableRef(*pg_table, "edge_count") << ")";
	return query.str();
}

static string PGQSQLIsolatedNodes(shared_ptr<PropertyGraphTable> &pg_table, bool is_source) {
	auto table_reference = is_source ? pg_table->source_reference : pg_table->destination_reference;
	auto table_schema = is_source ? pg_table->source_schema : pg_table->destination_schema;
	auto table_catalog = is_source ? pg_table->source_catalog : pg_table->destination_catalog;
	auto pk_reference = is_source ? pg_table->source_pk[0] : pg_table->destination_pk[0];
	auto fk_reference = is_source ? pg_table->source_fk[0] : pg_table->destination_fk[0];

	std::ostringstream query;
	query << "(SELECT count(" << DuckPGQSQL::Column(pk_reference, "vertex_table") << ") FROM "
	      << DuckPGQSQL::TableRef(table_catalog, table_schema, table_reference, "vertex_table") << " LEFT JOIN "
	      << DuckPGQSQL::TableRef(*pg_table, "edge_table") << " ON " << DuckPGQSQL::Column(pk_reference, "vertex_table")
	      << " = " << DuckPGQSQL::Column(fk_reference, "edge_table") << " WHERE "
	      << DuckPGQSQL::Column(fk_reference, "edge_table") << " IS NULL)";
	return query.str();
}

static string PGQSQLDegreeStatisticScalar(const string &aggregate_function, bool is_in_degree) {
	auto statistic_column = PGQSQLDegreeStatisticColumn(aggregate_function, is_in_degree);
	auto cte_name = is_in_degree ? "in_degrees" : "out_degrees";
	return "(SELECT " + DuckPGQSQL::Identifier(statistic_column) + " FROM " + DuckPGQSQL::Identifier(cte_name) + ")";
}

static string PGQSQLVertexTableCTE(const shared_ptr<PropertyGraphTable> &vertex_table) {
	std::ostringstream query;
	query << "SELECT " << PGQSQLStringAlias(vertex_table->table_name, "table_name") << ", "
	      << PGQSQLBooleanAlias(true, "is_vertex_table") << ", " << PGQSQLNullAlias("source_table") << ", "
	      << PGQSQLNullAlias("destination_table") << ", " << PGQSQLCountStarAlias("vertex_count") << ", "
	      << PGQSQLNullAlias("edge_count") << ", " << PGQSQLNullAlias("unique_source_count") << ", "
	      << PGQSQLNullAlias("unique_destination_count") << ", " << PGQSQLNullAlias("isolated_sources") << ", "
	      << PGQSQLNullAlias("isolated_destinations") << ", " << PGQSQLNullAlias("avg_in_degree") << ", "
	      << PGQSQLNullAlias("min_in_degree") << ", " << PGQSQLNullAlias("max_in_degree") << ", "
	      << PGQSQLNullAlias("q25_in_degree") << ", " << PGQSQLNullAlias("q50_in_degree") << ", "
	      << PGQSQLNullAlias("q75_in_degree") << ", " << PGQSQLNullAlias("avg_out_degree") << ", "
	      << PGQSQLNullAlias("min_out_degree") << ", " << PGQSQLNullAlias("max_out_degree") << ", "
	      << PGQSQLNullAlias("q25_out_degree") << ", " << PGQSQLNullAlias("q50_out_degree") << ", "
	      << PGQSQLNullAlias("q75_out_degree") << " FROM " << DuckPGQSQL::TableRef(*vertex_table, "vertex_table");
	return query.str();
}

static string PGQSQLEdgeTableCTE(shared_ptr<PropertyGraphTable> &edge_table) {
	std::ostringstream query;
	query << "WITH in_degrees AS (" << PGQSQLDegreeStatisticsCTE(edge_table, edge_table->destination_fk[0], true)
	      << "), out_degrees AS (" << PGQSQLDegreeStatisticsCTE(edge_table, edge_table->source_fk[0], false)
	      << ") SELECT " << PGQSQLStringAlias(edge_table->table_name, "table_name") << ", "
	      << PGQSQLBooleanAlias(false, "is_vertex_table") << ", "
	      << PGQSQLStringAlias(edge_table->source_reference, "source_table") << ", "
	      << PGQSQLStringAlias(edge_table->destination_reference, "destination_table") << ", "
	      << PGQSQLNullAlias("vertex_count") << ", " << PGQSQLCountStarAlias("edge_count") << ", "
	      << PGQSQLDistinctCount(edge_table, true) << " AS " << DuckPGQSQL::Identifier("unique_source_count") << ", "
	      << PGQSQLDistinctCount(edge_table, false) << " AS " << DuckPGQSQL::Identifier("unique_destination_count")
	      << ", " << PGQSQLIsolatedNodes(edge_table, true) << " AS " << DuckPGQSQL::Identifier("isolated_sources")
	      << ", " << PGQSQLIsolatedNodes(edge_table, false) << " AS " << DuckPGQSQL::Identifier("isolated_destinations")
	      << ", " << PGQSQLDegreeStatisticScalar("avg", true) << " AS " << DuckPGQSQL::Identifier("avg_in_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("min", true) << " AS " << DuckPGQSQL::Identifier("min_in_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("max", true) << " AS " << DuckPGQSQL::Identifier("max_in_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("q25", true) << " AS " << DuckPGQSQL::Identifier("q25_in_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("q50", true) << " AS " << DuckPGQSQL::Identifier("q50_in_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("q75", true) << " AS " << DuckPGQSQL::Identifier("q75_in_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("avg", false) << " AS " << DuckPGQSQL::Identifier("avg_out_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("min", false) << " AS " << DuckPGQSQL::Identifier("min_out_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("max", false) << " AS " << DuckPGQSQL::Identifier("max_out_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("q25", false) << " AS " << DuckPGQSQL::Identifier("q25_out_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("q50", false) << " AS " << DuckPGQSQL::Identifier("q50_out_degree")
	      << ", " << PGQSQLDegreeStatisticScalar("q75", false) << " AS " << DuckPGQSQL::Identifier("q75_out_degree")
	      << " FROM " << DuckPGQSQL::TableRef(*edge_table, "edge_table");
	return query.str();
}

static string PGQSQLSummaryQuery(CreatePropertyGraphInfo &pg_info) {
	std::ostringstream query;
	query << "WITH ";
	bool needs_comma = false;
	vector<string> stat_table_aliases;
	for (auto &vertex_table : pg_info.vertex_tables) {
		auto stat_table_alias = vertex_table->table_name + "_stats";
		if (needs_comma) {
			query << ", ";
		}
		query << DuckPGQSQL::Identifier(stat_table_alias) << " AS (" << PGQSQLVertexTableCTE(vertex_table) << ")";
		stat_table_aliases.push_back(stat_table_alias);
		needs_comma = true;
	}
	for (auto &edge_table : pg_info.edge_tables) {
		auto stat_table_alias = edge_table->source_reference + "_" + edge_table->table_name + "_" +
		                        edge_table->destination_reference + "_stats";
		if (needs_comma) {
			query << ", ";
		}
		query << DuckPGQSQL::Identifier(stat_table_alias) << " AS (" << PGQSQLEdgeTableCTE(edge_table) << ")";
		stat_table_aliases.push_back(stat_table_alias);
		needs_comma = true;
	}
	for (idx_t i = 0; i < stat_table_aliases.size(); i++) {
		if (i > 0) {
			query << " UNION ALL ";
		}
		query << " SELECT * FROM " << DuckPGQSQL::Identifier(stat_table_aliases[i]);
	}
	return query.str();
}

unique_ptr<ParsedExpression> GetTableNameConstantExpression(const string &table_name, const string &alias) {
	return DuckPGQSQL::ParseExpression(DuckPGQSQL::StringLiteral(table_name), alias);
}

unique_ptr<ParsedExpression> GetFunctionExpression(const string &aggregate_function, const string &alias,
                                                   bool is_in_degree, const Value &value = nullptr) {
	string argument;
	if (!value.IsNull()) {
		argument = value.ToSQLString();
	}
	return DuckPGQSQL::ParseExpression(
	    PGQSQLDegreeStatisticExpression(aggregate_function, alias, is_in_degree, argument),
	    alias + (is_in_degree ? "_in_degree" : "_out_degree"));
}

unique_ptr<ParsedExpression> GetConstantNullExpressionWithAlias(const string &alias) {
	return DuckPGQSQL::ParseExpression("NULL", alias);
}

unique_ptr<ParsedExpression> IsVertexTableConstantExpression(bool is_vertex_table, const string &alias) {
	return DuckPGQSQL::ParseExpression(is_vertex_table ? "true" : "false", alias);
}

unique_ptr<ParsedExpression> GetTableCount(const string &alias) {
	return DuckPGQSQL::ParseExpression("count(*)", alias);
}

unique_ptr<ParsedExpression>
SummarizePropertyGraphFunction::GetDistinctCount(const shared_ptr<PropertyGraphTable> &pg_table, const string &alias,
                                                 bool is_source) {
	return DuckPGQSQL::ParseExpression(PGQSQLDistinctCount(pg_table, is_source), alias);
}

unique_ptr<ParsedExpression> SummarizePropertyGraphFunction::GetIsolatedNodes(shared_ptr<PropertyGraphTable> &pg_table,
                                                                              const string &alias, bool is_source) {
	return DuckPGQSQL::ParseExpression(PGQSQLIsolatedNodes(pg_table, is_source), alias);
}

unique_ptr<SubqueryRef>
SummarizePropertyGraphFunction::CreateGroupBySubquery(const shared_ptr<PropertyGraphTable> &pg_table, bool is_in_degree,
                                                      const string &degree_column) {
	std::ostringstream query;
	query << "SELECT " << DuckPGQSQL::Identifier(degree_column) << ", count(*) AS "
	      << DuckPGQSQL::Identifier(PGQSQLDegreeColumn(is_in_degree)) << " FROM "
	      << DuckPGQSQL::TableRef(*pg_table, "degree_source") << " GROUP BY " << DuckPGQSQL::Identifier(degree_column);
	return DuckPGQSQL::ParseSubqueryRef(query.str());
}

unique_ptr<CommonTableExpressionInfo>
SummarizePropertyGraphFunction::CreateDegreeStatisticsCTE(const shared_ptr<PropertyGraphTable> &pg_table,
                                                          const string &degree_column, bool is_in_degree) {
	return DuckPGQSQL::ParseCTE(PGQSQLDegreeStatisticsCTE(pg_table, degree_column, is_in_degree));
}

unique_ptr<ParsedExpression> SummarizePropertyGraphFunction::GetDegreeStatistics(const string &aggregate_function,
                                                                                 bool is_in_degree) {
	return DuckPGQSQL::ParseExpression(PGQSQLDegreeStatisticScalar(aggregate_function, is_in_degree),
	                                   PGQSQLDegreeStatisticColumn(aggregate_function, is_in_degree));
}

unique_ptr<CommonTableExpressionInfo>
SummarizePropertyGraphFunction::CreateVertexTableCTE(const shared_ptr<PropertyGraphTable> &vertex_table) {
	return DuckPGQSQL::ParseCTE(PGQSQLVertexTableCTE(vertex_table));
}

unique_ptr<CommonTableExpressionInfo>
SummarizePropertyGraphFunction::CreateEdgeTableCTE(shared_ptr<PropertyGraphTable> &edge_table) {
	return DuckPGQSQL::ParseCTE(PGQSQLEdgeTableCTE(edge_table));
}

unique_ptr<TableRef>
SummarizePropertyGraphFunction::HandleSingleVertexTable(const shared_ptr<PropertyGraphTable> &vertex_table,
                                                        const string &stat_table_alias) {
	std::ostringstream query;
	query << "WITH " << DuckPGQSQL::Identifier(stat_table_alias) << " AS (" << PGQSQLVertexTableCTE(vertex_table)
	      << ") SELECT * FROM " << DuckPGQSQL::Identifier(stat_table_alias);
	return DuckPGQSQL::ParseSubqueryRef(query.str());
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

	return DuckPGQSQL::ParseSubqueryRef(PGQSQLSummaryQuery(*pg_info));
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterSummarizePropertyGraphTableFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(SummarizePropertyGraphFunction());
}

} // namespace duckdb
