#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckpgq/parser/property_graph_table.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

struct DuckPGQSQL {
	static string Identifier(const string &identifier);
	static string StringLiteral(const string &value);
	static string QualifiedTableName(const string &catalog, const string &schema, const string &table);
	static string QualifiedTableName(const PropertyGraphTable &table);
	static string TableRef(const string &catalog, const string &schema, const string &table, const string &alias = "");
	static string TableRef(const PropertyGraphTable &table, const string &alias = "");
	static string Column(const string &column_name, const string &table_name = "");

	static unique_ptr<SelectStatement> ParseSelect(const string &query, const string &context = "DuckPGQ query");
	static unique_ptr<CommonTableExpressionInfo> ParseCTE(const string &query, const string &context = "DuckPGQ query");
	static unique_ptr<SubqueryExpression> ParseScalarSubquery(const string &query,
	                                                          const string &context = "DuckPGQ query");
	static unique_ptr<SubqueryRef> ParseSubqueryRef(const string &query, const string &alias = "",
	                                                const string &context = "DuckPGQ query");
	static unique_ptr<duckdb::TableRef> ParseFromTableRef(const string &table_ref_sql,
	                                                      const string &context = "DuckPGQ query");
	static unique_ptr<ParsedExpression> ParseExpression(const string &expression,
	                                                    const string &context = "DuckPGQ expression");
	static unique_ptr<ParsedExpression> ParseExpression(const string &expression, const string &alias,
	                                                    const string &context);
};

} // namespace duckdb
