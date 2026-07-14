#include "duckpgq/core/utils/duckpgq_sql.hpp"

#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"

namespace duckdb {

string DuckPGQSQL::Identifier(const string &identifier) {
	return SQLIdentifier::ToString(identifier);
}

string DuckPGQSQL::StringLiteral(const string &value) {
	return SQLString::ToString(value);
}

string DuckPGQSQL::QualifiedTableName(const string &catalog, const string &schema, const string &table) {
	string result;
	if (!catalog.empty()) {
		result += Identifier(catalog) + ".";
	}
	if (!schema.empty()) {
		result += Identifier(schema) + ".";
	}
	result += Identifier(table);
	return result;
}

string DuckPGQSQL::QualifiedTableName(const PropertyGraphTable &table) {
	return QualifiedTableName(table.catalog_name, table.schema_name, table.table_name);
}

string DuckPGQSQL::TableRef(const string &catalog, const string &schema, const string &table, const string &alias) {
	auto result = QualifiedTableName(catalog, schema, table);
	if (!alias.empty()) {
		result += " AS " + Identifier(alias);
	}
	return result;
}

string DuckPGQSQL::TableRef(const PropertyGraphTable &table, const string &alias) {
	auto result = QualifiedTableName(table);
	if (!alias.empty()) {
		result += " AS " + Identifier(alias);
	}
	return result;
}

string DuckPGQSQL::Column(const string &column_name, const string &table_name) {
	if (table_name.empty()) {
		return Identifier(column_name);
	}
	return Identifier(table_name) + "." + Identifier(column_name);
}

unique_ptr<SelectStatement> DuckPGQSQL::ParseSelect(const string &query, const string &context) {
	Parser parser;
	parser.ParseQuery(query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement while building " + context);
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
}

unique_ptr<CommonTableExpressionInfo> DuckPGQSQL::ParseCTE(const string &query, const string &context) {
	auto result = make_uniq<CommonTableExpressionInfo>();
	auto select_statement = ParseSelect(query, context);
	result->query_node = std::move(select_statement->node);
	return result;
}

unique_ptr<SubqueryExpression> DuckPGQSQL::ParseScalarSubquery(const string &query, const string &context) {
	auto result = make_uniq<SubqueryExpression>();
	result->SubqueryMutable() = ParseSelect(query, context);
	result->GetSubqueryTypeMutable() = SubqueryType::SCALAR;
	return result;
}

unique_ptr<SubqueryRef> DuckPGQSQL::ParseSubqueryRef(const string &query, const string &alias, const string &context) {
	return make_uniq<SubqueryRef>(ParseSelect(query, context), duckdb::Identifier(alias));
}

unique_ptr<duckdb::TableRef> DuckPGQSQL::ParseFromTableRef(const string &table_ref_sql, const string &context) {
	auto statement = ParseSelect("SELECT * FROM " + table_ref_sql, context);
	auto &select_node = statement->node->Cast<SelectNode>();
	return std::move(select_node.from_table);
}

unique_ptr<ParsedExpression> DuckPGQSQL::ParseExpression(const string &expression, const string &context) {
	auto select_statement = ParseSelect("SELECT " + expression, context);
	auto &select_node = select_statement->node->Cast<SelectNode>();
	return std::move(select_node.select_list[0]);
}

unique_ptr<ParsedExpression> DuckPGQSQL::ParseExpression(const string &expression, const string &alias,
                                                         const string &context) {
	auto select_statement = ParseSelect("SELECT " + expression + " AS " + Identifier(alias), context);
	auto &select_node = select_statement->node->Cast<SelectNode>();
	return std::move(select_node.select_list[0]);
}

} // namespace duckdb
