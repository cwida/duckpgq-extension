
#include "duckpgq/core/parser/duckpgq_parser.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/copy_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/insert_statement.hpp>
#include <duckpgq/core/functions/table/create_property_graph.hpp>
#include <duckpgq_state.hpp>

#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include <duckpgq/core/functions/table/describe_property_graph.hpp>
#include <duckpgq/core/functions/table/drop_property_graph.hpp>

#include <duckdb/parser/tableref/matchref.hpp>
#include <duckpgq/core/functions/table/summarize_property_graph.hpp>

#include "duckpgq/core/utils/duckpgq_utils.hpp"

namespace duckdb {

ParserExtensionParseResult duckpgq_parse(ParserExtensionInfo *info, const std::string &query) {
	Parser parser;
	parser.ParseQuery((query[0] == '-') ? query.substr(1, query.length()) : query);
	if (parser.statements.size() != 1) {
		throw Exception(ExceptionType::PARSER, "More than one statement detected, please only give one.");
	}
	return ParserExtensionParseResult(
	    make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(std::move(parser.statements[0])));
}

void duckpgq_find_match_function(unique_ptr<TableRef> &table_ref, DuckPGQState &duckpgq_state) {
	switch (table_ref->type) {
		case TableReferenceType::BASE_TABLE: {
			auto &table_function_ref = table_ref->Cast<TableFunctionRef>();
			auto &function = table_function_ref.function->Cast<FunctionExpression>();
			if (function.function_name != "duckpgq_match") {
				return;
			}
			table_function_ref.alias = function.children[0]->Cast<MatchExpression>().alias;
			int32_t match_index = duckpgq_state.match_index++;
			duckpgq_state.transform_expression[match_index] = std::move(function.children[0]);
			function.children.pop_back();
			auto function_identifier = make_uniq<ConstantExpression>(Value::CreateValue(match_index));
			function.children.push_back(std::move(function_identifier));
		}
			break;
		case TableReferenceType::SUBQUERY:
			// Handle SubqueryRef case
			auto subquery = table_ref->Cast<SubqueryRef>;
			duckpgq_find_select_statement(subquery, duckpgq_state);
			break;
		case TableReferenceType::JOIN:
			// Handle JoinRef case
			auto &join_ref = table_ref->Cast<JoinRef>();
			duckpgq_find_match_function(join_ref.left.get(), duckpgq_state);
			duckpgq_find_match_function(join_ref.right.get(), duckpgq_state);
			break;
		case TableReferenceType::TABLE_FUNCTION:
		case TableReferenceType::EXPRESSION_LIST:
		case TableReferenceType::CTE:
		case TableReferenceType::EMPTY_FROM:
		case TableReferenceType::PIVOT:
		case TableReferenceType::SHOW_REF:
		case TableReferenceType::COLUMN_DATA:
		case TableReferenceType::DELIM_GET:
		case TableReferenceType::BOUND_TABLE_REF:
		case TableReferenceType::INVALID:
		default:
			throw BinderException("MATCH statement is not yet supported in this table reference type");
	}
}

ParserExtensionPlanResult duckpgq_parse_showref(unique_ptr<TableRef> &table_ref, DuckPGQState &duckpgq_state) {
	auto &describe_node = table_ref->Cast<ShowRef>();
	ParserExtensionPlanResult result;
	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::QUERY_RESULT;
	if (describe_node.show_type == ShowType::SUMMARY) {
		result.function = SummarizePropertyGraphFunction();
		result.parameters.push_back(Value(describe_node.table_name));
		return result;
	}
	if (describe_node.show_type == ShowType::DESCRIBE) {
		result.function = DescribePropertyGraphFunction();
		return result;
	}
	throw BinderException("Unknown show type %s found.", describe_node.show_type);
}

ParserExtensionPlanResult duckpgq_find_select_statement(SQLStatement *statement, DuckPGQState &duckpgq_state) {
	auto &select_statement = statement->Cast<SelectStatement>();
	if (select_statement.node->type == QueryNodeType::SELECT_NODE) {
		auto &node = select_statement.node->Cast<SelectNode>();
		if (node.from_table->type == TableReferenceType::SHOW_REF) {
			duckpgq_parse_showref(node.from_table, duckpgq_state);
		}
		for (const auto &cte_kv : node.cte_map.map) {
			auto &cte = cte_kv.second;
			if (cte->query->type != StatementType::SELECT_STATEMENT) {
				continue;
			}
			auto &select_statement = cte->query->Cast<SelectStatement>();
			auto &select_node = select_statement.node->Cast<SelectNode>();
			duckpgq_find_match_function(select_node.from_table, duckpgq_state);

		}
	} else if (select_statement.node->type == QueryNodeType::CTE_NODE) {
		auto &cte_node = select_statement.node->Cast<CTENode>();
	}

	CommonTableExpressionMap *cte_map = nullptr;
	if (node) {
		cte_map = &node->cte_map;
	} else if (cte_node) {
		cte_map = &cte_node->cte_map;
	}

	if (!cte_map) {
		return {};
	}

	for (auto const &kv_pair : cte_map->map) {
		auto const &cte = kv_pair.second;

		auto *cte_select_statement = dynamic_cast<SelectStatement *>(cte->query.get());
		if (!cte_select_statement) {
			continue;
		}

		auto *select_node = dynamic_cast<SelectNode *>(cte_select_statement->node.get());
		if (!select_node) {
			continue; // The SelectStatement has no SelectNode, skip.
		}

		// If we get here, we know select_node is valid.
		duckpgq_find_match_function(select_node->from_table.get(), duckpgq_state);
	}
	if (node) {
		duckpgq_find_match_function(node->from_table.get(), duckpgq_state);
	} else {
		throw Exception(ExceptionType::INTERNAL, "node is a nullptr.");
	}
	return {};
}

ParserExtensionPlanResult duckpgq_handle_statement(SQLStatement *statement, DuckPGQState &duckpgq_state) {
	if (statement->type == StatementType::SELECT_STATEMENT) {
		auto result = duckpgq_find_select_statement(statement, duckpgq_state);
		if (result.function.bind == nullptr) {
			throw Exception(ExceptionType::BINDER, "use duckpgq_bind instead");
		}
		return result;
	}
	if (statement->type == StatementType::CREATE_STATEMENT) {
		const auto &create_statement = statement->Cast<CreateStatement>();
		const auto create_property_graph = dynamic_cast<CreatePropertyGraphInfo *>(create_statement.info.get());
		if (create_property_graph) {
			ParserExtensionPlanResult result;
			result.function = CreatePropertyGraphFunction();
			result.requires_valid_transaction = true;
			result.return_type = StatementReturnType::QUERY_RESULT;
			return result;
		}
		const auto create_table = reinterpret_cast<CreateTableInfo *>(create_statement.info.get());
		duckpgq_handle_statement(create_table->query.get(), duckpgq_state);
	}
	if (statement->type == StatementType::DROP_STATEMENT) {
		ParserExtensionPlanResult result;
		result.function = DropPropertyGraphFunction();
		result.requires_valid_transaction = true;
		result.return_type = StatementReturnType::QUERY_RESULT;
		return result;
	}
	if (statement->type == StatementType::EXPLAIN_STATEMENT) {
		auto &explain_statement = statement->Cast<ExplainStatement>();
		duckpgq_handle_statement(explain_statement.stmt.get(), duckpgq_state);
	}
	if (statement->type == StatementType::COPY_STATEMENT) {
		const auto &copy_statement = statement->Cast<CopyStatement>();
		const auto select_node = dynamic_cast<SelectNode *>(copy_statement.info->select_statement.get());
		duckpgq_find_match_function(select_node->from_table.get(), duckpgq_state);
		throw Exception(ExceptionType::BINDER, "use duckpgq_bind instead");
	}
	if (statement->type == StatementType::INSERT_STATEMENT) {
		const auto &insert_statement = statement->Cast<InsertStatement>();
		duckpgq_handle_statement(insert_statement.select_statement.get(), duckpgq_state);
	}

	throw Exception(ExceptionType::NOT_IMPLEMENTED,
	                StatementTypeToString(statement->type) + "has not been implemented yet for DuckPGQ queries");
}

ParserExtensionPlanResult duckpgq_plan(ParserExtensionInfo *, ClientContext &context,
                                       unique_ptr<ParserExtensionParseData> parse_data) {
	auto duckpgq_state = GetDuckPGQState(context);
	duckpgq_state->parse_data = std::move(parse_data);
	auto duckpgq_parse_data = dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());

	if (!duckpgq_parse_data) {
		throw Exception(ExceptionType::BINDER, "No DuckPGQ parse data found");
	}

	auto statement = duckpgq_parse_data->statement.get();
	return duckpgq_handle_statement(statement, *duckpgq_state);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CorePGQParser::RegisterPGQParserExtension(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.parser_extensions.push_back(DuckPGQParserExtension());
}

} // namespace duckdb
