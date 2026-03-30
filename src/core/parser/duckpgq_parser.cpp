
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
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include <duckpgq/core/functions/table/describe_property_graph.hpp>
#include <duckpgq/core/functions/table/drop_property_graph.hpp>

#include <duckdb/parser/tableref/matchref.hpp>
#include <duckpgq/core/functions/table/summarize_property_graph.hpp>

#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
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

// Recursively traverse a QueryNode tree, finding and processing all MATCH expressions.
// Handles SelectNode, CTENode, and SetOperationNode (UNION/INTERSECT/EXCEPT).
static void duckpgq_traverse_query_node(QueryNode *query_node, DuckPGQState &duckpgq_state) {
	if (!query_node) {
		return;
	}

	// All QueryNode types can have CTEs — process them first
	for (auto const &kv_pair : query_node->cte_map.map) {
		auto const &cte = kv_pair.second;
		auto *cte_select_statement = dynamic_cast<SelectStatement *>(cte->query.get());
		if (cte_select_statement) {
			duckpgq_traverse_query_node(cte_select_statement->node.get(), duckpgq_state);
		}
	}

	switch (query_node->type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select_node = query_node->Cast<SelectNode>();
		if (select_node.from_table) {
			duckpgq_find_match_function(select_node.from_table.get(), duckpgq_state);
		}
		break;
	}
	case QueryNodeType::CTE_NODE: {
		auto &cte_node = query_node->Cast<CTENode>();
		duckpgq_traverse_query_node(cte_node.child.get(), duckpgq_state);
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop_node = query_node->Cast<SetOperationNode>();
		for (auto &child : setop_node.children) {
			duckpgq_traverse_query_node(child.get(), duckpgq_state);
		}
		break;
	}
	case QueryNodeType::INSERT_QUERY_NODE: {
		auto &insert_node = query_node->Cast<InsertQueryNode>();
		if (insert_node.select_statement) {
			duckpgq_traverse_query_node(insert_node.select_statement->node.get(), duckpgq_state);
		}
		break;
	}
	default:
		break;
	}
}

void duckpgq_find_match_function(TableRef *table_ref, DuckPGQState &duckpgq_state) {
	// TODO(dtenwolde) add support for other style of tableRef (e.g. PivotRef)
	if (auto table_function_ref = dynamic_cast<TableFunctionRef *>(table_ref)) {
		// Handle TableFunctionRef case
		auto function = dynamic_cast<FunctionExpression *>(table_function_ref->function.get());
		if (function->function_name != "duckpgq_match") {
			return;
		}
		table_function_ref->alias = function->children[0]->Cast<MatchExpression>().alias;
		int32_t match_index = duckpgq_state.match_index++;
		duckpgq_state.transform_expression[match_index] = std::move(function->children[0]);
		function->children.pop_back();
		auto function_identifier = make_uniq<ConstantExpression>(Value::CreateValue(match_index));
		function->children.push_back(std::move(function_identifier));
	} else if (auto join_ref = dynamic_cast<JoinRef *>(table_ref)) {
		// Handle JoinRef case
		duckpgq_find_match_function(join_ref->left.get(), duckpgq_state);
		duckpgq_find_match_function(join_ref->right.get(), duckpgq_state);
	} else if (auto subquery_ref = dynamic_cast<SubqueryRef *>(table_ref)) {
		// Handle SubqueryRef case
		auto subquery = subquery_ref->subquery.get();
		duckpgq_find_select_statement(subquery, duckpgq_state);
	}
}

ParserExtensionPlanResult duckpgq_find_select_statement(SQLStatement *statement, DuckPGQState &duckpgq_state) {
	const auto select_statement = dynamic_cast<SelectStatement *>(statement);
	if (!select_statement) {
		return {};
	}
	auto *query_node = select_statement->node.get();

	// Check for ShowRef (DESCRIBE / SUMMARY property graph) — only applies to SelectNode.
	// A CTENode wrapping a SelectNode is also checked for the DESCRIBE/SUMMARY case.
	SelectNode *show_check_node = nullptr;
	if (query_node->type == QueryNodeType::SELECT_NODE) {
		show_check_node = &query_node->Cast<SelectNode>();
	} else if (query_node->type == QueryNodeType::CTE_NODE) {
		auto &cte_node = query_node->Cast<CTENode>();
		if (cte_node.child && cte_node.child->type == QueryNodeType::SELECT_NODE) {
			show_check_node = &cte_node.child->Cast<SelectNode>();
		}
	}
	if (show_check_node && show_check_node->from_table) {
		const auto describe_node = dynamic_cast<ShowRef *>(show_check_node->from_table.get());
		if (describe_node) {
			ParserExtensionPlanResult result;
			result.requires_valid_transaction = true;
			result.return_type = StatementReturnType::QUERY_RESULT;
			if (describe_node->show_type == ShowType::SUMMARY) {
				result.function = SummarizePropertyGraphFunction();
				result.parameters.push_back(Value(describe_node->table_name));
				return result;
			}
			if (describe_node->show_type == ShowType::DESCRIBE) {
				result.function = DescribePropertyGraphFunction();
				return result;
			}
			throw BinderException("Unknown show type %s found.", describe_node->show_type);
		}
	}

	// Recursively find and process all MATCH expressions in the query tree
	duckpgq_traverse_query_node(query_node, duckpgq_state);
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
		duckpgq_handle_statement(insert_statement.node->select_statement.get(), duckpgq_state);
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
	auto &manager = ExtensionCallbackManager::Get(db);
	manager.Register(DuckPGQParserExtension());
}

} // namespace duckdb
