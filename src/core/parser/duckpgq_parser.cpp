
#include "duckpgq/core/parser/duckpgq_parser.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/copy_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/extension_statement.hpp>
#include <duckdb/parser/statement/insert_statement.hpp>
#include <duckpgq/third_party/duckdb_peg_parser/peg/matcher.hpp>
#include <duckpgq/third_party/duckdb_peg_parser/peg/tokenizer/parser_tokenizer.hpp>
#include <duckpgq/third_party/duckdb_peg_parser/peg/transformer/peg_transformer.hpp>
#include <duckpgq/core/functions/table/create_property_graph.hpp>
#include <duckpgq_state.hpp>

#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include <duckpgq/core/functions/table/describe_property_graph.hpp>
#include <duckpgq/core/functions/table/drop_property_graph.hpp>
#include <duckdb/parser/parsed_data/drop_property_graph_info.hpp>

#include <duckdb/parser/tableref/matchref.hpp>
#include <duckpgq/core/functions/table/summarize_property_graph.hpp>

#include "duckdb/main/extension_callback_manager.hpp"
#include "duckpgq/core/utils/duckpgq_utils.hpp"

namespace duckdb {

static bool DuckPGQShouldWrapStatement(const unique_ptr<SQLStatement> &statement) {
	if (statement->type == StatementType::CREATE_STATEMENT) {
		auto &create_statement = statement->Cast<CreateStatement>();
		if (create_statement.info->type != CatalogType::INVALID) {
			return false;
		}
		create_statement.info->Cast<CreatePropertyGraphInfo>();
		return true;
	}
	if (statement->type == StatementType::DROP_STATEMENT) {
		auto &drop_statement = statement->Cast<DropStatement>();
		if (drop_statement.info->type != CatalogType::INVALID) {
			return false;
		}
		drop_statement.info->Cast<DropPropertyGraphInfo>();
		return true;
	}
	return false;
}

static unique_ptr<SQLStatement> DuckPGQWrapStatement(unique_ptr<SQLStatement> statement) {
	auto parse_data = make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(std::move(statement));
	return make_uniq<ExtensionStatement>(DuckPGQParserExtension(), std::move(parse_data));
}

ParserOverrideResult duckpgq_parser_override(ParserExtensionInfo *info, const string &query, ParserOptions &options) {
	try {
		auto normalized_query = Parser::NormalizeSQLString(query);
		vector<duckpgq_peg::MatcherToken> tokens;
		duckpgq_peg::ParserTokenizer tokenizer(normalized_query, tokens);
		tokenizer.TokenizeInput();

		static duckpgq_peg::ParserCache duckpgq_parser_cache;
		auto matcher = duckpgq_parser_cache.GetMatcher();
		auto transformer_factory = duckpgq_parser_cache.GetTransformerFactory();

		vector<unique_ptr<SQLStatement>> statements;
		idx_t token_cursor = 0;
		while (token_cursor < tokens.size()) {
			auto statement = transformer_factory->TransformTopLevelStatement(
			    tokens, options, matcher->TopLevelStatementMatcher(), token_cursor);
			if (statement) {
				if (DuckPGQShouldWrapStatement(statement)) {
					statement = DuckPGQWrapStatement(std::move(statement));
				}
				statements.push_back(std::move(statement));
			}
		}

		if (!statements.empty()) {
			for (idx_t i = 0; i + 1 < statements.size(); i++) {
				statements[i]->stmt_length = statements[i + 1]->stmt_location - statements[i]->stmt_location;
			}
			statements.back()->stmt_length = normalized_query.size() - statements.back()->stmt_location;
			for (auto &statement : statements) {
				statement->query = normalized_query.substr(statement->stmt_location, statement->stmt_length);
				statement->stmt_location = 0;
				statement->stmt_length = statement->query.size();
				if (statement->type == StatementType::CREATE_STATEMENT) {
					auto &create = statement->Cast<CreateStatement>();
					create.info->sql = statement->query;
				}
			}
		}

		return ParserOverrideResult(std::move(statements));
	} catch (std::exception &ex) {
		return ParserOverrideResult(ex);
	}
}

void duckpgq_find_match_function(TableRef *table_ref, DuckPGQState &duckpgq_state) {
	// TODO(dtenwolde) add support for other style of tableRef (e.g. PivotRef)
	if (auto table_function_ref = dynamic_cast<TableFunctionRef *>(table_ref)) {
		// Handle TableFunctionRef case
		auto function = dynamic_cast<FunctionExpression *>(table_function_ref->function.get());
		if (function->FunctionName() != "duckpgq_match") {
			return;
		}
		auto &arguments = function->GetArgumentsMutable();
		table_function_ref->alias = Identifier(arguments[0].GetExpressionMutable()->Cast<MatchExpression>().alias);
		int32_t match_index = duckpgq_state.match_index++;
		duckpgq_state.transform_expression[match_index] = std::move(arguments[0].GetExpressionMutable());
		arguments.pop_back();
		auto function_identifier = make_uniq<ConstantExpression>(Value::CreateValue(match_index));
		arguments.emplace_back(std::move(function_identifier));
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
	auto node = dynamic_cast<SelectNode *>(select_statement->node.get());
	CTENode *cte_node = nullptr;

	// Check if node is not a SelectNode
	if (!node) {
		// Attempt to cast to CTENode
		cte_node = dynamic_cast<CTENode *>(select_statement->node.get());
		if (cte_node) {
			// Get the child node as a SelectNode if cte_node is valid
			node = dynamic_cast<SelectNode *>(cte_node->child.get());
		}
	}

	// Check if node is a ShowRef
	if (node) {
		const auto describe_node = dynamic_cast<ShowRef *>(node->from_table.get());
		if (describe_node) {
			ParserExtensionPlanResult result;
			result.requires_valid_transaction = true;
			result.return_type = StatementReturnType::QUERY_RESULT;
			if (describe_node->show_type == ShowType::SUMMARY) {
				result.function = SummarizePropertyGraphFunction();
				result.parameters.push_back(Value(describe_node->GetTableName().GetIdentifierName()));
				return result;
			}
			if (describe_node->show_type == ShowType::DESCRIBE) {
				result.function = DescribePropertyGraphFunction();
				return result;
			}
			throw BinderException("Unknown show type %s found.", describe_node->show_type);
		}
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

		auto *select_node = dynamic_cast<SelectNode *>(cte->query_node.get());
		if (!select_node) {
			continue;
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
