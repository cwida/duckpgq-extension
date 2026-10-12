#include "duckpgq/core/parser/duckpgq_parser.hpp"

#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckpgq/core/functions/table/create_property_graph.hpp"
#include "duckpgq/core/functions/table/drop_property_graph.hpp"
#include "duckpgq/core/utils/duckpgq_utils.hpp"
#include "duckpgq/parser/parsed_data/create_property_graph_info.hpp"
#include "duckpgq_state.hpp"

namespace duckdb {

//! Plans the graph-management statements that the grammar extension wraps in an ExtensionStatement. Queries
//! containing GRAPH_TABLE are native SELECT statements and never reach this callback.
static ParserExtensionPlanResult duckpgq_plan_statement(SQLStatement &statement) {
	if (statement.type == StatementType::CREATE_STATEMENT) {
		auto &create_statement = statement.Cast<CreateStatement>();
		if (dynamic_cast<CreatePropertyGraphInfo *>(create_statement.info.get())) {
			ParserExtensionPlanResult result;
			result.function = CreatePropertyGraphFunction();
			result.requires_valid_transaction = true;
			result.return_type = StatementReturnType::QUERY_RESULT;
			return result;
		}
	}
	if (statement.type == StatementType::DROP_STATEMENT) {
		ParserExtensionPlanResult result;
		result.function = DropPropertyGraphFunction();
		result.requires_valid_transaction = true;
		result.return_type = StatementReturnType::QUERY_RESULT;
		return result;
	}
	throw NotImplementedException("%s has not been implemented yet for DuckPGQ statements",
	                              StatementTypeToString(statement.type));
}

ParserExtensionPlanResult duckpgq_plan(ParserExtensionInfo *, ClientContext &context,
                                       unique_ptr<ParserExtensionParseData> parse_data) {
	auto duckpgq_state = GetDuckPGQState(context);
	duckpgq_state->parse_data = std::move(parse_data);
	auto duckpgq_parse_data = dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());
	if (!duckpgq_parse_data) {
		throw BinderException("No DuckPGQ parse data found");
	}
	return duckpgq_plan_statement(*duckpgq_parse_data->statement);
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
