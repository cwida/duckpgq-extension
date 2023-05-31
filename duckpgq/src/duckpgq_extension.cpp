#define DUCKDB_EXTENSION_MAIN


#include "duckdb/parser/transformer.hpp"
#include "postgres_parser.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/parser_extension.hpp"

#include "duckpgq_extension.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckpgq/duckpgq_functions.hpp"
#include "duckdb/parser/parser_options.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

inline void DuckpgqScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) { 
			return StringVector::AddString(result, "Duckpgq "+name.GetString()+" 🐥");;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    auto &config = duckdb::DBConfig::GetConfig(instance);
    DuckPGQParserExtension pgq_parser;
    config.parser_extensions.push_back(pgq_parser);

	Connection con(instance);
    con.BeginTransaction();

    auto &catalog = Catalog::GetSystemCatalog(*con.context);

    for (auto &fun : DuckPGQFunctions::GetFunctions()) {
        catalog.CreateFunction(*con.context, fun);
    }

    CreateScalarFunctionInfo duckpgq_fun_info(
            ScalarFunction("duckpgq", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DuckpgqScalarFun));
    duckpgq_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    catalog.CreateFunction(*con.context, duckpgq_fun_info);
    con.Commit();
}

void DuckpgqExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

ParserExtensionParseResult duckpgq_parse(ParserExtensionInfo *info,
                                         const std::string &query) {
    ParserOptions options;
    Transformer transformer(options);
    vector<unique_ptr<SQLStatement>> statements;
    auto parse_info = (DuckPGQParserExtensionInfo &)(info);
    PostgresParser parser;
    string parser_error;
    parser.Parse((query[0] == '-') ? query.substr(1, query.length()) : query);
    if (parser.success) {
        if (!parser.parse_tree) {
            // empty statement
            return {"Empty statement"};
        }

        // if it succeeded, we transform the Postgres parse tree into a list of
        // SQLStatements
        transformer.TransformParseTree(parser.parse_tree, statements);
    } else {
        parser_error = QueryErrorContext::Format(query, parser.error_message, parser.error_location - 1);
        return {std::move(parser_error)};
    }

}

ParserExtensionPlanResult duckpgq_plan(ParserExtensionInfo *info, ClientContext &context,
                                       unique_ptr<ParserExtensionParseData> parse_data) {
    auto duckpgq_state = make_shared<DuckPGQState>(std::move(parse_data));
    context.registered_state["duckpgq"] = duckpgq_state;
    throw BinderException("use duckpgq_bind instead");
}


std::string DuckpgqExtension::Name() {
	return "duckpgq";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void duckpgq_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *duckpgq_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
