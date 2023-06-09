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
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "duckdb/parser/tableref/subqueryref.hpp"


namespace duckdb {

inline void DuckpgqScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Duckpgq "+name.GetString()+" 🐥");;
        });
}

struct BindReplaceDuckPGQFun {
    struct DuckPGQFunctionData : public TableFunctionData {
        bool done = false;
    };

    static duckdb::unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
                                                 duckdb::vector<LogicalType> &return_types,
                                                 duckdb::vector<string> &names) {
        auto result = make_uniq<BindReplaceDuckPGQFun::DuckPGQFunctionData>();

        return std::move(result);
    }

    static unique_ptr<TableRef> BindReplace(ClientContext &context, TableFunctionBindInput &input) {
        auto data = make_uniq<BindReplaceDuckPGQFun::DuckPGQFunctionData>();

        auto select_statement = make_uniq<SelectStatement>();
        auto result = make_uniq<SubqueryRef>(std::move(select_statement));

        return result;
    }

    static void Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
        auto &state = (BindReplaceDuckPGQFun::DuckPGQFunctionData &)*data.bind_data;

        if (!state.done) {
            state.done = true;
            output.SetCardinality(1);
        } else {
            output.SetCardinality(0);
        }
    }
};


static void LoadInternal(DatabaseInstance &instance) {
    auto &config = DBConfig::GetConfig(instance);
    DuckPGQParserExtension pgq_parser;
    config.parser_extensions.push_back(pgq_parser);
    config.operator_extensions.push_back(make_uniq<DuckPGQOperatorExtension>());

	Connection con(instance);
    con.BeginTransaction();

    auto &catalog = Catalog::GetSystemCatalog(*con.context);

    TableFunction bind_replace_duckpgq("graph_table", {},
                                       BindReplaceDuckPGQFun::Function, BindReplaceDuckPGQFun::Bind);
    bind_replace_duckpgq.bind_replace = BindReplaceDuckPGQFun::BindReplace;
    CreateTableFunctionInfo bind_replace_duckpgq_info(bind_replace_duckpgq);
    catalog.CreateTableFunction(*con.context, bind_replace_duckpgq_info);

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
    auto parse_info = (DuckPGQParserExtensionInfo &)(info);
    Parser parser;
    parser.ParseQuery((query[0] == '-') ? query.substr(1, query.length()) : query);
    if (parser.statements.size() != 1) {
        throw ParserException("More than 1 statement detected, please only give one.");
    }
    return {make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(std::move(parser.statements[0]))};

}

BoundStatement duckpgq_bind(ClientContext &context, Binder &binder,
                            OperatorExtensionInfo *info, SQLStatement &statement) {
    switch (statement.type) {
        case StatementType::EXTENSION_STATEMENT: {
            auto &extension_statement = dynamic_cast<ExtensionStatement &>(statement);
            if (extension_statement.extension.parse_function == duckpgq_parse) {
                auto lookup = context.registered_state.find("duckpgq");
                if (lookup != context.registered_state.end()) {
                    auto duckpgq_state = (DuckPGQState *)lookup->second.get();
                    auto duckpgq_binder = Binder::CreateBinder(context);
                    auto duckpgq_parse_data =
                            dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());
                    return duckpgq_binder->Bind(*(duckpgq_parse_data->statement));
                }
                throw BinderException("Registered state not found");
            }
        }
        default:
            // No-op empty
            return {};
    }
}

ParserExtensionPlanResult duckpgq_plan(ParserExtensionInfo *info, ClientContext &context,
                                       unique_ptr<ParserExtensionParseData> parse_data) {
    auto duckpgq_state_entry = context.registered_state.find("duckpgq");
    if (duckpgq_state_entry == context.registered_state.end()) {
        context.registered_state["duckpgq"] = make_shared<DuckPGQState>(std::move(parse_data));
    } else {
        auto duckpgq_state = (DuckPGQState *)duckpgq_state_entry->second.get();
        duckpgq_state->parse_data = std::move(parse_data);
    }
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
