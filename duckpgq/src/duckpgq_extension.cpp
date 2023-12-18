#define DUCKDB_EXTENSION_MAIN

#include "duckpgq_extension.hpp"

#include <duckpgq/operators/path_finding_operator.hpp>

#include "duckdb/function/scalar_function.hpp"
#include "duckpgq/duckpgq_functions.hpp"

#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"

#include "duckpgq/functions/tablefunctions/drop_property_graph.hpp"
#include "duckpgq/functions/tablefunctions/create_property_graph.hpp"
#include "duckpgq/functions/tablefunctions/match.hpp"

namespace duckdb {

inline void DuckpgqScalarFun(DataChunk &args, ExpressionState &state,
                             Vector &result) {
  auto &name_vector = args.data[0];
  UnaryExecutor::Execute<string_t, string_t>(
      name_vector, result, args.size(), [&](string_t name) {
        return StringVector::AddString(result,
                                       "Duckpgq " + name.GetString() + " 🐥");
      });
}

static void LoadInternal(DatabaseInstance &instance) {
  auto &config = DBConfig::GetConfig(instance);
  DuckPGQParserExtension pgq_parser;
  config.parser_extensions.push_back(pgq_parser);
  config.operator_extensions.push_back(make_uniq<DuckPGQOperatorExtension>());

  Connection con(instance);
  con.BeginTransaction();

  auto &catalog = Catalog::GetSystemCatalog(*con.context);

  const PGQMatchFunction match_pg_function;
  CreateTableFunctionInfo match_pg_info(match_pg_function);
  catalog.CreateTableFunction(*con.context, match_pg_info);

  const CreatePropertyGraphFunction create_pg_function;
  CreateTableFunctionInfo create_pg_info(create_pg_function);
  catalog.CreateTableFunction(*con.context, create_pg_info);

  const DropPropertyGraphFunction drop_pg_function;
  CreateTableFunctionInfo drop_pg_info(drop_pg_function);
  catalog.CreateTableFunction(*con.context, drop_pg_info);

  for (auto &fun : DuckPGQFunctions::GetFunctions()) {
    catalog.CreateFunction(*con.context, fun);
  }

  for (auto &fun : DuckPGQFunctions::GetTableFunctions()) {
    catalog.CreateFunction(*con.context, fun);
  }

  CreateScalarFunctionInfo duckpgq_fun_info(
      ScalarFunction("duckpgq", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
                     DuckpgqScalarFun));
  duckpgq_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
  catalog.CreateFunction(*con.context, duckpgq_fun_info);
  con.Commit();
}

void DuckpgqExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }

ParserExtensionParseResult duckpgq_parse(ParserExtensionInfo *info,
                                         const std::string &query) {
  auto parse_info = reinterpret_cast<DuckPGQParserExtensionInfo &>(info);
  Parser parser;
  parser.ParseQuery((query[0] == '-') ? query.substr(1, query.length())
                                      : query);
  if (parser.statements.size() != 1) {
    throw ParserException(
        "More than 1 statement detected, please only give one.");
  }
  return {make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(
      std::move(parser.statements[0]))};
}

BoundStatement duckpgq_bind(ClientContext &context, Binder &binder,
                            OperatorExtensionInfo *info,
                            SQLStatement &statement) {
  auto lookup = context.registered_state.find("duckpgq");
  if (lookup != context.registered_state.end()) {
    auto duckpgq_state = (DuckPGQState *)lookup->second.get();
    auto duckpgq_binder = Binder::CreateBinder(context);
    auto duckpgq_parse_data =
        dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());
    if (duckpgq_parse_data) {
      return duckpgq_binder->Bind(*(duckpgq_parse_data->statement));
    }
  }
  throw BinderException("Registered state not found");
}

ParserExtensionPlanResult
duckpgq_plan(ParserExtensionInfo *, ClientContext &context,
             unique_ptr<ParserExtensionParseData> parse_data) {
  auto duckpgq_state_entry = context.registered_state.find("duckpgq");
  DuckPGQState *duckpgq_state;
  if (duckpgq_state_entry == context.registered_state.end()) {
    auto state = make_shared<DuckPGQState>(std::move(parse_data));
    context.registered_state["duckpgq"] = state;
    duckpgq_state = state.get();
  } else {
    duckpgq_state = (DuckPGQState *)duckpgq_state_entry->second.get();
    duckpgq_state->parse_data = std::move(parse_data);
  }
  auto duckpgq_parse_data =
      dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());

  if (!duckpgq_parse_data) {
    throw BinderException("Not DuckPGQ parse data");
  }
  auto statement =
      dynamic_cast<SQLStatement *>(duckpgq_parse_data->statement.get());
  if (statement->type == StatementType::SELECT_STATEMENT) {
    auto select_statement = dynamic_cast<SelectStatement *>(statement);
    auto select_node = dynamic_cast<SelectNode *>(select_statement->node.get());
    auto from_table_function =
        dynamic_cast<TableFunctionRef *>(select_node->from_table.get());
    if (!from_table_function) {
      throw Exception("Use duckpgq_bind instead");
    }
    auto function =
        dynamic_cast<FunctionExpression *>(from_table_function->function.get());
    if (function->function_name == "duckpgq_match") {
      duckpgq_state->transform_expression =
          std::move(std::move(function->children[0]));
      function->children.pop_back();
    }
    throw Exception("use duckpgq_bind instead");
  } else if (statement->type == StatementType::CREATE_STATEMENT) {
    ParserExtensionPlanResult result;
    result.function = CreatePropertyGraphFunction();
    result.requires_valid_transaction = true;
    result.return_type = StatementReturnType::QUERY_RESULT;
    return result;
  } else if (statement->type == StatementType::DROP_STATEMENT) {
    ParserExtensionPlanResult result;
    result.function = DropPropertyGraphFunction();
    result.requires_valid_transaction = true;
    result.return_type = StatementReturnType::QUERY_RESULT;
    return result;
  }
  throw BinderException("Unknown DuckPGQ query encountered");
}

std::string DuckpgqExtension::Name() { return "duckpgq"; }

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
