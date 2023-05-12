#define DUCKDB_EXTENSION_MAIN

#include "duckpgq_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"


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

    DuckPGQParser pgq_parser(instance);
    config.parser_extensions.push_back(pgq_parser);

	Connection con(instance);
    con.BeginTransaction();

    auto &catalog = Catalog::GetSystemCatalog(*con.context);

    CreateScalarFunctionInfo duckpgq_fun_info(
            ScalarFunction("duckpgq", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DuckpgqScalarFun));
    duckpgq_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    catalog.CreateFunction(*con.context, &duckpgq_fun_info);
    con.Commit();
}

void DuckpgqExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
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
