#define DUCKDB_EXTENSION_MAIN

#include "sqlpgq_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"


#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
    con.BeginTransaction();
    auto &catalog = Catalog::GetSystemCatalog(*con.context);
    for (auto &fun : SQLPGQFunctions::GetFunctions()) {
        catalog.CreateFunction(*con.context, &fun);
    }
    con.Commit();
}

void SqlpgqExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string SqlpgqExtension::Name() {
	return "sqlpgq";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void sqlpgq_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *sqlpgq_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
