#define DUCKDB_EXTENSION_MAIN

#include "duckpgq_extension.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/module.hpp"
#include <duckpgq_extension_callback.hpp>
#include "duckdb/main/connection_manager.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	duckpgq::core::CoreModule::Register(instance);
	// for (auto &connection :
	//      ConnectionManager::Get(instance).GetConnectionList()) {
	//   connection->registered_state->Insert(
	//       "duckpgq", make_shared_ptr<DuckPGQState>());
	// }

	// Fill in extension load information.
	std::string description = StringUtil::Format("Adds support for SQL/PGQ and graph algorithms.");
	ExtensionUtil::RegisterExtension(instance, /*name=*/"duckpgq", ExtensionLoadedInfo {std::move(description)});
}

void DuckpgqExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string DuckpgqExtension::Name() {
	return "duckpgq";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void duckpgq_init(DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *duckpgq_version() {
	return DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
