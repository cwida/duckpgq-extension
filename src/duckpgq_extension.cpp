#define DUCKDB_EXTENSION_MAIN

#include "duckpgq_extension.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/module.hpp"


namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
  duckpgq::core::CoreModule::Register(instance);
}

void DuckpgqExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }

std::string DuckpgqExtension::Name() { return "duckpgq"; }

} // namespace duckpgq

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
