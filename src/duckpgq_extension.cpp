#define DUCKDB_EXTENSION_MAIN

#include "duckpgq_extension.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/module.hpp"


namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
  duckpgq::core::CoreModule::Register(instance);
  auto &config = DBConfig::GetConfig(instance);
  config.extension_callbacks.push_back(make_uniq<DuckpgqLoadExtension>());
}

void DuckpgqExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }

std::string DuckpgqExtension::Name() { return "duckpgq"; }

} // namespace duckpgq

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
