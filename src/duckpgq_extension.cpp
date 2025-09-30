#define DUCKDB_EXTENSION_MAIN

#include "duckpgq_extension.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/module.hpp"
#include <duckpgq_extension_callback.hpp>
#include "duckdb/main/connection_manager.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	CoreModule::Register(loader);
}

void DuckpgqExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DuckpgqExtension::Name() {
	return "duckpgq";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duckpgq, loader) {
	duckdb::LoadInternal(loader);
}
}
