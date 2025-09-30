#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

struct CorePGQOperator {
	static void Register(ExtensionLoader &loader) {
		RegisterPGQBindOperator(loader);
	}

private:
	static void RegisterPGQBindOperator(ExtensionLoader &loader);
};

} // namespace duckdb
