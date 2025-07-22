#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

struct CorePGQOperator {
	static void Register(DatabaseInstance &db) {
		RegisterPGQBindOperator(db);
	}

private:
	static void RegisterPGQBindOperator(DatabaseInstance &db);
};

} // namespace duckdb
