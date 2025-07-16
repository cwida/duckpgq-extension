#pragma once
#include "duckpgq/common.hpp"

namespace duckdb {



struct CoreModule {
public:
	static void Register(DatabaseInstance &db);
};



} // namespace duckdb
