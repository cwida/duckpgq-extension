#pragma once
#include "duckpgq/common.hpp"

namespace duckdb {

struct CorePGQOptimizer {
	static void Register(ExtensionLoader &loader) {
		RegisterPathFindingOptimizerRule(loader);
	}

private:
	static void RegisterPathFindingOptimizerRule(ExtensionLoader &loader);
};

} // namespace duckdb
