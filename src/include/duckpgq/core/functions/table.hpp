#pragma once
#include "duckpgq/common.hpp"

namespace duckdb {

struct CoreTableFunctions {
	static void Register(ExtensionLoader &loader) {
		RegisterCreatePropertyGraphTableFunction(loader);
		RegisterMatchTableFunction(loader);
		RegisterDropPropertyGraphTableFunction(loader);
		RegisterDescribePropertyGraphTableFunction(loader);
		RegisterLocalClusteringCoefficientTableFunction(loader);
		RegisterScanTableFunctions(loader);
		RegisterSummarizePropertyGraphTableFunction(loader);
		RegisterWeaklyConnectedComponentTableFunction(loader);
		RegisterPageRankTableFunction(loader);
	}

private:
	static void RegisterCreatePropertyGraphTableFunction(ExtensionLoader &loader);
	static void RegisterMatchTableFunction(ExtensionLoader &loader);
	static void RegisterDropPropertyGraphTableFunction(ExtensionLoader &loader);
	static void RegisterDescribePropertyGraphTableFunction(ExtensionLoader &loader);
	static void RegisterLocalClusteringCoefficientTableFunction(ExtensionLoader &loader);
	static void RegisterScanTableFunctions(ExtensionLoader &loader);
	static void RegisterWeaklyConnectedComponentTableFunction(ExtensionLoader &loader);
	static void RegisterPageRankTableFunction(ExtensionLoader &loader);
	static void RegisterSummarizePropertyGraphTableFunction(ExtensionLoader &loader);
};

} // namespace duckdb
