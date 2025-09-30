//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckpgq/include/core/pragma/show_property_graphs.hpp
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckpgq/common.hpp"

namespace duckdb {

//! Class to register the PRAGMA create_inbox function
class CorePGQPragma {
public:
	//! Register the PRAGMA function
	static void Register(ExtensionLoader &loader) {
		RegisterShowPropertyGraphs(loader);
		RegisterCreateVertexTable(loader);
	}

private:
	static void RegisterShowPropertyGraphs(ExtensionLoader &loader);
	static void RegisterCreateVertexTable(ExtensionLoader &loader);
};

} // namespace duckdb
