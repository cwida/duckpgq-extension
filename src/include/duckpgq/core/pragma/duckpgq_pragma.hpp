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
	static void Register(DatabaseInstance &instance) {
		RegisterShowPropertyGraphs(instance);
		RegisterCreateVertexTable(instance);
	}

private:
	static void RegisterShowPropertyGraphs(DatabaseInstance &instance);
	static void RegisterCreateVertexTable(DatabaseInstance &instance);
};

} // namespace duckdb
