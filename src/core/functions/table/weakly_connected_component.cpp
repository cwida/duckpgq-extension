#include "duckpgq/core/functions/table/weakly_connected_component.hpp"
#include "duckdb/function/table_function.hpp"

#include <duckpgq/core/functions/table.hpp>

namespace duckpgq {
namespace core {



//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterWeaklyConnectedComponentTableFunction(DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(db, WeaklyConnectedComponentFunction());
}

} // namespace core
} // namespace duckpgq