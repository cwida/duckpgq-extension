#include "duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp"

namespace duckpgq {

namespace core {

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterShortestPathScalarFunction(
    DatabaseInstance &db) {

  ExtensionUtil::RegisterFunction(
    db, GetShortestPathFunction());
}


} // namespace core

} // namespace duckpgq