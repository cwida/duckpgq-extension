
#include "duckpgq/functions/module.hpp"



namespace duckpgq {

namespace core {

void CoreModule::Register(DatabaseInstance &db) {
  CoreTableFunctions::Register(db);
  CoreScalarFunctions::Register(db);
}


} // namespace core

} // namespace duckpgq