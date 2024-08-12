
#include "duckpgq/core/module.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/functions/table.hpp"
#include "duckpgq/core/functions/scalar.hpp"
#include "duckpgq/core/parser/duckpgq_parser.hpp"
#include "duckpgq/core/operator/duckpgq_operator.hpp"

namespace duckpgq {

namespace core {

void CoreModule::Register(DatabaseInstance &db) {
    CoreTableFunctions::Register(db);
    CoreScalarFunctions::Register(db);
    CorePGQParser::Register(db);
    CorePGQOperator::Register(db);
}


} // namespace core

} // namespace duckpgq