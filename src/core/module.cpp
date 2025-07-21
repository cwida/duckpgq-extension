
#include "duckpgq/core/module.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/functions/scalar.hpp"
#include "duckpgq/core/functions/table.hpp"
#include "duckpgq/core/operator/duckpgq_operator.hpp"
#include "duckpgq/core/option/duckpgq_option.hpp"
#include "duckpgq/core/optimizer/duckpgq_optimizer.hpp"
#include "duckpgq/core/parser/duckpgq_parser.hpp"
#include "duckpgq/core/pragma/duckpgq_pragma.hpp"

namespace duckpgq {

namespace core {

void CoreModule::Register(DatabaseInstance &db) {
    CoreTableFunctions::Register(db);
    CoreScalarFunctions::Register(db);
    CorePGQOperator::Register(db);
    CorePGQOptions::Register(db);
    CorePGQOptimizer::Register(db);
    CorePGQParser::Register(db);
    CorePGQPragma::Register(db);
}

} // namespace core

} // namespace duckpgq