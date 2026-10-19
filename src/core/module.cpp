
#include "duckpgq/core/module.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/functions/scalar.hpp"
#include "duckpgq/core/functions/table.hpp"
#include "duckpgq/core/index/csr_index.hpp"
#include "duckpgq/core/optimizer/duckpgq_optimizer.hpp"
#include "duckpgq/core/optimizer/factorized_join_optimizer.hpp"
#include "duckpgq/core/option/duckpgq_option.hpp"
#include "duckpgq/core/parser/duckpgq_parser.hpp"
#include "duckpgq/core/pragma/duckpgq_pragma.hpp"

namespace duckdb {

void CoreModule::Register(ExtensionLoader &loader) {
	CoreTableFunctions::Register(loader);
	CoreScalarFunctions::Register(loader);
	CorePGQParser::Register(loader);
	CorePGQPragma::Register(loader);
	CorePGQOptions::Register(loader);
	CorePGQOptimizer::Register(loader);
	CoreOptimizer::Register(loader);
	CorePGQIndex::Register(loader);
}

} // namespace duckdb
