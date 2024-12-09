#include "duckpgq/core/operator/duckpgq_bind.hpp"
#include "duckpgq/common.hpp"

#include <duckpgq/core/parser/duckpgq_parser.hpp>
#include "duckpgq/core/operator/duckpgq_operator.hpp"
#include <duckpgq_state.hpp>

namespace duckpgq {

namespace core {

BoundStatement duckpgq_bind(ClientContext &context, Binder &binder,
                            OperatorExtensionInfo *info,
                            SQLStatement &statement) {
  auto duckpgq_state = context.registered_state->Get<DuckPGQState>("duckpgq");
  if (!duckpgq_state) {
    throw; // Throw the original error that got us here if DuckPGQ is not loaded
  }

  auto duckpgq_binder = Binder::CreateBinder(context, &binder);
  auto duckpgq_parse_data =
      dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());
  if (duckpgq_parse_data) {
    return duckpgq_binder->Bind(*(duckpgq_parse_data->statement));
  }
  throw;
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CorePGQOperator::RegisterPGQBindOperator(DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);
  config.operator_extensions.push_back(make_uniq<DuckPGQOperatorExtension>());
}

} // namespace core

} // namespace duckpgq
