#include "duckpgq/core/option/duckpgq_option.hpp"
#include "duckpgq/common.hpp"

namespace duckpgq {
namespace core {


//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterExperimentalPathFindingOperator(
    DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);
  config.AddExtensionOption("experimental_path_finding_operator",
  "Enables the experimental path finding operator to be triggered",
  LogicalType::BOOLEAN, Value(false));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingTaskSize(
    DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);

  config.AddExtensionOption("experimental_path_finding_operator_task_size",
    "Number of vertices processed per thread at a time", LogicalType::INTEGER, Value(256));
}



} // namespace core
} // namespace duckpgq

