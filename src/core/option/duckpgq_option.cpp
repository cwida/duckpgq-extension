#include "duckpgq/core/option/duckpgq_option.hpp"
#include "duckpgq/common.hpp"

namespace duckpgq {
namespace core {

bool GetPathFindingOption(ClientContext &context) {
  Value value;
  context.TryGetCurrentSetting("experimental_path_finding_operator", value);
  return value.GetValue<bool>();
}

int32_t GetPathFindingTaskSize(ClientContext &context) {
  Value value;
  context.TryGetCurrentSetting("experimental_path_finding_operator_task_size", value);
  return value.GetValue<int32_t>();
}

int32_t GetPartitionMultiplier(ClientContext &context) {
  Value value;
  context.TryGetCurrentSetting("experimental_path_finding_operator_partition_multiplier", value);
  return value.GetValue<int32_t>();
}

int32_t GetPartitionSize(ClientContext &context) {
  Value value;
  context.TryGetCurrentSetting("experimental_path_finding_operator_partition_size", value);
  return value.GetValue<int32_t>();
}

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

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingPartitionMultiplier(
    DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);

  config.AddExtensionOption("experimental_path_finding_operator_partition_multiplier",
    "Multiplier used for local CSR partitioning", LogicalType::INTEGER, Value(4));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingPartitionSize(
    DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);

  config.AddExtensionOption("experimental_path_finding_operator_partition_size",
    "Number of edges for local CSR partitioning", LogicalType::INTEGER, Value(524288));
}

} // namespace core
} // namespace duckpgq

