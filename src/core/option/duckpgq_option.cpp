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

int32_t GetLightPartitionMultiplier(ClientContext &context) {
  Value value;
  context.TryGetCurrentSetting("experimental_path_finding_operator_light_partition_multiplier", value);
  return value.GetValue<int32_t>();
}

double_t GetHeavyPartitionFraction(ClientContext &context) {
  Value value;
  context.TryGetCurrentSetting("experimental_path_finding_operator_heavy_partition_fraction", value);
  return value.GetValue<double_t>();
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
void CorePGQOptions::RegisterPathFindingLightPartitionMultiplier(
    DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);

  config.AddExtensionOption("experimental_path_finding_operator_light_partition_multiplier",
    "Multiplier used for the light partitions of the local CSR partitioning", LogicalType::INTEGER, Value(1));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingHeavyPartitionFraction(
    DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);

  config.AddExtensionOption("experimental_path_finding_operator_heavy_partition_fraction",
    "Fraction of edges part of the heavy partitions for the local CSR partitioning", LogicalType::DOUBLE, Value(0.75));
}

} // namespace core
} // namespace duckpgq

