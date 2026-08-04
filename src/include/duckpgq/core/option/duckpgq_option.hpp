#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

bool GetPathFindingOption(ClientContext &context);
int32_t GetPathFindingTaskSize(ClientContext &context);
int32_t GetLightPartitionMultiplier(ClientContext &context);
double_t GetHeavyPartitionFraction(ClientContext &context);

struct CorePGQOptions {
  static void Register(ExtensionLoader &loader) {
    RegisterExperimentalPathFindingOperator(loader);
    RegisterPathFindingTaskSize(loader);
    RegisterPathFindingLightPartitionMultiplier(loader);
    RegisterPathFindingHeavyPartitionFraction(loader);
  }

private:
  static void RegisterExperimentalPathFindingOperator(ExtensionLoader &loader);
  static void RegisterPathFindingTaskSize(ExtensionLoader &loader);
  static void RegisterPathFindingLightPartitionMultiplier(ExtensionLoader &loader);
  static void RegisterPathFindingHeavyPartitionFraction(ExtensionLoader &loader);
};;

} // namespace duckdb
