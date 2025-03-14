#pragma once

#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

bool GetPathFindingOption(ClientContext &context);
int32_t GetPathFindingTaskSize(ClientContext &context);
int32_t GetPartitionMultiplier(ClientContext &context);
int32_t GetPartitionSize(ClientContext &context);

struct CorePGQOptions {
  static void Register(DatabaseInstance &db) {
    RegisterExperimentalPathFindingOperator(db);
    RegisterPathFindingTaskSize(db);
    RegisterPathFindingPartitionMultiplier(db);
    RegisterPathFindingPartitionSize(db);
  }

private:
  static void RegisterExperimentalPathFindingOperator(DatabaseInstance &db);
  static void RegisterPathFindingTaskSize(DatabaseInstance &db);
  static void RegisterPathFindingPartitionMultiplier(DatabaseInstance &db);
  static void RegisterPathFindingPartitionSize(DatabaseInstance &db);
};;

} // namespace core

} // namespace duckpgq
