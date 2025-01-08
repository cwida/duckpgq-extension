#pragma once

#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

bool GetPathFindingOption(ClientContext &context);
int32_t GetPathFindingTaskSize(ClientContext &context);

struct CorePGQOptions {
  static void Register(DatabaseInstance &db) {
    RegisterExperimentalPathFindingOperator(db);
    RegisterPathFindingTaskSize(db);
  }

private:
  static void RegisterExperimentalPathFindingOperator(DatabaseInstance &db);
  static void RegisterPathFindingTaskSize(DatabaseInstance &db);
};;

} // namespace core

} // namespace duckpgq
