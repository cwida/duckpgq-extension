#pragma once
#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

struct CorePGQOptimizer {
  static void Register(DatabaseInstance &db) {
    RegisterPathFindingOptimizerRule(db);
  }

private:
  static void RegisterPathFindingOptimizerRule(DatabaseInstance &db);
};


} // namespace core
} // namespace duckpgq