#pragma once
#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

struct CoreTableFunctions {
  static void Register(DatabaseInstance &db) {
    RegisterCreatePropertyGraphTableFunction(db);
    RegisterMatchTableFunction(db);
    RegisterDropPropertyGraphTableFunction(db);
    RegisterDescribePropertyGraphTableFunction(db);
    RegisterLocalClusteringCoefficientTableFunction(db);
    RegisterScanTableFunctions(db);
  }

private:
  static void RegisterCreatePropertyGraphTableFunction(DatabaseInstance &db);
  static void RegisterMatchTableFunction(DatabaseInstance &db);
  static void RegisterDropPropertyGraphTableFunction(DatabaseInstance &db);
  static void RegisterDescribePropertyGraphTableFunction(DatabaseInstance &db);
  static void RegisterLocalClusteringCoefficientTableFunction(DatabaseInstance &db);
  static void RegisterScanTableFunctions(DatabaseInstance &db);
};


} // namespace core

} // namespace duckpgq
