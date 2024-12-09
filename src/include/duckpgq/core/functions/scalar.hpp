#pragma once
#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

struct CoreScalarFunctions {
  static void Register(DatabaseInstance &db) {
    RegisterCheapestPathLengthScalarFunction(db);
    RegisterCSRCreationScalarFunctions(db);
    RegisterCSRDeletionScalarFunction(db);
    RegisterGetCSRWTypeScalarFunction(db);
    RegisterIterativeLengthScalarFunction(db);
    RegisterIterativeLength2ScalarFunction(db);
    RegisterIterativeLengthBidirectionalScalarFunction(db);
    RegisterLocalClusteringCoefficientScalarFunction(db);
    RegisterReachabilityScalarFunction(db);
    RegisterShortestPathScalarFunction(db);
    RegisterWeaklyConnectedComponentScalarFunction(db);
    RegisterPageRankScalarFunction(db);
  }

private:
  static void RegisterCheapestPathLengthScalarFunction(DatabaseInstance &db);
  static void RegisterCSRCreationScalarFunctions(DatabaseInstance &db);
  static void RegisterCSRDeletionScalarFunction(DatabaseInstance &db);
  static void RegisterGetCSRWTypeScalarFunction(DatabaseInstance &db);
  static void RegisterIterativeLengthScalarFunction(DatabaseInstance &db);
  static void RegisterIterativeLength2ScalarFunction(DatabaseInstance &db);
  static void
  RegisterIterativeLengthBidirectionalScalarFunction(DatabaseInstance &db);
  static void
  RegisterLocalClusteringCoefficientScalarFunction(DatabaseInstance &db);
  static void RegisterReachabilityScalarFunction(DatabaseInstance &db);
  static void RegisterShortestPathScalarFunction(DatabaseInstance &db);
  static void
  RegisterWeaklyConnectedComponentScalarFunction(DatabaseInstance &db);
  static void RegisterPageRankScalarFunction(DatabaseInstance &db);
};

} // namespace core

} // namespace duckpgq
