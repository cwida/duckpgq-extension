//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckpgq/include/core/pragma/show_property_graphs.hpp
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

//! Class to register the PRAGMA create_inbox function
class CorePGQPragma {
public:
  //! Register the PRAGMA function
  static void Register(DatabaseInstance &instance) {
    RegisterShowPropertyGraphs(instance);
  }

private:
  static void RegisterShowPropertyGraphs(DatabaseInstance &instance);
};

} // namespace core

} // namespace duckpgq