#pragma once

#include "duckpgq/common.hpp"

namespace duckpgq {
namespace core {

struct CorePGQOperator {
  static void Register(DatabaseInstance &db) { RegisterPGQBindOperator(db); }

private:
  static void RegisterPGQBindOperator(DatabaseInstance &db);
};

} // namespace core

} // namespace duckpgq
