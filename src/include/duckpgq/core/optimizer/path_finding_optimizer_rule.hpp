#pragma once

#include "duckpgq/common.hpp"

namespace duckpgq {
namespace core {

class DuckpgqOptimizerExtension : public OptimizerExtension {
public:
  DuckpgqOptimizerExtension() {
    optimize_function = DuckpgqOptimizeFunction;
  }

  static bool InsertPathFindingOperator(LogicalOperator &op, ClientContext &context);

  static void DuckpgqOptimizeFunction(OptimizerExtensionInput &input,
                                     unique_ptr<LogicalOperator> &plan);
};

} // namespace core
} // namespace duckpgq