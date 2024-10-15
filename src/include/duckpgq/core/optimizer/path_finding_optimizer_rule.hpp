#pragma once

#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>

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

  static unique_ptr<LogicalPathFindingOperator> FindCSRAndPairs(
      unique_ptr<LogicalOperator>& first_child,
      unique_ptr<LogicalOperator>& second_child,
      LogicalProjection& op_proj);
};

} // namespace core
} // namespace duckpgq