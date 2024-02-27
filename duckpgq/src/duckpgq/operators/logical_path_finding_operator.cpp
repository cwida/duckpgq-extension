
#include "duckpgq/operators/logical_path_finding_operator.hpp"
#include <duckpgq/operators/physical_path_finding_operator.hpp>
#include <duckpgq_extension.hpp>

namespace duckdb {
unique_ptr<PhysicalOperator> LogicalPathFindingOperator::CreatePlan(
    ClientContext &, duckdb::PhysicalPlanGenerator &generator) {
  D_ASSERT(children.size() == 2);
  auto left = generator.CreatePlan(std::move(children[0]));
  auto right = generator.CreatePlan(std::move(children[1]));
  return make_uniq<PhysicalPathFinding>(*this, std::move(left),
                                        std::move(right));
}
vector<ColumnBinding> LogicalPathFindingOperator::GetColumnBindings() {
  auto left_bindings = children[0]->GetColumnBindings();
  auto right_bindings = children[1]->GetColumnBindings();
  left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
  return left_bindings;
}

void LogicalPathFindingOperator::ResolveTypes() {
  types = children[0]->types;
  auto right_types = children[1]->types;
  types.insert(types.end(), right_types.begin(), right_types.end());
}
} // namespace duckdb
