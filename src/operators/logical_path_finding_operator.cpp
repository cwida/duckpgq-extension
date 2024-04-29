
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
  left_bindings.push_back(ColumnBinding(10, 0));
  return left_bindings;
}

void LogicalPathFindingOperator::ResolveTypes() {
  types = children[0]->types;
  if (mode == "iterativelength") {
    types.push_back(LogicalType::BIGINT);
  } else if (mode == "shortestpath") {
    types.push_back(LogicalType::LIST(LogicalType::BIGINT));
  } else {
    throw NotImplementedException("Unrecognized mode in PathFindingOperator: " + mode);
  }
}

string LogicalPathFindingOperator::ParamsToString() const {
  string extra_info;
  for (auto &expr : expressions) {
    extra_info += "\n";
    extra_info += expr->ToString();
  }
  return extra_info;

}
} // namespace duckdb
