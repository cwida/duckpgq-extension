
#include "duckpgq/core/operator/logical_path_finding_operator.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

unique_ptr<PhysicalOperator> LogicalPathFindingOperator::CreatePlan(
    ClientContext &context, duckdb::PhysicalPlanGenerator &generator) {
  D_ASSERT(children.size() == 3);
  estimated_cardinality = children[0]->EstimateCardinality(context);
  auto csr_v = generator.CreatePlan(std::move(children[0]));
  auto csr_e = generator.CreatePlan(std::move(children[1]));
  auto pairs = generator.CreatePlan(std::move(children[2]));
  return make_uniq<PhysicalPathFinding>(*this, std::move(csr_v),
                                        std::move(csr_e), std::move(pairs));
}

vector<ColumnBinding> LogicalPathFindingOperator::GetColumnBindings() {
  auto left_bindings = children[0]->GetColumnBindings();
  for (const auto &offset : offsets) {
    auto binding = ColumnBinding(table_index, offset);
    left_bindings.push_back(binding);
  }
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

InsertionOrderPreservingMap<string>  LogicalPathFindingOperator::ParamsToString() const {
  InsertionOrderPreservingMap<string> result;
  string expression_info;
  for (auto &expr : expressions) {
    expression_info += "\n";
    expression_info += expr->GetName();
  }
  result["Expressions"] = expression_info;
  return result;

}

} // namespace core
} // namespace duckdb
