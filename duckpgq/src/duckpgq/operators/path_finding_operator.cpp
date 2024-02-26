
#include "duckpgq/operators/path_finding_operator.hpp"
#include <duckpgq/operators/physical_path_finding.hpp>
#include <duckpgq_extension.hpp>

namespace duckdb {
unique_ptr<PhysicalOperator>
PathFindingOperator::CreatePlan(ClientContext &,
                                duckdb::PhysicalPlanGenerator &generator) {
   D_ASSERT(children.size() == 2);
   auto left = generator.CreatePlan(std::move(children[0]));
   auto right = generator.CreatePlan(std::move(children[1]));
   return make_uniq<PhysicalPathFinding>(*this, std::move(left), std::move(right));
}
vector<ColumnBinding> PathFindingOperator::GetColumnBindings() {
  return LogicalOperator::GetColumnBindings();
}
} // namespace duckdb
