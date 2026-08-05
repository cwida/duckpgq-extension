
#include "duckpgq/core/operator/logical_path_finding_operator.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/common.hpp"

namespace duckdb {

PhysicalOperator &LogicalPathFindingOperator::CreatePlan(ClientContext &context,
                                                         duckdb::PhysicalPlanGenerator &generator) {
	D_ASSERT(children.size() == 2);
	estimated_cardinality = children[0]->EstimateCardinality(context);
	auto &pairs = generator.CreatePlan(*children[0]);
	auto &csr = generator.CreatePlan(*children[1]);
	return generator.Make<PhysicalPathFinding>(*this, pairs, csr);
}

vector<ColumnBinding> LogicalPathFindingOperator::GetColumnBindings() {
	auto left_bindings = children[0]->GetColumnBindings();
	for (const auto &offset : offsets) {
		auto binding = ColumnBinding(table_index, ProjectionIndex(offset));
		left_bindings.push_back(binding);
	}
	return left_bindings;
}

void LogicalPathFindingOperator::ResolveTypes() {
	types = children[0]->types;
	if (mode == "iterativelength" || mode == "bidirectionaliterativelength") {
		types.push_back(LogicalType::BIGINT);
	} else if (mode == "shortestpath") {
		types.push_back(LogicalType::LIST(LogicalType::BIGINT));
	} else {
		throw NotImplementedException("Unrecognized mode in PathFindingOperator: " + mode);
	}
}

InsertionOrderPreservingMap<string> LogicalPathFindingOperator::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string expression_info;
	for (auto &expr : expressions) {
		expression_info += "\n";
		expression_info += expr->GetName();
	}
	result["Expressions"] = expression_info;
	return result;
}

} // namespace duckdb
