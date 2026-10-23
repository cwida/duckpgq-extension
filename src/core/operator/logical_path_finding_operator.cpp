
#include "duckpgq/core/functions/scalar.hpp"
#include "duckpgq/core/operator/logical_path_finding_operator.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/common.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"

namespace duckdb {

PhysicalOperator &LogicalPathFindingOperator::CreatePlan(ClientContext &context,
                                                         duckdb::PhysicalPlanGenerator &generator) {
	D_ASSERT(children.size() == 1 || children.size() == 2 || children.size() == 3);
	estimated_cardinality = children[0]->EstimateCardinality(context);
	auto &pairs = generator.CreatePlan(*children[0]);
	if (cached_partitioned_csr_input) {
		D_ASSERT(children.size() == 1);
		return generator.Make<PhysicalPathFinding>(*this, pairs, nullptr, nullptr);
	}
	if (children.size() == 3) {
		auto &counts = generator.CreatePlan(*children[1]);
		auto &edges = generator.CreatePlan(*children[2]);
		return generator.Make<PhysicalPathFinding>(*this, pairs, &edges, &counts);
	}
	auto &csr = generator.CreatePlan(*children[1]);
	return generator.Make<PhysicalPathFinding>(*this, pairs, &csr, nullptr);
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
	if (mode == "iterativelength" || mode == "pushpulliterativelength" || mode == "bidirectionaliterativelength") {
		types.push_back(LogicalType::BIGINT);
	} else if (mode == "shortestpath") {
		types.push_back(LogicalType::LIST(LogicalType::BIGINT));
	} else if (mode == "reach") {
		types.push_back(ReachOperatorResultType());
	} else {
		throw NotImplementedException("Unrecognized mode in PathFindingOperator: " + mode);
	}
}

void LogicalPathFindingOperator::ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) {
	// The pair input is visited last: the resolver keeps the types of the operator it visited last and checks
	// them against the bindings when it resolves the arguments below
	for (idx_t child_idx = children.size(); child_idx > 1; child_idx--) {
		res.VisitOperator(*children[child_idx - 1]);
	}
	res.VisitOperator(*children[0]);
	// The source and destination arguments of the marker function are evaluated over the pair input (children[0]);
	// the remaining arguments (edges, counts, cache key) are planning-only and are not resolved
	bindings = children[0]->GetColumnBindings();
	auto &function = PhysicalPathFinding::PathFindingFunction(*expressions[0]);
	auto &arguments = function.GetChildrenMutable();
	if (arguments.size() < 2) {
		throw InternalException("Path-finding function needs source and destination arguments");
	}
	res.VisitExpression(&arguments[0]);
	res.VisitExpression(&arguments[1]);
	bindings = GetColumnBindings();
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
