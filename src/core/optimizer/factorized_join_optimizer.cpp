#include "duckpgq/core/optimizer/factorized_join_optimizer.hpp"

#include "duckpgq/core/operator/logical_factorized_group_join.hpp"
#include "duckpgq/core/operator/physical_factorized_group_join.hpp"

#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/column_lifetime_analyzer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

static void CollectColumnBindings(const Expression &expr, column_binding_set_t &result) {
	vector<ColumnBinding> bindings;
	ColumnLifetimeAnalyzer::ExtractColumnBindings(expr, bindings);
	result.insert(bindings.begin(), bindings.end());
}

static column_binding_set_t ToBindingSet(const vector<ColumnBinding> &bindings) {
	column_binding_set_t result;
	for (auto &binding : bindings) {
		result.insert(binding);
	}
	return result;
}

static bool IsSubset(const column_binding_set_t &subset, const column_binding_set_t &superset) {
	for (auto &binding : subset) {
		if (superset.find(binding) == superset.end()) {
			return false;
		}
	}
	return true;
}

static idx_t FindBinding(const vector<ColumnBinding> &bindings, const ColumnBinding &binding) {
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (bindings[i] == binding) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

//! Projection maps select child columns by position, so they have to be rebuilt by binding whenever the child's
//! output layout changes. Bindings that the child no longer produces are dropped; nothing above needs them.
class ProjectionMapRemapper {
public:
	ProjectionMapRemapper(vector<ProjectionIndex> &projection_map, LogicalOperator &child)
	    : projection_map(projection_map) {
		selected = LogicalOperator::MapBindings(child.GetColumnBindings(), projection_map);
	}

	void Rebuild(LogicalOperator &child, const vector<FactorizedMultiplicity> &multiplicities) {
		auto child_bindings = child.GetColumnBindings();
		vector<ProjectionIndex> result;
		for (auto &binding : selected) {
			auto index = FindBinding(child_bindings, binding);
			if (index != DConstants::INVALID_INDEX) {
				result.push_back(ProjectionIndex(index));
			}
		}
		for (auto &multiplicity : multiplicities) {
			auto index = FindBinding(child_bindings, multiplicity.binding);
			if (index == DConstants::INVALID_INDEX) {
				throw InternalException("FactorizedJoinOptimizer: multiplicity column is not produced by the child");
			}
			result.push_back(ProjectionIndex(index));
		}
		// An empty map stands for every column
		bool identity = result.size() == child_bindings.size();
		for (idx_t i = 0; identity && i < result.size(); i++) {
			identity = result[i].GetIndex() == i;
		}
		if (identity || result.empty()) {
			projection_map.clear();
		} else {
			projection_map = std::move(result);
		}
	}

private:
	vector<ProjectionIndex> &projection_map;
	vector<ColumnBinding> selected;
};

static unique_ptr<Expression> ColumnRef(const FactorizedMultiplicity &multiplicity) {
	return make_uniq<BoundColumnRefExpression>(multiplicity.type, multiplicity.binding);
}

static bool IsFloating(const LogicalType &type) {
	return type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE;
}

//===--------------------------------------------------------------------===//
// Entry point
//===--------------------------------------------------------------------===//

FactorizedJoinOptimizer::FactorizedJoinOptimizer(ClientContext &context_p, Optimizer &optimizer_p)
    : context(context_p), optimizer(optimizer_p) {
}

void FactorizedJoinOptimizer::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	Value enabled;
	if (input.context.TryGetCurrentSetting(Identifier(SETTING_NAME), enabled) && !BooleanValue::Get(enabled)) {
		return;
	}
	FactorizedJoinOptimizer factorizer(input.context, input.optimizer);
	Value use_operator;
	if (input.context.TryGetCurrentSetting(Identifier(OPERATOR_SETTING_NAME), use_operator)) {
		factorizer.use_operator = BooleanValue::Get(use_operator);
	}
	Value use_directory;
	if (input.context.TryGetCurrentSetting(Identifier(DIRECTORY_SETTING_NAME), use_directory)) {
		factorizer.use_directory = BooleanValue::Get(use_directory);
	}
	factorizer.Run(plan);
}

void FactorizedJoinOptimizer::Run(unique_ptr<LogicalOperator> &plan) {
	root = &plan;
	VisitOperator(plan);
	if (changed) {
		plan->ResolveOperatorTypes();
	}
}

void FactorizedJoinOptimizer::VisitOperator(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY && TryFactorizeAggregate(op)) {
		return;
	}
	for (auto &child : op->children) {
		VisitOperator(child);
	}
}

//===--------------------------------------------------------------------===//
// Aggregate
//===--------------------------------------------------------------------===//

bool FactorizedJoinOptimizer::GetAggregateKind(const BoundAggregateExpression &aggregate,
                                               FactorizedAggregateKind &kind) {
	if (aggregate.IsDistinct() || aggregate.GetFilter() || aggregate.GetOrderBys() ||
	    aggregate.StateExportMode() != AggregateStateExportMode::NONE) {
		return false;
	}
	auto &name = aggregate.Function().GetName();
	auto &children = aggregate.GetChildren();
	if (name == "count_star" && children.empty()) {
		kind = FactorizedAggregateKind::COUNT_STAR;
		return true;
	}
	if (children.size() != 1) {
		return false;
	}
	auto &input_type = children[0]->GetReturnType();
	if (name == "count") {
		kind = FactorizedAggregateKind::COUNT;
		return true;
	}
	if (name == "sum" && (FactorizedKeyFitsInBigint(input_type) || IsFloating(input_type))) {
		kind = FactorizedAggregateKind::SUM;
		return true;
	}
	if (name == "min" || name == "max") {
		kind = FactorizedAggregateKind::MIN_MAX;
		return true;
	}
	return false;
}

bool FactorizedJoinOptimizer::TryFactorizeAggregate(unique_ptr<LogicalOperator> &op) {
	auto &aggr = op->Cast<LogicalAggregate>();
	if (aggr.children.size() != 1 || aggr.grouping_sets.size() > 1 || !aggr.grouping_functions.empty()) {
		return false;
	}
	vector<FactorizedAggregateKind> kinds;
	for (auto &expr : aggr.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		FactorizedAggregateKind kind;
		if (!GetAggregateKind(expr->Cast<BoundAggregateExpression>(), kind)) {
			return false;
		}
		kinds.push_back(kind);
	}

	// Everything the aggregate reads has to survive the rewrite under its current binding
	column_binding_set_t required;
	for (auto &group : aggr.groups) {
		CollectColumnBindings(*group, required);
	}
	for (auto &expr : aggr.expressions) {
		CollectColumnBindings(*expr, required);
	}

	auto multiplicities = Factorize(aggr.children[0], required);
	if (multiplicities.empty()) {
		return true;
	}
	auto product = MultiplicityProduct(multiplicities);

	// Correct the aggregates and restore their original types in a projection on top
	auto old_bindings = aggr.GetColumnBindings();
	vector<unique_ptr<Expression>> projection_list;
	for (idx_t i = 0; i < aggr.groups.size(); i++) {
		projection_list.push_back(
		    make_uniq<BoundColumnRefExpression>(aggr.groups[i]->GetReturnType(), old_bindings[i]));
	}
	bool needs_projection = false;
	bool ungrouped = aggr.groups.empty();
	for (idx_t i = 0; i < aggr.expressions.size(); i++) {
		auto &old_aggregate = aggr.expressions[i]->Cast<BoundAggregateExpression>();
		auto old_type = old_aggregate.GetReturnType();
		auto replacement = RewriteAggregate(old_aggregate, kinds[i], *product);
		if (replacement) {
			aggr.expressions[i] = std::move(replacement);
		}
		auto &new_type = aggr.expressions[i]->GetReturnType();
		unique_ptr<Expression> ref =
		    make_uniq<BoundColumnRefExpression>(new_type, ColumnBinding(aggr.aggregate_index, ProjectionIndex(i)));
		bool is_count = kinds[i] == FactorizedAggregateKind::COUNT_STAR || kinds[i] == FactorizedAggregateKind::COUNT;
		if (ungrouped && is_count) {
			// A count over no rows is 0, a sum over no rows is NULL
			auto coalesce = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, new_type);
			coalesce->GetChildrenMutable().push_back(std::move(ref));
			coalesce->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value::Numeric(new_type, 0)));
			ref = std::move(coalesce);
			needs_projection = true;
		}
		if (ref->GetReturnType() != old_type) {
			ref = BoundCastExpression::AddCastToType(context, std::move(ref), old_type);
			needs_projection = true;
		}
		projection_list.push_back(std::move(ref));
	}
	aggr.ResolveOperatorTypes();
	changed = true;

	if (!needs_projection) {
		return true;
	}
	auto table_index = optimizer.binder.GenerateTableIndex();
	auto projection = make_uniq<LogicalProjection>(table_index, std::move(projection_list));
	projection->estimated_cardinality = aggr.estimated_cardinality;
	projection->has_estimated_cardinality = aggr.has_estimated_cardinality;
	projection->children.push_back(std::move(op));
	projection->ResolveOperatorTypes();

	CorrelatedColumnBindingReplacer replacer;
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(old_bindings[i], ColumnBinding(table_index, ProjectionIndex(i)));
	}
	replacer.stop_operator = *projection;
	op = std::move(projection);
	replacer.VisitOperator(**root);
	return true;
}

unique_ptr<Expression> FactorizedJoinOptimizer::RewriteAggregate(const BoundAggregateExpression &aggregate,
                                                                 FactorizedAggregateKind kind,
                                                                 const Expression &product) {
	vector<unique_ptr<Expression>> children;
	switch (kind) {
	case FactorizedAggregateKind::COUNT_STAR:
		children.push_back(product.Copy());
		break;
	case FactorizedAggregateKind::COUNT: {
		// count(x) counts the rows where x is not NULL
		auto is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		is_null->GetChildrenMutable().push_back(aggregate.GetChildren()[0]->Copy());
		children.push_back(make_uniq<BoundCaseExpression>(
		    std::move(is_null), make_uniq<BoundConstantExpression>(Value::BIGINT(0)), product.Copy()));
		break;
	}
	case FactorizedAggregateKind::SUM: {
		// sum(x) over the flattened rows is sum(x * multiplicity) over the factorized ones
		auto input = aggregate.GetChildren()[0]->Copy();
		auto arithmetic_type = IsFloating(input->GetReturnType()) ? LogicalType::DOUBLE : LogicalType::HUGEINT;
		input = BoundCastExpression::AddCastToType(context, std::move(input), arithmetic_type);
		auto multiplicity = BoundCastExpression::AddCastToType(context, product.Copy(), arithmetic_type);
		children.push_back(Multiply(std::move(input), std::move(multiplicity)));
		break;
	}
	case FactorizedAggregateKind::MIN_MAX:
		// Every distinct input value is still present once, so min and max are unchanged
		return nullptr;
	}
	auto result = BindAggregate("sum", std::move(children));
	if (!result) {
		throw InternalException("FactorizedJoinOptimizer: could not bind sum over the multiplicity");
	}
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Factorization of the subtree below the aggregate
//===--------------------------------------------------------------------===//

vector<FactorizedMultiplicity> FactorizedJoinOptimizer::Factorize(unique_ptr<LogicalOperator> &op,
                                                                  const column_binding_set_t &required) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return FactorizeJoin(op, required);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return FactorizeProjection(op, required);
	case LogicalOperatorType::LOGICAL_FILTER:
		return FactorizeFilter(op, required);
	default:
		return VisitChildren(op);
	}
}

vector<FactorizedMultiplicity> FactorizedJoinOptimizer::VisitChildren(unique_ptr<LogicalOperator> &op) {
	// Not factorized itself; it may still be, or contain, an aggregate that can be
	VisitOperator(op);
	return {};
}

vector<FactorizedMultiplicity> FactorizedJoinOptimizer::FactorizeProjection(unique_ptr<LogicalOperator> &op,
                                                                            const column_binding_set_t &required) {
	auto &projection = op->Cast<LogicalProjection>();
	for (auto &binding : required) {
		if (binding.table_index != projection.table_index || binding.column_index >= projection.expressions.size()) {
			return VisitChildren(op);
		}
	}
	// Every expression is evaluated, whether or not the parent uses it
	column_binding_set_t child_required;
	for (auto &expr : projection.expressions) {
		CollectColumnBindings(*expr, child_required);
	}
	auto inner = Factorize(projection.children[0], child_required);
	vector<FactorizedMultiplicity> result;
	for (auto &multiplicity : inner) {
		projection.expressions.push_back(ColumnRef(multiplicity));
		result.push_back(FactorizedMultiplicity {
		    ColumnBinding(projection.table_index, ProjectionIndex(projection.expressions.size() - 1)),
		    multiplicity.type});
	}
	return result;
}

vector<FactorizedMultiplicity> FactorizedJoinOptimizer::FactorizeFilter(unique_ptr<LogicalOperator> &op,
                                                                        const column_binding_set_t &required) {
	auto &filter = op->Cast<LogicalFilter>();
	column_binding_set_t child_required = required;
	for (auto &expr : filter.expressions) {
		CollectColumnBindings(*expr, child_required);
	}
	ProjectionMapRemapper remapper(filter.projection_map, *filter.children[0]);
	auto inner = Factorize(filter.children[0], child_required);
	remapper.Rebuild(*filter.children[0], inner);
	return inner;
}

vector<FactorizedMultiplicity> FactorizedJoinOptimizer::FactorizeJoin(unique_ptr<LogicalOperator> &op,
                                                                      const column_binding_set_t &required) {
	auto &join = op->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::INNER || join.conditions.empty() || !join.duplicate_eliminated_columns.empty()) {
		return VisitChildren(op);
	}
	column_binding_set_t side_bindings[2];
	column_binding_set_t key_bindings[2];
	for (idx_t side = 0; side < 2; side++) {
		side_bindings[side] = ToBindingSet(join.children[side]->GetColumnBindings());
	}
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
			return VisitChildren(op);
		}
		column_binding_set_t lhs;
		column_binding_set_t rhs;
		CollectColumnBindings(condition.GetLHS(), lhs);
		CollectColumnBindings(condition.GetRHS(), rhs);
		if (lhs.empty() || rhs.empty() || !IsSubset(lhs, side_bindings[0]) || !IsSubset(rhs, side_bindings[1])) {
			return VisitChildren(op);
		}
		key_bindings[0].insert(lhs.begin(), lhs.end());
		key_bindings[1].insert(rhs.begin(), rhs.end());
	}
	column_binding_set_t required_side[2];
	for (auto &binding : required) {
		if (side_bindings[0].count(binding)) {
			required_side[0].insert(binding);
		} else if (side_bindings[1].count(binding)) {
			required_side[1].insert(binding);
		} else {
			return VisitChildren(op);
		}
	}

	// Decide for both sides before either of them is modified: the estimates depend on both children
	auto key_estimate = EstimateKeyCount(join);
	bool aggregate_side[2];
	for (idx_t side = 0; side < 2; side++) {
		aggregate_side[side] = required_side[side].empty() && SideCollapses(join, side);
	}

	if (use_operator && aggregate_side[0] && aggregate_side[1]) {
		return FactorizeBothSides(op, key_bindings, key_estimate);
	}

	vector<FactorizedMultiplicity> result;
	for (idx_t side = 0; side < 2; side++) {
		auto &child = join.children[side];
		auto &projection_map = side == 0 ? join.left_projection_map : join.right_projection_map;
		ProjectionMapRemapper remapper(projection_map, *child);
		if (!aggregate_side[side]) {
			// Keep this side as is, only propagate multiplicities from below
			auto child_required = required_side[side];
			child_required.insert(key_bindings[side].begin(), key_bindings[side].end());
			auto inner = Factorize(child, child_required);
			remapper.Rebuild(*child, inner);
			result.insert(result.end(), inner.begin(), inner.end());
			continue;
		}
		// Nothing above the join needs this side beyond its keys: pre-aggregate it per key
		auto inner = Factorize(child, key_bindings[side]);
		vector<unique_ptr<Expression>> keys;
		for (auto &condition : join.conditions) {
			keys.push_back((side == 0 ? condition.GetLHS() : condition.GetRHS()).Copy());
		}
		vector<idx_t> key_to_group;
		FactorizedMultiplicity multiplicity;
		auto aggregate = CreateKeyAggregate(std::move(child), std::move(keys), inner, key_to_group, multiplicity);
		if (key_estimate != DConstants::INVALID_INDEX) {
			aggregate->SetEstimatedCardinality(key_estimate);
		}
		for (idx_t k = 0; k < join.conditions.size(); k++) {
			auto &condition = join.conditions[k];
			auto &expr = side == 0 ? condition.LeftReference() : condition.RightReference();
			expr = make_uniq<BoundColumnRefExpression>(
			    expr->GetReturnType(), ColumnBinding(aggregate->group_index, ProjectionIndex(key_to_group[k])));
		}
		child = std::move(aggregate);
		remapper.Rebuild(*child, {multiplicity});
		result.push_back(multiplicity);
	}
	return result;
}

unique_ptr<LogicalAggregate>
FactorizedJoinOptimizer::CreateKeyAggregate(unique_ptr<LogicalOperator> child, vector<unique_ptr<Expression>> keys,
                                            const vector<FactorizedMultiplicity> &multiplicities,
                                            vector<idx_t> &key_to_group, FactorizedMultiplicity &result) {
	vector<unique_ptr<Expression>> aggregates;
	unique_ptr<BoundAggregateExpression> count;
	if (multiplicities.empty()) {
		count = BindAggregate("count_star", {});
	} else {
		vector<unique_ptr<Expression>> children;
		children.push_back(MultiplicityProduct(multiplicities));
		count = BindAggregate("sum", std::move(children));
	}
	if (!count) {
		throw InternalException("FactorizedJoinOptimizer: could not bind the multiplicity aggregate");
	}
	aggregates.push_back(std::move(count));

	auto group_index = optimizer.binder.GenerateTableIndex();
	auto aggregate_index = optimizer.binder.GenerateTableIndex();
	auto aggregate = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	for (auto &key : keys) {
		idx_t group = DConstants::INVALID_INDEX;
		for (idx_t g = 0; g < aggregate->groups.size(); g++) {
			if (aggregate->groups[g]->Equals(*key)) {
				group = g;
				break;
			}
		}
		if (group == DConstants::INVALID_INDEX) {
			group = aggregate->groups.size();
			aggregate->groups.push_back(std::move(key));
		}
		key_to_group.push_back(group);
	}
	aggregate->children.push_back(std::move(child));
	aggregate->ResolveOperatorTypes();
	result.binding = ColumnBinding(aggregate_index, ProjectionIndex(0));
	result.type = aggregate->types[aggregate->groups.size()];
	return aggregate;
}

vector<FactorizedMultiplicity>
FactorizedJoinOptimizer::FactorizeBothSides(unique_ptr<LogicalOperator> &op,
                                            const column_binding_set_t (&key_bindings)[2], idx_t key_estimate) {
	auto &join = op->Cast<LogicalComparisonJoin>();
	// Nothing above needs either side beyond its keys: aggregate both into one table and emit one row per key
	unique_ptr<Expression> weights[2];
	for (idx_t side = 0; side < 2; side++) {
		auto inner = Factorize(join.children[side], key_bindings[side]);
		if (!inner.empty()) {
			weights[side] = MultiplicityProduct(inner);
		}
	}
	// Only now copy the keys: factorizing a side may have rebound the conditions
	Value directory_min;
	idx_t directory_range = 0;
	if (use_directory) {
		GetDirectoryBounds(join, directory_min, directory_range);
	}
	vector<unique_ptr<Expression>> keys[2];
	for (auto &condition : join.conditions) {
		keys[0].push_back(condition.GetLHS().Copy());
		keys[1].push_back(condition.GetRHS().Copy());
	}
	auto group_index = optimizer.binder.GenerateTableIndex();
	auto aggregate_index = optimizer.binder.GenerateTableIndex();
	auto group_join =
	    make_uniq<LogicalFactorizedGroupJoin>(group_index, aggregate_index, std::move(keys[0]), std::move(keys[1]),
	                                          std::move(weights[0]), std::move(weights[1]));
	group_join->directory_min = directory_min;
	group_join->directory_range = directory_range;
	group_join->children.push_back(std::move(join.children[0]));
	group_join->children.push_back(std::move(join.children[1]));
	group_join->ResolveOperatorTypes();
	if (key_estimate != DConstants::INVALID_INDEX) {
		group_join->SetEstimatedCardinality(key_estimate);
	}
	vector<FactorizedMultiplicity> result;
	for (idx_t side = 0; side < 2; side++) {
		result.push_back(FactorizedMultiplicity {ColumnBinding(aggregate_index, ProjectionIndex(side)),
		                                         group_join->multiplicity_types[side]});
	}
	op = std::move(group_join);
	return result;
}

//===--------------------------------------------------------------------===//
// Expressions
//===--------------------------------------------------------------------===//

unique_ptr<Expression> FactorizedJoinOptimizer::Multiply(unique_ptr<Expression> left, unique_ptr<Expression> right) {
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(left));
	children.push_back(std::move(right));
	return BindBuiltinScalarFunction(context, Identifier("*"), std::move(children));
}

unique_ptr<Expression>
FactorizedJoinOptimizer::MultiplicityProduct(const vector<FactorizedMultiplicity> &multiplicities) {
	unique_ptr<Expression> result;
	for (auto &multiplicity : multiplicities) {
		auto factor = BoundCastExpression::AddCastToType(context, ColumnRef(multiplicity), LogicalType::BIGINT);
		result = result ? Multiply(std::move(result), std::move(factor)) : std::move(factor);
	}
	return result;
}

unique_ptr<BoundAggregateExpression> FactorizedJoinOptimizer::BindAggregate(const string &name,
                                                                            vector<unique_ptr<Expression>> children) {
	return BindFactorizedAggregate(context, name, std::move(children));
}

//===--------------------------------------------------------------------===//
// Heuristics
//===--------------------------------------------------------------------===//

idx_t FactorizedJoinOptimizer::EstimateKeyCount(const LogicalComparisonJoin &join) {
	auto &left = *join.children[0];
	auto &right = *join.children[1];
	if (!join.has_estimated_cardinality || !left.has_estimated_cardinality || !right.has_estimated_cardinality) {
		return DConstants::INVALID_INDEX;
	}
	auto c0 = static_cast<double>(MaxValue<idx_t>(left.estimated_cardinality, 1));
	auto c1 = static_cast<double>(MaxValue<idx_t>(right.estimated_cardinality, 1));
	auto cj = static_cast<double>(MaxValue<idx_t>(join.estimated_cardinality, 1));
	// An n:m join of c0 and c1 rows over k keys produces about c0 * c1 / k rows
	auto keys = MinValue<double>(MinValue<double>(c0, c1), (c0 * c1) / cj);
	return MaxValue<idx_t>(static_cast<idx_t>(keys), 1);
}

bool FactorizedJoinOptimizer::GetDirectoryBounds(const LogicalComparisonJoin &join, Value &directory_min,
                                                 idx_t &directory_range) const {
	if (join.conditions.size() != 1) {
		return false;
	}
	auto &condition = join.conditions[0];
	if (!FactorizedKeyFitsInBigint(condition.GetLHS().GetReturnType())) {
		return false;
	}
	auto &left_stats = condition.GetLeftStats();
	auto &right_stats = condition.GetRightStats();
	if (!left_stats || !right_stats || !NumericStats::HasMinMax(*left_stats) ||
	    !NumericStats::HasMinMax(*right_stats)) {
		return false;
	}
	// Cover both sides completely, so that a key outside the directory can only mean unsound statistics
	auto min = MinValue<int64_t>(NumericStats::Min(*left_stats).GetValue<int64_t>(),
	                             NumericStats::Min(*right_stats).GetValue<int64_t>());
	auto max = MaxValue<int64_t>(NumericStats::Max(*left_stats).GetValue<int64_t>(),
	                             NumericStats::Max(*right_stats).GetValue<int64_t>());
	auto range = static_cast<uint64_t>(max) - static_cast<uint64_t>(min) + 1;
	// Every sinking thread of both sides holds private counters for the whole range, plus the two shared
	// arrays; these are not tracked by the buffer manager, so keep the worst case to a small share of memory
	auto threads = NumericCast<uint64_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	auto max_memory = BufferManager::GetBufferManager(context).GetMaxMemory();
	if (range > (1ULL << 23) || range * sizeof(int64_t) * (2 * threads + 2) > max_memory / 16) {
		return false;
	}
	directory_min = Value::BIGINT(min);
	directory_range = range;
	return true;
}

bool FactorizedJoinOptimizer::SideCollapses(const LogicalComparisonJoin &join, idx_t side) {
	auto keys = EstimateKeyCount(join);
	if (keys == DConstants::INVALID_INDEX) {
		// Without statistics the pre-aggregation costs at most about as much as the hash join build it replaces
		return true;
	}
	// Pre-aggregating only pays for an n:m join. A side with many rows per matched key but a tiny other side
	// (an edge table against a handful of filtered vertices) is served better by probing the small hash table,
	// and a side whose key is unique gains nothing from being grouped; both show up as the other side having
	// fewer than two rows per matched key.
	auto rows = join.children[side]->estimated_cardinality;
	auto other_rows = join.children[1 - side]->estimated_cardinality;
	return rows >= 2 * keys && other_rows >= 2 * keys;
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

void CoreOptimizer::Register(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption(Identifier(FactorizedJoinOptimizer::SETTING_NAME),
	                          "Pre-aggregate join sides that are only counted or summed above the join, so that "
	                          "path counts become degree products instead of enumerating every path",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption(Identifier(FactorizedJoinOptimizer::OPERATOR_SETTING_NAME),
	                          "Use the FACTORIZED_GROUP_JOIN operator when both sides of a join are only counted or "
	                          "summed, instead of separate GROUP BY aggregates and a hash join",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	config.AddExtensionOption(Identifier(FactorizedJoinOptimizer::DIRECTORY_SETTING_NAME),
	                          "Let FACTORIZED_GROUP_JOIN count into a dense array indexed by the key when the key "
	                          "bounds are known, instead of a hash table",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
	OptimizerExtension extension;
	extension.optimize_function = FactorizedJoinOptimizer::Optimize;
	OptimizerExtension::Register(config, extension);
}

} // namespace duckdb
