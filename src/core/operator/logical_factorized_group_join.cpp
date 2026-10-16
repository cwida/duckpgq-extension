#include "duckpgq/core/operator/logical_factorized_group_join.hpp"

#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckpgq/core/operator/physical_factorized_group_join.hpp"

namespace duckdb {

LogicalFactorizedGroupJoin::LogicalFactorizedGroupJoin(TableIndex group_index_p, TableIndex aggregate_index_p,
                                                       vector<unique_ptr<Expression>> left_keys,
                                                       vector<unique_ptr<Expression>> right_keys,
                                                       unique_ptr<Expression> left_weight,
                                                       unique_ptr<Expression> right_weight)
    : group_index(group_index_p), aggregate_index(aggregate_index_p), key_count(left_keys.size()) {
	if (left_keys.size() != right_keys.size() || left_keys.empty()) {
		throw InternalException("LogicalFactorizedGroupJoin: both sides need the same, non-empty set of keys");
	}
	for (auto &key : left_keys) {
		expressions.push_back(std::move(key));
	}
	for (auto &key : right_keys) {
		expressions.push_back(std::move(key));
	}
	has_weight[0] = left_weight != nullptr;
	has_weight[1] = right_weight != nullptr;
	if (left_weight) {
		expressions.push_back(std::move(left_weight));
	}
	if (right_weight) {
		expressions.push_back(std::move(right_weight));
	}
	for (idx_t side = 0; side < SIDES; side++) {
		// count_star() is a BIGINT, the sum of BIGINT weights is a HUGEINT
		multiplicity_types[side] = has_weight[side] ? LogicalType::HUGEINT : LogicalType::BIGINT;
	}
}

idx_t LogicalFactorizedGroupJoin::WeightIndex(idx_t side) const {
	auto index = SIDES * key_count;
	if (side == 1 && has_weight[0]) {
		index++;
	}
	return index;
}

optional_ptr<const Expression> LogicalFactorizedGroupJoin::Weight(idx_t side) const {
	if (!has_weight[side]) {
		return nullptr;
	}
	return expressions[WeightIndex(side)].get();
}

vector<ColumnBinding> LogicalFactorizedGroupJoin::GetColumnBindings() {
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < key_count; i++) {
		result.emplace_back(group_index, ProjectionIndex(i));
	}
	for (idx_t side = 0; side < SIDES; side++) {
		result.emplace_back(aggregate_index, ProjectionIndex(side));
	}
	return result;
}

void LogicalFactorizedGroupJoin::ResolveTypes() {
	types.clear();
	for (idx_t i = 0; i < key_count; i++) {
		types.push_back(Key(0, i).GetReturnType());
	}
	for (idx_t side = 0; side < SIDES; side++) {
		types.push_back(multiplicity_types[side]);
	}
}

vector<TableIndex> LogicalFactorizedGroupJoin::GetTableIndex() const {
	return {group_index, aggregate_index};
}

string LogicalFactorizedGroupJoin::GetName() const {
	return "FACTORIZED_GROUP_JOIN";
}

InsertionOrderPreservingMap<string> LogicalFactorizedGroupJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string conditions;
	for (idx_t i = 0; i < key_count; i++) {
		if (i > 0) {
			conditions += "\n";
		}
		conditions += Key(0, i).ToString() + " = " + Key(1, i).ToString();
	}
	result["Conditions"] = conditions;
	for (idx_t side = 0; side < SIDES; side++) {
		auto weight = Weight(side);
		if (weight) {
			result[side == 0 ? "Left Weight" : "Right Weight"] = weight->ToString();
		}
	}
	result["Lookup"] = UsesDirectory() ? "DIRECTORY" : "HASH";
	return result;
}

string LogicalFactorizedGroupJoin::GetExtensionName() const {
	return "duckpgq";
}

optional_ptr<const string> LogicalFactorizedGroupJoin::GetTypeBindingVerificationIdentifier() const noexcept {
	// Returning an identifier opts into the standard traversal, which cannot resolve two-sided keys
	return nullptr;
}

//! Turns the column references in `expr` into positions in `input_bindings`
static void ResolveAgainst(unique_ptr<Expression> &expr, const vector<ColumnBinding> &input_bindings) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [&](BoundColumnRefExpression &ref, unique_ptr<Expression> &ptr) {
		    for (idx_t i = 0; i < input_bindings.size(); i++) {
			    if (input_bindings[i] == ref.Binding()) {
				    ptr = make_uniq<BoundReferenceExpression>(ref.GetAlias(), ref.GetReturnType(), i);
				    return;
			    }
		    }
		    throw InternalException("FACTORIZED_GROUP_JOIN: failed to bind column reference %s", ref.ToString());
	    });
}

void LogicalFactorizedGroupJoin::ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) {
	// Every side's keys and weight refer to that side's input only
	for (idx_t side = 0; side < SIDES; side++) {
		res.VisitOperator(*children[side]);
		auto input_bindings = children[side]->GetColumnBindings();
		for (idx_t i = 0; i < key_count; i++) {
			ResolveAgainst(expressions[side * key_count + i], input_bindings);
		}
		if (has_weight[side]) {
			ResolveAgainst(expressions[WeightIndex(side)], input_bindings);
		}
	}
	bindings = GetColumnBindings();
}

PhysicalOperator &LogicalFactorizedGroupJoin::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &left = planner.CreatePlan(*children[0]);
	auto &right = planner.CreatePlan(*children[1]);
	vector<vector<unique_ptr<Expression>>> keys(SIDES);
	vector<unique_ptr<Expression>> weights(SIDES);
	for (idx_t side = 0; side < SIDES; side++) {
		for (idx_t i = 0; i < key_count; i++) {
			keys[side].push_back(Key(side, i).Copy());
		}
		auto weight = Weight(side);
		if (weight) {
			weights[side] = weight->Copy();
		}
	}
	return planner.Make<PhysicalFactorizedGroupJoin>(context, *this, left, right, std::move(keys), std::move(weights),
	                                                 directory_min, directory_range, estimated_cardinality);
}

void LogicalFactorizedGroupJoin::Serialize(Serializer &serializer) const {
	throw NotImplementedException("FACTORIZED_GROUP_JOIN plans cannot be serialized");
}

} // namespace duckdb
