//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/optimizer/factorized_join_optimizer.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/column_binding_map.hpp"

namespace duckdb {

class BoundAggregateExpression;
class LogicalAggregate;
class LogicalComparisonJoin;
class Optimizer;

//! A column telling how many flattened rows each row of a factorized subtree stands for
struct FactorizedMultiplicity {
	ColumnBinding binding;
	LogicalType type;
};

//! Aggregates that can be computed from a factorized join result
enum class FactorizedAggregateKind : uint8_t { COUNT_STAR, COUNT, SUM, MIN_MAX };

//! Rewrites aggregates over n:m joins into factorized form: a join side whose columns are not needed above the join
//! is pre-aggregated by its join keys, and the aggregate above is corrected with the resulting multiplicities.
//! For a path pattern (a)->(b)->(c) this turns count(*) into sum(in_degree(b) * out_degree(b)).
class FactorizedJoinOptimizer {
public:
	static constexpr const char *SETTING_NAME = "duckpgq_factorized_joins";

	FactorizedJoinOptimizer(ClientContext &context, Optimizer &optimizer);

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
	void Run(unique_ptr<LogicalOperator> &plan);

private:
	void VisitOperator(unique_ptr<LogicalOperator> &op);
	//! Returns true when the subtree below the aggregate has been visited, whether or not it was rewritten
	bool TryFactorizeAggregate(unique_ptr<LogicalOperator> &op);
	//! Rewrites `op` so that it still emits the `required` columns under their current bindings, but possibly with
	//! fewer rows; returns the multiplicity columns that were added to the output (empty when nothing changed)
	vector<FactorizedMultiplicity> Factorize(unique_ptr<LogicalOperator> &op, const column_binding_set_t &required);
	vector<FactorizedMultiplicity> FactorizeJoin(unique_ptr<LogicalOperator> &op, const column_binding_set_t &required);
	vector<FactorizedMultiplicity> FactorizeProjection(unique_ptr<LogicalOperator> &op,
	                                                   const column_binding_set_t &required);
	vector<FactorizedMultiplicity> FactorizeFilter(unique_ptr<LogicalOperator> &op,
	                                               const column_binding_set_t &required);
	//! Fallback for operators that are not factorized: look for further aggregates below them
	vector<FactorizedMultiplicity> VisitChildren(unique_ptr<LogicalOperator> &op);

	//! Wraps `child` in GROUP BY keys with a single count or multiplicity sum
	unique_ptr<LogicalAggregate> CreateKeyAggregate(unique_ptr<LogicalOperator> child,
	                                                vector<unique_ptr<Expression>> keys,
	                                                const vector<FactorizedMultiplicity> &multiplicities,
	                                                vector<idx_t> &key_to_group, FactorizedMultiplicity &result);
	unique_ptr<Expression> RewriteAggregate(const BoundAggregateExpression &aggregate, FactorizedAggregateKind kind,
	                                        const Expression &product);
	//! The product of all multiplicities as a BIGINT expression
	unique_ptr<Expression> MultiplicityProduct(const vector<FactorizedMultiplicity> &multiplicities);
	unique_ptr<Expression> Multiply(unique_ptr<Expression> left, unique_ptr<Expression> right);
	unique_ptr<BoundAggregateExpression> BindAggregate(const string &name, vector<unique_ptr<Expression>> children);

	static bool GetAggregateKind(const BoundAggregateExpression &aggregate, FactorizedAggregateKind &kind);
	//! Whether pre-aggregating `side` by the join keys is expected to shrink it
	static bool SideCollapses(const LogicalComparisonJoin &join, idx_t side);
	static idx_t EstimateKeyCount(const LogicalComparisonJoin &join);

private:
	ClientContext &context;
	Optimizer &optimizer;
	unique_ptr<LogicalOperator> *root = nullptr;
	bool changed = false;
};

struct CoreOptimizer {
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
