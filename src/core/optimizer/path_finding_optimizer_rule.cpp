#include "duckpgq/core/optimizer/path_finding_optimizer_rule.hpp"

#include "duckpgq/core/optimizer/duckpgq_optimizer.hpp"
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_cross_product.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include "duckdb/main/extension_callback_manager.hpp"
#include <duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp>
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

namespace duckdb {

static string GetPathFindingFunctionName(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function_expr = expr.Cast<BoundFunctionExpression>();
		auto function_name = function_expr.Function().GetName().GetIdentifierName();
		if (function_name == "iterativelengthoperator" || function_name == "pushpulliterativelengthoperator" ||
		    function_name == "bidirectionaliterativelengthoperator" || function_name == "shortestpathoperator") {
			return function_name;
		}
	}

	string result;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!result.empty()) {
			return;
		}
		result = GetPathFindingFunctionName(child);
	});
	return result;
}

static bool IsCSRIdProjection(const LogicalProjection &projection) {
	for (const auto &expr : projection.expressions) {
		if (expr->GetAlias() == "csr_id" || expr->GetName() == "csr_id") {
			return true;
		}
		if (BoundCastExpression::IsCast(*expr)) {
			auto &cast_expr = expr->Cast<BoundFunctionExpression>();
			if (BoundCastExpression::Child(cast_expr).GetName() == "csr_id") {
				return true;
			}
		}
	}
	return false;
}

static bool ContainsCSRIdProjection(const LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION && IsCSRIdProjection(op.Cast<LogicalProjection>())) {
		return true;
	}
	for (const auto &child : op.children) {
		if (ContainsCSRIdProjection(*child)) {
			return true;
		}
	}
	return false;
}

// Helper function to create the required BoundColumnRefExpression
unique_ptr<Expression> CreateReplacementExpression(const Identifier &alias, const string &functionName,
                                                   TableIndex tableIndex, idx_t position) {
	if (functionName == "iterativelengthoperator" || functionName == "pushpulliterativelengthoperator" ||
	    functionName == "bidirectionaliterativelengthoperator") {
		return make_uniq<BoundColumnRefExpression>(alias, LogicalType::BIGINT,
		                                           ColumnBinding(tableIndex, ProjectionIndex(position)));
	}
	if (functionName == "shortestpathoperator") {
		return make_uniq<BoundColumnRefExpression>(alias, LogicalType::LIST(LogicalType::BIGINT),
		                                           ColumnBinding(tableIndex, ProjectionIndex(position)));
	}
	return nullptr;
}

void ReplaceExpressions(LogicalProjection &op, unique_ptr<Expression> &function_expression, string &mode,
                        vector<idx_t> &offsets) {
	// Create a temporary vector to hold the new expressions
	vector<unique_ptr<Expression>> new_expressions;
	new_expressions.reserve(op.expressions.size()); // Reserve space to avoid multiple reallocations

	for (size_t offset = 0; offset < op.expressions.size(); ++offset) {
		const auto &expr = op.expressions[offset];
		auto function_name = GetPathFindingFunctionName(*expr);
		if (function_name.empty()) {
			// Directly transfer the expression to the new vector if no replacement is needed
			new_expressions.push_back(std::move(op.expressions[offset]));
			continue;
		}

		// Create the replacement expression
		auto replacement_expr = CreateReplacementExpression(expr->GetAlias(), function_name, op.table_index, offset);
		if (replacement_expr) {
			// store the offsets of the expressions that need to be replaced
			offsets.push_back(offset);
			// Push the replacement into the new vector
			new_expressions.push_back(std::move(replacement_expr));
			// Optionally, copy the original expression if it's needed elsewhere
			function_expression = expr->Copy();
			if (function_name == "iterativelengthoperator") {
				mode = "iterativelength";
			} else if (function_name == "pushpulliterativelengthoperator") {
				mode = "pushpulliterativelength";
			} else if (function_name == "bidirectionaliterativelengthoperator") {
				mode = "bidirectionaliterativelength";
			} else {
				mode = "shortestpath";
			}
		} else {
			// If no replacement is created, throw an internal exception
			throw InternalException(
			    "Found a bound path-finding function that should be replaced but could not be replaced.");
		}
	}

	// Replace the old expressions vector with the new one
	op.expressions = std::move(new_expressions);
}

unique_ptr<LogicalPathFindingOperator>
DuckpgqOptimizerExtension::FindCSRAndPairs(unique_ptr<LogicalOperator> &first_child,
                                           unique_ptr<LogicalOperator> &second_child, LogicalProjection &op_proj,
                                           ClientContext &context) {
	bool marker_found = false;
	for (const auto &expr : op_proj.expressions) {
		if (!GetPathFindingFunctionName(*expr).empty()) {
			marker_found = true;
			break;
		}
	}
	if (!marker_found) {
		return nullptr;
	}

	bool csr_found = false;
	vector<unique_ptr<Expression>> path_finding_expressions;
	vector<unique_ptr<LogicalOperator>> path_finding_children;
	csr_found = ContainsCSRIdProjection(*first_child);

	if (csr_found) {
		path_finding_children.push_back(std::move(second_child));
		path_finding_children.push_back(std::move(first_child));
		if (path_finding_children.size() != 2) {
			throw InternalException("Path-finding operator should have 2 children");
		}
		unique_ptr<Expression> function_expression;
		string path_finding_mode;
		vector<idx_t> offsets;
		ReplaceExpressions(op_proj, function_expression, path_finding_mode, offsets);
		return make_uniq<LogicalPathFindingOperator>(path_finding_children, path_finding_expressions, path_finding_mode,
		                                             op_proj.table_index, offsets);
	}
	// Didn't find the CSR
	return nullptr;
}

bool DuckpgqOptimizerExtension::InsertPathFindingOperator(LogicalOperator &op, ClientContext &context) {
	unique_ptr<Expression> function_expression;
	string mode;
	vector<idx_t> offsets;
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		for (auto &child : op.children) {
			if (InsertPathFindingOperator(*child, context)) {
				return true;
			}
		}
		return false;
	}
	auto &op_proj = op.Cast<LogicalProjection>();

	for (const auto &child : op_proj.children) {
		vector<unique_ptr<LogicalOperator>> path_finding_children;
		if (child->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			continue;
		}
		auto &get_join = child->Cast<LogicalCrossProduct>();
		//! For now we assume this is enough to detect we have found a
		//! path-finding query. Should be improved in the future
		if (get_join.children.size() != 2) {
			continue;
		}

		auto &left_child = get_join.children[0];
		auto &right_child = get_join.children[1];
		auto path_finding_operator = FindCSRAndPairs(left_child, right_child, op_proj, context);
		if (path_finding_operator == nullptr) {
			path_finding_operator = FindCSRAndPairs(right_child, left_child, op_proj, context);
		}
		if (path_finding_operator != nullptr) {
			op.children.clear();
			op.children.push_back(std::move(path_finding_operator));
			return true;
		}

		return false; // No path-finding operator found
	}
	for (auto &child : op.children) {
		if (InsertPathFindingOperator(*child, context)) {
			return true;
		}
	}
	return false;
}

void DuckpgqOptimizerExtension::DuckpgqOptimizeFunction(OptimizerExtensionInput &input,
                                                        unique_ptr<LogicalOperator> &plan) {
	if (!GetPathFindingOption(input.context)) {
		return;
	}
	InsertPathFindingOperator(*plan, input.context);
}

//------------------------------------------------------------------------------
// Register optimizer
//------------------------------------------------------------------------------
void CorePGQOptimizer::RegisterPathFindingOptimizerRule(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &manager = ExtensionCallbackManager::Get(db);
	manager.Register(DuckpgqOptimizerExtension());
}

} // namespace duckdb
