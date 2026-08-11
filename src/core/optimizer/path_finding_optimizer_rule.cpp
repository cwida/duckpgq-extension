#include "duckpgq/core/optimizer/path_finding_optimizer_rule.hpp"

#include "duckpgq/core/optimizer/duckpgq_optimizer.hpp"
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
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

static const BoundFunctionExpression *GetPathFindingFunction(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function_expr = expr.Cast<BoundFunctionExpression>();
		auto function_name = function_expr.Function().GetName().GetIdentifierName();
		if (function_name == "iterativelengthoperator" || function_name == "pushpulliterativelengthoperator" ||
		    function_name == "bidirectionaliterativelengthoperator" || function_name == "shortestpathoperator") {
			return &function_expr;
		}
	}

	const BoundFunctionExpression *result = nullptr;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!result) {
			result = GetPathFindingFunction(child);
		}
	});
	return result;
}

static string GetPathFindingCacheKey(const Expression &expr) {
	auto function_expr = GetPathFindingFunction(expr);
	if (!function_expr || function_expr->GetChildren().size() < 4) {
		return string();
	}
	auto cache_key_index = function_expr->GetChildren().size() - 1;
	if (function_expr->GetChildren()[cache_key_index]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		throw BinderException("The path-finding cache key must be a constant VARCHAR");
	}
	auto &constant = function_expr->GetChildren()[cache_key_index]->Cast<BoundConstantExpression>();
	if (constant.GetValue().IsNull()) {
		return string();
	}
	return constant.GetValue().GetValue<string>();
}

static int64_t GetConstantInt64(const Expression &expr, const string &description) {
	auto current = &expr;
	while (BoundCastExpression::IsCast(*current)) {
		auto &cast_expr = current->Cast<BoundFunctionExpression>();
		current = &BoundCastExpression::Child(cast_expr);
	}
	if (current->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		throw BinderException("%s must be a constant BIGINT value", description);
	}
	auto &constant = current->Cast<BoundConstantExpression>();
	if (constant.GetValue().IsNull()) {
		throw BinderException("%s cannot be NULL", description);
	}
	return constant.GetValue().GetValue<int64_t>();
}

static bool IsCSRIdProjection(const LogicalProjection &projection) {
	for (const auto &expr : projection.expressions) {
		if (expr->GetAlias() == "csr_id" || expr->GetName() == "csr_id" || expr->GetAlias() == "pathfinding_edge_src" ||
		    expr->GetName() == "pathfinding_edge_src") {
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

void ReplaceExpressions(LogicalProjection &op, unique_ptr<Expression> &function_expression, string &mode,
                        vector<idx_t> &offsets, string &cache_key);

static bool ProjectionContainsName(const LogicalProjection &projection, const string &name) {
	for (const auto &expr : projection.expressions) {
		if (expr->GetAlias() == name || expr->GetName() == name) {
			return true;
		}
	}
	return false;
}

static void CollectPrecountedInputs(unique_ptr<LogicalOperator> &op, vector<unique_ptr<LogicalOperator>> &inputs) {
	auto defines_count_input = op->type == LogicalOperatorType::LOGICAL_PROJECTION &&
	                           ProjectionContainsName(op->Cast<LogicalProjection>(), "pathfinding_count_data");
	auto defines_edge_input = op->type == LogicalOperatorType::LOGICAL_PROJECTION &&
	                          ProjectionContainsName(op->Cast<LogicalProjection>(), "pathfinding_edge_data");
	if (defines_count_input || defines_edge_input || op->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		inputs.push_back(std::move(op));
		return;
	}
	auto &cross_product = op->Cast<LogicalCrossProduct>();
	for (auto &child : cross_product.children) {
		CollectPrecountedInputs(child, inputs);
	}
}

static bool ExpressionReferencesInput(const Expression &expr, LogicalOperator &input) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &column_ref = expr.Cast<BoundColumnRefExpression>();
		auto bindings = input.GetColumnBindings();
		return std::find(bindings.begin(), bindings.end(), column_ref.Binding()) != bindings.end();
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { found = found || ExpressionReferencesInput(child, input); });
	return found;
}

static unique_ptr<LogicalPathFindingOperator> FindPrecountedEdgesAndPairs(unique_ptr<LogicalOperator> &root,
                                                                          LogicalProjection &projection) {
	const BoundFunctionExpression *path_function = nullptr;
	for (const auto &expr : projection.expressions) {
		path_function = GetPathFindingFunction(*expr);
		if (path_function && path_function->GetChildren().size() == 7 &&
		    path_function->GetChildren()[2]->GetReturnType().id() == LogicalTypeId::STRUCT) {
			break;
		}
		path_function = nullptr;
	}
	if (!path_function) {
		return nullptr;
	}
	auto vertex_count_value =
	    GetConstantInt64(*path_function->GetChildren()[4], "Pre-counted path-finding vertex count");
	auto edge_count_value = GetConstantInt64(*path_function->GetChildren()[5], "Pre-counted path-finding edge count");
	if (vertex_count_value < 0 || static_cast<uint64_t>(vertex_count_value) > NumericLimits<uint32_t>::Maximum()) {
		throw OutOfRangeException("Pre-counted path-finding vertex count is outside the supported uint32 range: %lld",
		                          vertex_count_value);
	}
	if (edge_count_value < 0) {
		throw OutOfRangeException("Pre-counted path-finding edge count cannot be negative: %lld", edge_count_value);
	}

	vector<unique_ptr<LogicalOperator>> inputs;
	CollectPrecountedInputs(root, inputs);
	if (inputs.size() != 3) {
		throw BinderException("Pre-counted path finding expects pairs, count endpoints, and fill endpoints");
	}

	idx_t pair_index = DConstants::INVALID_INDEX;
	idx_t count_index = DConstants::INVALID_INDEX;
	idx_t edge_index = DConstants::INVALID_INDEX;
	for (idx_t input_idx = 0; input_idx < inputs.size(); input_idx++) {
		if (ExpressionReferencesInput(*path_function->GetChildren()[2], *inputs[input_idx])) {
			count_index = input_idx;
		} else if (ExpressionReferencesInput(*path_function->GetChildren()[3], *inputs[input_idx])) {
			edge_index = input_idx;
		} else if (ExpressionReferencesInput(*path_function->GetChildren()[0], *inputs[input_idx])) {
			pair_index = input_idx;
		}
	}
	if (pair_index == DConstants::INVALID_INDEX || count_index == DConstants::INVALID_INDEX ||
	    edge_index == DConstants::INVALID_INDEX) {
		throw BinderException("Could not identify all pre-counted path-finding inputs");
	}
	vector<unique_ptr<LogicalOperator>> path_finding_children;
	path_finding_children.push_back(std::move(inputs[pair_index]));
	path_finding_children.push_back(std::move(inputs[count_index]));
	path_finding_children.push_back(std::move(inputs[edge_index]));
	vector<unique_ptr<Expression>> path_finding_expressions;
	unique_ptr<Expression> function_expression;
	string mode;
	vector<idx_t> offsets;
	string cache_key;
	ReplaceExpressions(projection, function_expression, mode, offsets, cache_key);
	path_finding_expressions.push_back(std::move(function_expression));
	return make_uniq<LogicalPathFindingOperator>(
	    path_finding_children, path_finding_expressions, mode, projection.table_index, offsets, std::move(cache_key),
	    true, true, static_cast<idx_t>(vertex_count_value), static_cast<idx_t>(edge_count_value));
}

static unique_ptr<LogicalPathFindingOperator> FindBufferedEdgesAndPairs(unique_ptr<LogicalOperator> &root,
                                                                        LogicalProjection &projection) {
	const BoundFunctionExpression *path_function = nullptr;
	for (const auto &expr : projection.expressions) {
		path_function = GetPathFindingFunction(*expr);
		if (path_function && path_function->GetChildren().size() == 6 &&
		    path_function->GetChildren()[2]->GetReturnType().id() == LogicalTypeId::STRUCT) {
			break;
		}
		path_function = nullptr;
	}
	if (!path_function) {
		return nullptr;
	}

	auto vertex_count_value = GetConstantInt64(*path_function->GetChildren()[3], "Buffered path-finding vertex count");
	auto edge_count_value = GetConstantInt64(*path_function->GetChildren()[4], "Buffered path-finding edge count");
	if (vertex_count_value < 0 || static_cast<uint64_t>(vertex_count_value) > NumericLimits<uint32_t>::Maximum()) {
		throw OutOfRangeException("Buffered path-finding vertex count is outside the supported uint32 range: %lld",
		                          vertex_count_value);
	}
	if (edge_count_value < 0) {
		throw OutOfRangeException("Buffered path-finding edge count cannot be negative: %lld", edge_count_value);
	}

	vector<unique_ptr<LogicalOperator>> inputs;
	CollectPrecountedInputs(root, inputs);
	if (inputs.size() != 2) {
		throw BinderException("Buffered path finding expects pairs and one endpoint input");
	}

	idx_t pair_index = DConstants::INVALID_INDEX;
	idx_t edge_index = DConstants::INVALID_INDEX;
	for (idx_t input_idx = 0; input_idx < inputs.size(); input_idx++) {
		if (ExpressionReferencesInput(*path_function->GetChildren()[2], *inputs[input_idx])) {
			edge_index = input_idx;
		} else if (ExpressionReferencesInput(*path_function->GetChildren()[0], *inputs[input_idx])) {
			pair_index = input_idx;
		}
	}
	if (pair_index == DConstants::INVALID_INDEX || edge_index == DConstants::INVALID_INDEX) {
		throw BinderException("Could not identify buffered path-finding inputs");
	}

	vector<unique_ptr<LogicalOperator>> path_finding_children;
	path_finding_children.push_back(std::move(inputs[pair_index]));
	path_finding_children.push_back(std::move(inputs[edge_index]));
	vector<unique_ptr<Expression>> path_finding_expressions;
	unique_ptr<Expression> function_expression;
	string mode;
	vector<idx_t> offsets;
	string cache_key;
	ReplaceExpressions(projection, function_expression, mode, offsets, cache_key);
	path_finding_expressions.push_back(std::move(function_expression));
	return make_uniq<LogicalPathFindingOperator>(
	    path_finding_children, path_finding_expressions, mode, projection.table_index, offsets, std::move(cache_key),
	    true, false, static_cast<idx_t>(vertex_count_value), static_cast<idx_t>(edge_count_value), false, true);
}

static unique_ptr<LogicalPathFindingOperator> FindCachedPartitionedCSRAndPairs(unique_ptr<LogicalOperator> &root,
                                                                               LogicalProjection &projection) {
	const BoundFunctionExpression *path_function = nullptr;
	for (const auto &expr : projection.expressions) {
		path_function = GetPathFindingFunction(*expr);
		if (path_function && path_function->GetChildren().size() == 5) {
			break;
		}
		path_function = nullptr;
	}
	if (!path_function) {
		return nullptr;
	}

	auto vertex_count_value = GetConstantInt64(*path_function->GetChildren()[2], "Cached path-finding vertex count");
	auto edge_count_value = GetConstantInt64(*path_function->GetChildren()[3], "Cached path-finding edge count");
	if (vertex_count_value < 0 || static_cast<uint64_t>(vertex_count_value) > NumericLimits<uint32_t>::Maximum()) {
		throw OutOfRangeException("Cached path-finding vertex count is outside the supported uint32 range: %lld",
		                          vertex_count_value);
	}
	if (edge_count_value < 0) {
		throw OutOfRangeException("Cached path-finding edge count cannot be negative: %lld", edge_count_value);
	}

	vector<unique_ptr<LogicalOperator>> path_finding_children;
	path_finding_children.push_back(std::move(root));
	vector<unique_ptr<Expression>> path_finding_expressions;
	unique_ptr<Expression> function_expression;
	string mode;
	vector<idx_t> offsets;
	string cache_key;
	ReplaceExpressions(projection, function_expression, mode, offsets, cache_key);
	if (cache_key.empty()) {
		throw BinderException("Cache-only path finding requires a non-empty constant cache key");
	}
	path_finding_expressions.push_back(std::move(function_expression));
	return make_uniq<LogicalPathFindingOperator>(
	    path_finding_children, path_finding_expressions, mode, projection.table_index, offsets, std::move(cache_key),
	    false, false, static_cast<idx_t>(vertex_count_value), static_cast<idx_t>(edge_count_value), true);
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
                        vector<idx_t> &offsets, string &cache_key) {
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
		auto expression_cache_key = GetPathFindingCacheKey(*expr);
		if (!cache_key.empty() && !expression_cache_key.empty() && cache_key != expression_cache_key) {
			throw BinderException("All path-finding expressions in a projection must use the same cache key");
		}
		if (!expression_cache_key.empty()) {
			cache_key = std::move(expression_cache_key);
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
		string cache_key;
		ReplaceExpressions(op_proj, function_expression, path_finding_mode, offsets, cache_key);
		auto path_finding_function = GetPathFindingFunction(*function_expression);
		auto edge_input = path_finding_function && path_finding_function->GetChildren().size() >= 7;
		return make_uniq<LogicalPathFindingOperator>(path_finding_children, path_finding_expressions, path_finding_mode,
		                                             op_proj.table_index, offsets, std::move(cache_key), edge_input);
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
	if (op_proj.children.size() == 1) {
		auto path_finding_operator = FindCachedPartitionedCSRAndPairs(op_proj.children[0], op_proj);
		if (!path_finding_operator) {
			path_finding_operator = FindBufferedEdgesAndPairs(op_proj.children[0], op_proj);
		}
		if (!path_finding_operator) {
			path_finding_operator = FindPrecountedEdgesAndPairs(op_proj.children[0], op_proj);
		}
		if (path_finding_operator) {
			op.children.clear();
			op.children.push_back(std::move(path_finding_operator));
			return true;
		}
	}

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
