#include "duckpgq/core/optimizer/path_finding_optimizer_rule.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include "duckpgq/core/optimizer/duckpgq_optimizer.hpp"
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_cross_product.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include "duckdb/main/extension_callback_manager.hpp"
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

namespace duckdb {

//! The marker function of a projection: where it sits in the SELECT list, what it computes and its argument shape
struct PathFindingMarker {
	const BoundFunctionExpression *function = nullptr;
	//! Offset of the SELECT-list expression that contains the marker (the marker may sit inside casts)
	idx_t offset = DConstants::INVALID_INDEX;
	PathFindingOperatorMode mode = PathFindingOperatorMode::ITERATIVE_LENGTH;
	//! Marker arguments without the optional trailing maximum path length
	idx_t base_arity = 0;

	const Expression &Argument(idx_t index) const {
		return *function->GetChildren()[index];
	}
	bool ArgumentIsStruct(idx_t index) const {
		return Argument(index).GetReturnType().id() == LogicalTypeId::STRUCT;
	}
};

//! The marker function inside `expr`, or nullptr
static const BoundFunctionExpression *FindMarkerFunction(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &function_expr = expr.Cast<BoundFunctionExpression>();
		if (IsPathFindingMarkerName(function_expr.Function().GetName().GetIdentifierName())) {
			return &function_expr;
		}
	}
	const BoundFunctionExpression *result = nullptr;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (!result) {
			result = FindMarkerFunction(child);
		}
	});
	return result;
}

//! The cache key is the last VARCHAR argument; an optional BIGINT maximum path length may follow it
static idx_t CacheKeyIndex(const BoundFunctionExpression &function) {
	auto &children = function.GetChildren();
	for (idx_t i = children.size(); i > 0; i--) {
		if (children[i - 1]->GetReturnType().id() == LogicalTypeId::VARCHAR) {
			return i - 1;
		}
	}
	return DConstants::INVALID_INDEX;
}

static bool HasTrailingMaxDepth(const BoundFunctionExpression &function) {
	auto key_index = CacheKeyIndex(function);
	return key_index != DConstants::INVALID_INDEX && key_index + 2 == function.GetChildren().size();
}

//! Finds the marker of the projection; a SELECT list with more than one is rejected
static bool FindProjectionMarker(const LogicalProjection &projection, PathFindingMarker &marker) {
	bool found = false;
	for (idx_t offset = 0; offset < projection.expressions.size(); offset++) {
		auto function = FindMarkerFunction(*projection.expressions[offset]);
		if (!function) {
			continue;
		}
		if (found) {
			throw BinderException("Only one path-finding function per SELECT list is supported; compute the path "
			                      "length once and reuse the column");
		}
		found = true;
		marker.function = function;
		marker.offset = offset;
		marker.mode = PathFindingModeFromMarkerName(function->Function().GetName().GetIdentifierName());
		marker.base_arity = function->GetChildren().size() - (HasTrailingMaxDepth(*function) ? 1 : 0);
	}
	return found;
}

static idx_t GetMaxDepth(ClientContext &context, const PathFindingMarker &marker) {
	if (!HasTrailingMaxDepth(*marker.function)) {
		return 0;
	}
	// Before DuckDB's own optimizer runs the literal is still wrapped in a cast, so fold it here
	auto &depth_expr = *marker.function->GetChildren().back();
	Value depth_value;
	if (!depth_expr.IsFoldable() || !ExpressionExecutor::TryEvaluateScalar(context, depth_expr, depth_value)) {
		throw BinderException("The path-finding maximum length must be a constant BIGINT");
	}
	if (depth_value.IsNull() || depth_value.GetValue<int64_t>() <= 0) {
		return 0;
	}
	return NumericCast<idx_t>(depth_value.GetValue<int64_t>());
}

//! The marker's constant cache key; empty when the marker has none (the global-CSR form) or it is NULL
static string GetCacheKey(const PathFindingMarker &marker) {
	auto key_index = CacheKeyIndex(*marker.function);
	if (key_index == DConstants::INVALID_INDEX) {
		return string();
	}
	auto &key_expr = marker.Argument(key_index);
	if (key_expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		throw BinderException("The path-finding cache key must be a constant VARCHAR");
	}
	auto &constant = key_expr.Cast<BoundConstantExpression>();
	return constant.GetValue().IsNull() ? string() : constant.GetValue().GetValue<string>();
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

//! The vertex count at argument `vertex_index` and the edge count after it; `route_name` names the form in errors
static void ReadGraphCounts(const PathFindingMarker &marker, idx_t vertex_index, const char *route_name,
                            PathFindingInput &input) {
	auto vertex_count = GetConstantInt64(marker.Argument(vertex_index),
	                                     StringUtil::Format("%s path-finding vertex count", route_name));
	auto edge_count = GetConstantInt64(marker.Argument(vertex_index + 1),
	                                   StringUtil::Format("%s path-finding edge count", route_name));
	if (vertex_count < 0 || static_cast<uint64_t>(vertex_count) > NumericLimits<uint32_t>::Maximum()) {
		throw OutOfRangeException("%s path-finding vertex count is outside the supported uint32 range: %lld",
		                          route_name, vertex_count);
	}
	if (edge_count < 0) {
		throw OutOfRangeException("%s path-finding edge count cannot be negative: %lld", route_name, edge_count);
	}
	input.vertex_count = static_cast<idx_t>(vertex_count);
	input.edge_count = static_cast<idx_t>(edge_count);
}

//! The key of an endpoint route describes the graph; MATCH ends it with |undirected when the endpoint input holds
//! both directions of every edge (the operator may then search from the destination side)
static void ReadEndpointKey(const PathFindingMarker &marker, PathFindingInput &input) {
	input.cache_key = GetCacheKey(marker);
	input.undirected = StringUtil::EndsWith(input.cache_key, "|undirected");
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

static bool ProjectionContainsName(const LogicalProjection &projection, const string &name) {
	for (const auto &expr : projection.expressions) {
		if (expr->GetAlias() == name || expr->GetName() == name) {
			return true;
		}
	}
	return false;
}

//! The inputs of an endpoint route are the sides of the cross products under the projection, except that a
//! projection defining pathfinding_count_data or pathfinding_edge_data is one input however it is built
static void CollectEndpointInputs(unique_ptr<LogicalOperator> &op, vector<unique_ptr<LogicalOperator>> &inputs) {
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
		CollectEndpointInputs(child, inputs);
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

//! The unclaimed input a marker argument reads from, claimed; INVALID_INDEX when there is none
static idx_t ClaimInput(const Expression &argument, vector<unique_ptr<LogicalOperator>> &inputs,
                        vector<bool> &claimed) {
	for (idx_t input_idx = 0; input_idx < inputs.size(); input_idx++) {
		if (!claimed[input_idx] && ExpressionReferencesInput(argument, *inputs[input_idx])) {
			claimed[input_idx] = true;
			return input_idx;
		}
	}
	return DConstants::INVALID_INDEX;
}

//! Replaces the marker's SELECT-list expression by a reference to the operator's result column and hands the
//! marker expression to the operator: its source and destination arguments are evaluated over the pairs there
static unique_ptr<LogicalPathFindingOperator> MakeOperator(vector<unique_ptr<LogicalOperator>> children,
                                                           LogicalProjection &projection,
                                                           const PathFindingMarker &marker, PathFindingInput input) {
	auto &select_expr = projection.expressions[marker.offset];
	auto marker_expr = std::move(select_expr);
	select_expr = make_uniq<BoundColumnRefExpression>(
	    marker_expr->GetAlias(), PathFindingResultType(marker.mode),
	    ColumnBinding(projection.table_index, ProjectionIndex(marker.offset)));
	return make_uniq<LogicalPathFindingOperator>(std::move(children), std::move(marker_expr), marker.mode,
	                                             projection.table_index, marker.offset, std::move(input));
}

//! (src, dst, {src, dst}, {src, dst}, vertex_count, edge_count, key): pairs, count endpoints and fill endpoints
//! are the three inputs under the projection
static unique_ptr<LogicalPathFindingOperator> PlanPrecountedEndpointsRoute(unique_ptr<LogicalOperator> &root,
                                                                           LogicalProjection &projection,
                                                                           const PathFindingMarker &marker,
                                                                           ClientContext &context) {
	PathFindingInput input;
	input.route = PathFindingInputRoute::PRECOUNTED_ENDPOINTS;
	ReadGraphCounts(marker, 4, "Pre-counted", input);
	ReadEndpointKey(marker, input);
	input.max_depth = GetMaxDepth(context, marker);

	vector<unique_ptr<LogicalOperator>> inputs;
	CollectEndpointInputs(root, inputs);
	if (inputs.size() != 3) {
		throw BinderException("Pre-counted path finding expects pairs, count endpoints, and fill endpoints");
	}
	vector<bool> claimed(inputs.size(), false);
	auto count_index = ClaimInput(marker.Argument(2), inputs, claimed);
	auto edge_index = ClaimInput(marker.Argument(3), inputs, claimed);
	auto pair_index = ClaimInput(marker.Argument(0), inputs, claimed);
	if (pair_index == DConstants::INVALID_INDEX || count_index == DConstants::INVALID_INDEX ||
	    edge_index == DConstants::INVALID_INDEX) {
		throw BinderException("Could not identify all pre-counted path-finding inputs");
	}
	vector<unique_ptr<LogicalOperator>> children;
	children.push_back(std::move(inputs[pair_index]));
	children.push_back(std::move(inputs[count_index]));
	children.push_back(std::move(inputs[edge_index]));
	return MakeOperator(std::move(children), projection, marker, std::move(input));
}

//! (src, dst, {src, dst[, id]}, vertex_count, edge_count, key[, max_depth]): pairs and the endpoint input are the
//! two inputs under the projection
static unique_ptr<LogicalPathFindingOperator> PlanBufferedEndpointsRoute(unique_ptr<LogicalOperator> &root,
                                                                         LogicalProjection &projection,
                                                                         const PathFindingMarker &marker,
                                                                         ClientContext &context) {
	PathFindingInput input;
	input.route = PathFindingInputRoute::BUFFERED_ENDPOINTS;
	ReadGraphCounts(marker, 3, "Buffered", input);
	ReadEndpointKey(marker, input);
	input.max_depth = GetMaxDepth(context, marker);

	vector<unique_ptr<LogicalOperator>> inputs;
	CollectEndpointInputs(root, inputs);
	if (inputs.size() != 2) {
		throw BinderException("Buffered path finding expects pairs and one endpoint input");
	}
	vector<bool> claimed(inputs.size(), false);
	auto edge_index = ClaimInput(marker.Argument(2), inputs, claimed);
	auto pair_index = ClaimInput(marker.Argument(0), inputs, claimed);
	if (pair_index == DConstants::INVALID_INDEX || edge_index == DConstants::INVALID_INDEX) {
		throw BinderException("Could not identify buffered path-finding inputs");
	}
	vector<unique_ptr<LogicalOperator>> children;
	children.push_back(std::move(inputs[pair_index]));
	children.push_back(std::move(inputs[edge_index]));
	return MakeOperator(std::move(children), projection, marker, std::move(input));
}

//! (src, dst, vertex_count, edge_count, 'csr-index|...'[, max_depth]): the projection's child is the pair input,
//! the partitions come from the index named by the key
static unique_ptr<LogicalPathFindingOperator> PlanCSRIndexRoute(unique_ptr<LogicalOperator> &root,
                                                                LogicalProjection &projection,
                                                                const PathFindingMarker &marker,
                                                                ClientContext &context) {
	PathFindingInput input;
	input.route = PathFindingInputRoute::CSR_INDEX;
	ReadGraphCounts(marker, 2, "Cached", input);
	input.cache_key = GetCacheKey(marker);
	if (input.cache_key.empty()) {
		throw BinderException("Cache-only path finding requires a non-empty constant cache key");
	}
	input.max_depth = GetMaxDepth(context, marker);

	vector<unique_ptr<LogicalOperator>> children;
	children.push_back(std::move(root));
	return MakeOperator(std::move(children), projection, marker, std::move(input));
}

//! (src, dst, csr_id): the pairs and the CSR builder (the side projecting csr_id) are the two sides of a cross
//! product under the projection; any other shape is left for the SQL fallback
static unique_ptr<LogicalPathFindingOperator> PlanGlobalCSRRoute(unique_ptr<LogicalOperator> &root,
                                                                 LogicalProjection &projection,
                                                                 const PathFindingMarker &marker) {
	if (root->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT || root->children.size() != 2) {
		return nullptr;
	}
	auto &left = root->children[0];
	auto &right = root->children[1];
	bool csr_left = ContainsCSRIdProjection(*left);
	if (!csr_left && !ContainsCSRIdProjection(*right)) {
		return nullptr;
	}
	PathFindingInput input;
	input.route = PathFindingInputRoute::GLOBAL_CSR;
	vector<unique_ptr<LogicalOperator>> children;
	children.push_back(std::move(csr_left ? right : left));
	children.push_back(std::move(csr_left ? left : right));
	return MakeOperator(std::move(children), projection, marker, std::move(input));
}

//! Replaces the path-finding marker in this projection with the operator; the marker's argument shape names the
//! route (PathFindingInputRoute). Returns whether the plan changed.
static bool TryReplaceProjection(LogicalProjection &projection, ClientContext &context) {
	PathFindingMarker marker;
	if (!FindProjectionMarker(projection, marker)) {
		return false;
	}
	D_ASSERT(projection.children.size() == 1);
	auto &root = projection.children[0];
	unique_ptr<LogicalPathFindingOperator> path_finding_operator;
	if (marker.base_arity == 5 && !marker.ArgumentIsStruct(2)) {
		path_finding_operator = PlanCSRIndexRoute(root, projection, marker, context);
	} else if (marker.base_arity == 6 && marker.ArgumentIsStruct(2)) {
		path_finding_operator = PlanBufferedEndpointsRoute(root, projection, marker, context);
	} else if (marker.base_arity == 7 && marker.ArgumentIsStruct(2) && marker.ArgumentIsStruct(3)) {
		path_finding_operator = PlanPrecountedEndpointsRoute(root, projection, marker, context);
	} else {
		path_finding_operator = PlanGlobalCSRRoute(root, projection, marker);
	}
	if (!path_finding_operator) {
		return false;
	}
	projection.children.clear();
	projection.children.push_back(std::move(path_finding_operator));
	return true;
}

bool DuckpgqOptimizerExtension::InsertPathFindingOperator(LogicalOperator &op, ClientContext &context) {
	// Every projection in the plan may carry a marker (comma joins, subqueries, set operations), so the whole plan
	// is visited and every marker is replaced; the children of a replaced projection are visited as well
	bool changed = false;
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		changed = TryReplaceProjection(op.Cast<LogicalProjection>(), context);
	}
	for (auto &child : op.children) {
		if (InsertPathFindingOperator(*child, context)) {
			changed = true;
		}
	}
	return changed;
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
