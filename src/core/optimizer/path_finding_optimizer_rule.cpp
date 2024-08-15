#include "duckpgq/core/optimizer/path_finding_optimizer_rule.hpp"

#include "duckpgq/core/optimizer/duckpgq_optimizer.hpp"

#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_empty_result.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_limit.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckpgq/core/functions/function_data/iterative_length_function_data.hpp>
#include <duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp>
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

bool DuckpgqOptimizerExtension::GetPathFindingOption(ClientContext &context) {
  Value value;
  context.TryGetCurrentSetting("experimental_path_finding_operator", value);
  return value.GetValue<bool>();
}

// Helper function to create the required BoundColumnRefExpression
unique_ptr<Expression> CreateReplacementExpression(const string &alias, const string &functionName, idx_t tableIndex, idx_t position) {
  if (functionName == "iterativelengthoperator") {
    return make_uniq<BoundColumnRefExpression>(alias, LogicalType::BIGINT, ColumnBinding(tableIndex, position));
  }
  if (functionName == "shortestpathoperator") {
    return make_uniq<BoundColumnRefExpression>(alias, LogicalType::LIST(LogicalType::BIGINT), ColumnBinding(tableIndex, position));
  }
  return nullptr;
}

void ReplaceExpressions(LogicalProjection &op, unique_ptr<Expression> &function_expression, string &mode, vector<idx_t> &offsets) {
    // Create a temporary vector to hold the new expressions
    vector<unique_ptr<Expression>> new_expressions;
    new_expressions.reserve(op.expressions.size());  // Reserve space to avoid multiple reallocations

    for (size_t offset = 0; offset < op.expressions.size(); ++offset) {
        const auto &expr = op.expressions[offset];
        if (expr->expression_class != ExpressionClass::BOUND_FUNCTION) {
            // Directly transfer the expression to the new vector if no replacement is needed
            new_expressions.push_back(std::move(op.expressions[offset]));
            continue;
        }

        auto &bound_function_expression = expr->Cast<BoundFunctionExpression>();
        const auto &function_name = bound_function_expression.function.name;

        if (function_name == "iterativelengthoperator" || function_name == "shortestpathoperator") {
            // Create the replacement expression
            auto replacement_expr = CreateReplacementExpression(expr->alias, function_name, op.table_index, offset);
            if (replacement_expr) {
                // store the offsets of the expressions that need to be replaced
                offsets.push_back(offset);
                // Push the replacement into the new vector
                new_expressions.push_back(std::move(replacement_expr));
                // Optionally, copy the original expression if it's needed elsewhere
                function_expression = expr->Copy();
                mode = function_name == "iterativelengthoperator" ? "iterativelength" : "shortestpath";
            } else {
                // If no replacement is created, throw an internal exception
                throw InternalException("Found a bound path-finding function that should be replaced but could not be replaced.");
            }
        } else {
            // If the expression is a bound function but not one we are interested in replacing
            new_expressions.push_back(std::move(op.expressions[offset]));
        }
    }

    // Replace the old expressions vector with the new one
    op.expressions = std::move(new_expressions);
}

bool DuckpgqOptimizerExtension::InsertPathFindingOperator(
    LogicalOperator &op, ClientContext &context) {
  unique_ptr<Expression> function_expression;
  string mode;
  vector<idx_t> offsets;
  // Iterate in reverse to not influence the upcoming iterations when
  // erasing an element from the list. Does not work if both iterativelength
  // and shortestpath are called in the same query for now. To be improved
  // in the future.
  if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
    for (auto &child : op.children) {
      if (InsertPathFindingOperator(*child, context)) {
        return true;
      }
    }
    return false;
  }
  auto &op_proj = op.Cast<LogicalProjection>();
  ReplaceExpressions(op_proj, function_expression, mode, offsets);

  for (const auto &child : op.children) {
    vector<unique_ptr<LogicalOperator>> path_finding_children;
    if (child->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
      continue;
    }
    auto &get_join = child->Cast<LogicalComparisonJoin>();
    //! For now we assume this is enough to detect we have found a
    //! path-finding query. Should be improved in the future
    if (get_join.children.size() != 2) {
      continue;
    }
    /*TODO Check both options:
      Left is aggregate and right is filter
      Right is aggregate, left is filter
    */

    if (get_join.children[1]->type !=
        LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
      continue;
    }
    auto &get_aggregate = get_join.children[1]->Cast<LogicalAggregate>();
    auto &get_limit = get_aggregate.children[0]->Cast<LogicalLimit>();
    auto &get_projection = get_limit.children[0]->Cast<LogicalProjection>();
    auto &get_function_expression =
        get_projection.expressions[0]->Cast<BoundFunctionExpression>();
    if (get_function_expression.function.name != "csr_operator") {
      continue;
    }
    vector<unique_ptr<Expression>> path_finding_expressions =
        std::move(get_function_expression.children);
    if (get_join.children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
      auto &get_filter = get_join.children[0]->Cast<LogicalFilter>();
      if (get_filter.children[0]->type != LogicalOperatorType::LOGICAL_GET) {
        continue;
      }
      path_finding_children.push_back(std::move(get_filter.children[0]));
    } else if (get_join.children[0]->type ==
               LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
      auto default_database = DatabaseManager::GetDefaultDatabase(context);
      auto &catalog = Catalog::GetCatalog(context, default_database);
      auto &bound_function_expression =
          function_expression->Cast<BoundFunctionExpression>();
      auto &bind_info = bound_function_expression.bind_info
                            ->Cast<ShortestPathOperatorData>();
      auto &duckdb_table = catalog.GetEntry<DuckTableEntry>(
          context, DEFAULT_SCHEMA, bind_info.table_to_scan);
      auto &get_empty_result = get_join.children[0]->Cast<LogicalEmptyResult>();
      vector<string> returned_names = {"src", "dst"};
      unique_ptr<FunctionData> bind_data;
      auto scan_function = duckdb_table.GetScanFunction(context, bind_data);
      auto logical_get = make_uniq<LogicalGet>(
          get_empty_result.bindings[0].table_index, scan_function,
          std::move(bind_data), get_empty_result.return_types, returned_names);
      vector<column_t> column_ids_vector;
      for (const auto &binding : get_empty_result.bindings) {
        column_ids_vector.push_back(binding.column_index);
      }
      logical_get->column_ids = std::move(column_ids_vector);
      path_finding_children.push_back(std::move(logical_get));
    }
    path_finding_children.push_back(std::move(get_projection.children[0]));

    auto path_finding_operator = make_uniq<LogicalPathFindingOperator>(
        path_finding_children, path_finding_expressions, mode, op_proj.table_index, offsets);
    op.children.clear();
    op.children.push_back(std::move(path_finding_operator));
    return true; // We have found the path-finding operator, no need to continue
  }
  for (auto &child : op.children) {
    if (InsertPathFindingOperator(*child, context)) {
      return true;
    }
  }
  return false;
}

void DuckpgqOptimizerExtension::DuckpgqOptimizeFunction(
    OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
  if (!GetPathFindingOption(input.context)) {
    return;
  }
  InsertPathFindingOperator(*plan, input.context);
}

//------------------------------------------------------------------------------
// Register optimizer
//------------------------------------------------------------------------------
void CorePGQOptimizer::RegisterPathFindingOptimizerRule(DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);

  config.optimizer_extensions.push_back(DuckpgqOptimizerExtension());
}

} // namespace core
} // namespace duckpgq