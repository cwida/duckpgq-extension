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
#include <duckpgq/core/functions/function_data/shortest_path_operator_function_data.hpp>
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

namespace duckpgq {
namespace core {

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
        std::cout << expr->ToString() << std::endl;
        if (expr->expression_class != ExpressionClass::BOUND_FUNCTION) {
            // Directly transfer the expression to the new vector if no replacement is needed
            new_expressions.push_back(std::move(op.expressions[offset]));
            continue;
        }

        auto &bound_function_expression = expr->Cast<BoundFunctionExpression>();
        std::cout << bound_function_expression.ToString();
        const auto &function_name = bound_function_expression.function.name;
        std::cout << "function name: " << function_name << std::endl;
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

unique_ptr<LogicalPathFindingOperator> DuckpgqOptimizerExtension::FindCSRAndPairs(
    unique_ptr<LogicalOperator>& first_child,
    unique_ptr<LogicalOperator>& second_child,
    LogicalProjection& op_proj) {
  bool csr_found = false;
  bool pairs_found = false;
  vector<unique_ptr<Expression>> path_finding_expressions;
  vector<unique_ptr<LogicalOperator>> path_finding_children;
  LogicalProjection *csr_projection = nullptr;
  // Find CSR
  if (first_child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    auto &get_aggregate = first_child->Cast<LogicalAggregate>();
    auto &potential_csr_projection = get_aggregate.children[0]->Cast<LogicalProjection>();
    auto &get_function_expression = potential_csr_projection.expressions[0]->Cast<BoundFunctionExpression>();
    if (get_function_expression.function.name == "csr_operator") {
      std::cout << "Found csr_operator" << std::endl;
      csr_found = true;
      path_finding_expressions = std::move(get_function_expression.children);
      csr_projection = &potential_csr_projection;
    }
  }

  // Find pairs
  if (second_child->type == LogicalOperatorType::LOGICAL_FILTER) {
    auto &get_filter = second_child->Cast<LogicalFilter>();
    if (get_filter.children[0]->type == LogicalOperatorType::LOGICAL_GET) {
      std::cout << "Found pairs" << std::endl;
      pairs_found = true;
      path_finding_children.push_back(std::move(get_filter.children[0]));
    }
  } else if (second_child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
      auto &get_projection = second_child->Cast<LogicalProjection>();
      if (get_projection.children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
        auto &get_filter = get_projection.children[0]->Cast<LogicalFilter>();
        if (get_filter.children[0]->type == LogicalOperatorType::LOGICAL_GET) {
          std::cout << "Found pairs" << std::endl;
          pairs_found = true;
          path_finding_children.push_back(std::move(get_filter.children[0]));
        }
      } else if (get_projection.children[0]->type == LogicalOperatorType::LOGICAL_GET) {
        path_finding_children.push_back(std::move(get_projection.children[0]));
        std::cout << "Found pairs" << std::endl;
        pairs_found = true;
      }
  }
  if (pairs_found && csr_found) {
    path_finding_children.push_back(std::move(csr_projection->children[0]));
    std::cout << "Found both csr and pairs" << std::endl;
    if (path_finding_children.size() != 2) {
      throw InternalException("Path-finding operator should have 2 children");
    }
    unique_ptr<Expression> function_expression;
    string path_finding_mode;
    vector<idx_t> offsets;
    ReplaceExpressions(op_proj, function_expression, path_finding_mode, offsets);
    return make_uniq<LogicalPathFindingOperator>(
      path_finding_children, path_finding_expressions, path_finding_mode, op_proj.table_index, offsets);
  } else {
    return nullptr;
  }
}

bool DuckpgqOptimizerExtension::InsertPathFindingOperator(
    LogicalOperator &op, ClientContext &context) {
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

      Right can also be projection into aggregate
    */
    auto &left_child = get_join.children[0];
    auto &right_child = get_join.children[1];
    std::cout << "left child type: " << LogicalOperatorToString(left_child->type) << std::endl;
    std::cout << "right child type: " << LogicalOperatorToString(right_child->type) << std::endl;
    auto path_finding_operator = FindCSRAndPairs(left_child, right_child, op_proj);
    if (path_finding_operator == nullptr) {
      path_finding_operator = FindCSRAndPairs(right_child, left_child, op_proj);
    }
    if (path_finding_operator != nullptr) {
      op.children.clear();
      op.children.push_back(std::move(path_finding_operator));
      std::cout << "Inserted path-finding operator" << std::endl;
      return true;
    }

    return false; // No path-finding operator found

//
//    if (right_child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
//      right_child = std::move(right_child->children[0]);
//    }
//    if (right_child->type !=
//        LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
//      continue;
//    }
//    auto &get_aggregate = right_child->Cast<LogicalAggregate>();
//    vector<unique_ptr<Expression>> path_finding_expressions;
//    auto *current_projection = &get_aggregate.children[0]->Cast<LogicalProjection>();
//
//    while (true) {
//      // Get the function expression from the current projection
//      auto &get_function_expression = current_projection->expressions[0]->Cast<BoundFunctionExpression>();
//      std::cout << get_function_expression.function.name << std::endl;
//      // Check if the function is 'csr_operator'
//      if (get_function_expression.function.name == "csr_operator") {
//        // Function found, move the expressions to path_finding_expressions
//        path_finding_expressions = std::move(get_function_expression.children);
//        std::cout << "Found csr_operator" << std::endl;
//        // Break the loop after moving the expressions
//        break;
//      }
//
//      // If not, check if there is a child projection to continue the search
//      if (current_projection->children.empty() ||
//          current_projection->children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
//          // No more child projections, exit the loop
//          break;
//          }
//
//      // Move to the child projection
//      current_projection = &current_projection->children[0]->Cast<LogicalProjection>();
//    }
//    if (path_finding_expressions.empty()) {
//      continue; // No path-finding expressions found, continue searching
//    }
//
//    if (left_child->type == LogicalOperatorType::LOGICAL_FILTER) {
//      auto &get_filter = left_child->Cast<LogicalFilter>();
//
//      if (get_filter.children[0]->type != LogicalOperatorType::LOGICAL_GET) {
//        continue;
//      }
//
//      path_finding_children.push_back(std::move(get_filter.children[0]));
//    } else if (left_child->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
//      auto default_database = DatabaseManager::GetDefaultDatabase(context);
//      auto &catalog = Catalog::GetCatalog(context, default_database);
//      auto &bound_function_expression =
//          function_expression->Cast<BoundFunctionExpression>();
//      auto &bind_info =
//          bound_function_expression.bind_info->Cast<ShortestPathOperatorData>();
//      auto &duckdb_table = catalog.GetEntry<DuckTableEntry>(
//          context, DEFAULT_SCHEMA, bind_info.table_to_scan);
//      auto &get_empty_result = left_child->Cast<LogicalEmptyResult>();
//
//      vector<string> returned_names = {"src", "dst"};
//      unique_ptr<FunctionData> bind_data;
//      auto scan_function = duckdb_table.GetScanFunction(context, bind_data);
//
//      auto logical_get = make_uniq<LogicalGet>(
//          get_empty_result.bindings[0].table_index, scan_function,
//          std::move(bind_data), get_empty_result.return_types, returned_names
//      );
//
//      vector<column_t> column_ids_vector;
//      for (const auto &binding : get_empty_result.bindings) {
//        column_ids_vector.push_back(binding.column_index);
//      }
//      logical_get->SetColumnIds(std::move(column_ids_vector));
//
//      path_finding_children.push_back(std::move(logical_get));
//    } else if (left_child->type == LogicalOperatorType::LOGICAL_PROJECTION) {
//      path_finding_children.push_back(std::move(left_child));
//    } else {
//      throw InternalException("Did not find pairs for path-finding operator. "
//                              "The left child was of type " +
//                              LogicalOperatorToString(left_child->type));
//    }
//    path_finding_children.push_back(std::move(current_projection->children[0]));
//    if (path_finding_children.size() != 2) {
//      throw InternalException("Path-finding operator should have 2 children");
//    }
//    ReplaceExpressions(op_proj, function_expression, mode, offsets);
//
//    auto path_finding_operator = make_uniq<LogicalPathFindingOperator>(
//        path_finding_children, path_finding_expressions, mode, op_proj.table_index, offsets);
//    op.children.clear();
//    op.children.push_back(std::move(path_finding_operator));
//    std::cout << "Inserted path-finding operator" << std::endl;
//    return true; // We have found the path-finding operator, no need to continue
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
    std::cout << "Disabled path finding operator, skipping optimizer rule" << std::endl;
    return;
  }
  if (InsertPathFindingOperator(*plan, input.context)) {
    plan->Print();
  }
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