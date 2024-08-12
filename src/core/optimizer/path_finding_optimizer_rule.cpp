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
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

bool DuckpgqOptimizerExtension::InsertPathFindingOperator(LogicalOperator &op, ClientContext &context) {
    unique_ptr<Expression> function_expression;
    string mode;
    for (int64_t i = op.expressions.size() - 1; i >= 0; --i) {
      const auto &expr = op.expressions[i];
      if (expr->expression_class != ExpressionClass::BOUND_FUNCTION) {
        continue;
      }
      auto &bound_function_expression = expr->Cast<BoundFunctionExpression>();
      if (bound_function_expression.function.name == "iterativelength") {
        op.expressions.emplace_back(make_uniq<BoundColumnRefExpression>(
    expr->alias, LogicalType::BIGINT, ColumnBinding(10, 0)));
        function_expression = expr->Copy();
        op.expressions.erase(op.expressions.begin() + i);
        mode = "iterativelength";
      } else if (bound_function_expression.function.name == "shortestpath") {
        op.expressions.emplace_back(make_uniq<BoundColumnRefExpression>(
    expr->alias, LogicalType::LIST(LogicalType::BIGINT), ColumnBinding(10, 0)));
        function_expression = expr->Copy();
        op.expressions.erase(op.expressions.begin() + i);
        mode = "shortestpath";
      }
    }
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
      if (get_join.children[1]->type !=
          LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
        continue;
      }
      auto &get_aggregate = get_join.children[1]->Cast<LogicalAggregate>();
      auto &get_limit = get_aggregate.children[0]->Cast<LogicalLimit>();
      auto &get_projection = get_limit.children[0]->Cast<LogicalProjection>();
      auto &get_function_expression =
          get_projection.expressions[0]->Cast<BoundFunctionExpression>();
      if (get_function_expression.function.name != "create_csr_edge") {
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
        auto &bound_function_expression = function_expression->Cast<BoundFunctionExpression>();
        auto &bind_info = bound_function_expression.bind_info->Cast<IterativeLengthFunctionData>();
        auto &duckdb_table = catalog.GetEntry<DuckTableEntry>(context, DEFAULT_SCHEMA, bind_info.table_to_scan);
        auto &get_empty_result =
            get_join.children[0]->Cast<LogicalEmptyResult>();
        vector<string> returned_names = {"src", "dst"};
		    unique_ptr<FunctionData> bind_data;
        auto scan_function = duckdb_table.GetScanFunction(context, bind_data);
        auto logical_get = make_uniq<LogicalGet>(
            get_empty_result.bindings[0].table_index, scan_function,
            std::move(bind_data), get_empty_result.return_types,
            returned_names);
        vector<column_t> column_ids_vector;
        for (const auto &binding : get_empty_result.bindings) {
          column_ids_vector.push_back(binding.column_index);
        }
        logical_get->column_ids = std::move(column_ids_vector);
        path_finding_children.push_back(std::move(logical_get));
      }
      path_finding_children.push_back(std::move(get_projection.children[0]));
      // Iterate in reverse to not influence the upcoming iterations when
      // erasing an element from the list. Does not work if both iterativelength
      // and shortestpath are called in the same query for now. To be improved
      // in the future.


      auto path_finding_operator = make_uniq<LogicalPathFindingOperator>(
          path_finding_children, path_finding_expressions, mode);
      op.children.clear();
      op.children.push_back(std::move(path_finding_operator));
      std::cout << "Found path-finding operator" << std::endl;
      return true; // We have found the path-finding operator, no need to continue
    }
    for (auto &child : op.children) {
      if (InsertPathFindingOperator(*child, context)) {
        return true;
      }
    }
    return false;
  }

void DuckpgqOptimizerExtension::DuckpgqOptimizeFunction(OptimizerExtensionInput &input,
                                    duckdb::unique_ptr<LogicalOperator> &plan) {
  auto& client_config = ClientConfig::GetConfig(input.context);
  auto const path_finding_operator_option = client_config.set_variables.find("experimental_path_finding_operator");
  if (path_finding_operator_option == client_config.set_variables.end()) {
    return; // If the path finding operator is not enabled, we do not need to do anything
  }
  if (!path_finding_operator_option->second.GetValue<bool>()) {
    return;
  }
  InsertPathFindingOperator(*plan, input.context);
}


//------------------------------------------------------------------------------
// Register optimizer
//------------------------------------------------------------------------------
void CorePGQOptimizer::RegisterPathFindingOptimizerRule(
    DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);

  config.optimizer_extensions.push_back(DuckpgqOptimizerExtension());
}

} // namespace core
} // namespace duckpgq