#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckpgq/utils/compressed_sparse_row.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"

namespace duckdb {

class DuckpgqExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

class DuckpgqOptimizerExtension : public OptimizerExtension {
public:
  DuckpgqOptimizerExtension() {
    optimize_function = DuckpgqOptimizeFunction;
  }

  static bool InsertPathFindingOperator(LogicalOperator &op, ClientContext &context) {
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
      return true; // We have found the path-finding operator, no need to continue
    }
    for (auto &child : op.children) {
      if (InsertPathFindingOperator(*child, context)) {
        return true;
      }
    }
    return false;
  }

  static void DuckpgqOptimizeFunction(ClientContext &context, OptimizerExtensionInfo *info,
                                     duckdb::unique_ptr<LogicalOperator> &plan) {
    auto& client_config = ClientConfig::GetConfig(context);
    auto const path_finding_operator_option = client_config.set_variables.find("experimental_path_finding_operator");
    if (path_finding_operator_option == client_config.set_variables.end()) {
      return; // If the path finding operator is not enabled, we do not need to do anything
    }
    if (!path_finding_operator_option->second.GetValue<bool>()) {
      return;
    }
    InsertPathFindingOperator(*plan, context);
  }
};

struct DuckPGQParserExtensionInfo : public ParserExtensionInfo {
public:
  DuckPGQParserExtensionInfo() : ParserExtensionInfo(){};
  ~DuckPGQParserExtensionInfo() override = default;
};

BoundStatement duckpgq_bind(ClientContext &context, Binder &binder,
                            OperatorExtensionInfo *info,
                            SQLStatement &statement);

struct DuckPGQOperatorExtension : public OperatorExtension {
  DuckPGQOperatorExtension() : OperatorExtension() { Bind = duckpgq_bind; }

  std::string GetName() override { return "duckpgq_bind"; }

  unique_ptr<LogicalExtensionOperator>
  Deserialize(Deserializer &deserializer) override {
    throw InternalException("DuckPGQ operator should not be serialized");
  }
};

ParserExtensionParseResult duckpgq_parse(ParserExtensionInfo *info,
                                         const std::string &query);

ParserExtensionPlanResult duckpgq_plan(ParserExtensionInfo *info,
                                       ClientContext &,
                                       unique_ptr<ParserExtensionParseData>);

ParserExtensionPlanResult
duckpgq_handle_statement(unique_ptr<SQLStatement> &statement);

struct DuckPGQParserExtension : public ParserExtension {
  DuckPGQParserExtension() : ParserExtension() {
    parse_function = duckpgq_parse;
    plan_function = duckpgq_plan;
    parser_info = make_shared_ptr<DuckPGQParserExtensionInfo>();
  }
};

struct DuckPGQParseData : ParserExtensionParseData {
  unique_ptr<SQLStatement> statement;

  unique_ptr<ParserExtensionParseData> Copy() const override {
    return make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(
        statement->Copy());
  }

  string ToString() const override { return statement->ToString(); };

  explicit DuckPGQParseData(unique_ptr<SQLStatement> statement)
      : statement(std::move(statement)) {}
};

class DuckPGQState : public ClientContextState {
public:
  explicit DuckPGQState(unique_ptr<ParserExtensionParseData> parse_data)
      : parse_data(std::move(parse_data)) {}

  void QueryEnd() override {
    parse_data.reset();
    transform_expression.clear();
    match_index = 0;              // Reset the index
    unnamed_graphtable_index = 1; // Reset the index
    for (const auto &csr_id : csr_to_delete) {
      csr_list.erase(csr_id);
    }
  }

  CreatePropertyGraphInfo *GetPropertyGraph(const string &pg_name) {
    auto pg_table_entry = registered_property_graphs.find(pg_name);
    if (pg_table_entry == registered_property_graphs.end()) {
      throw BinderException("Property graph %s does not exist", pg_name);
    }
    return reinterpret_cast<CreatePropertyGraphInfo *>(
        pg_table_entry->second.get());
  }

  CSR *GetCSR(int32_t id) {
    auto csr_entry = csr_list.find(id);
    if (csr_entry == csr_list.end()) {
      throw ConstraintException("CSR not found with ID %d", id);
    }
    return csr_entry->second.get();
  }

public:
  unique_ptr<ParserExtensionParseData> parse_data;

  unordered_map<int32_t, unique_ptr<ParsedExpression>> transform_expression;
  int32_t match_index = 0;
  int32_t unnamed_graphtable_index = 1; // Used to generate unique names for
                                        // unnamed graph tables

  //! Property graphs that are registered
  std::unordered_map<string, unique_ptr<CreateInfo>> registered_property_graphs;

  //! Used to build the CSR data structures required for path-finding queries
  std::unordered_map<int32_t, unique_ptr<CSR>> csr_list;
  std::mutex csr_lock;
  std::unordered_set<int32_t> csr_to_delete;
};

} // namespace duckdb
