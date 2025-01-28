#include "duckpgq/core/utils/compressed_sparse_row.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {
string CSR::ToString() const {
    std::ostringstream result;

    if (initialized_v) {
        result << "v (Node Offsets):\n";
        for (size_t i = 0; i < vsize; i++) {
            result << "  Node " << i << ": Offset " << v[i].load() << "\n";
        }
    } else {
        result << "v: V has not been initialized\n";
    }

    result << "\n";

    if (initialized_e) {
        result << "e (Edges):\n";
        for (size_t i = 0; i < vsize - 2; i++) {
            result << "  Node " << i << " connects to: ";
            for (size_t j = v[i].load(); j < v[i + 1].load(); j++) {
                result << e[j] << " ";
            }
            result << "\n";
        }
    } else {
        result << "e: E has not been initialized\n";
    }

    result << "\n";

    if (initialized_w) {
        result << "w (Weights):\n";
        for (size_t i = 0; i < vsize - 1; i++) {
            result << "  Node " << i << " weights: ";
            for (size_t j = v[i].load(); j < v[i + 1].load(); j++) {
                result << w[j] << " ";
            }
            result << "\n";
        }
    } else {
        result << "w: W has not been initialized\n";
    }

    return result.str();
}

CSRFunctionData::CSRFunctionData(ClientContext &context, int32_t id,
                                 LogicalType weight_type)
    : context(context), id(id), weight_type(std::move(weight_type)) {}

unique_ptr<FunctionData> CSRFunctionData::Copy() const {
  return make_uniq<CSRFunctionData>(context, id, weight_type);
}

bool CSRFunctionData::Equals(const FunctionData &other_p) const {
  auto &other = (const CSRFunctionData &)other_p;
  return id == other.id && weight_type == other.weight_type;
}

unique_ptr<FunctionData>
CSRFunctionData::CSRVertexBind(ClientContext &context,
                               ScalarFunction &bound_function,
                               vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }

  Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
  if (arguments.size() == 4) {
    auto logical_type = LogicalType::SQLNULL;
    return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                      logical_type);
  }
  return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                    arguments[3]->return_type);
}

unique_ptr<FunctionData>
CSRFunctionData::CSREdgeBind(ClientContext &context,
                             ScalarFunction &bound_function,
                             vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }
  Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
  if (arguments.size() == 8) {
    return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                      arguments[7]->return_type);
  }
  auto logical_type = LogicalType::SQLNULL;
  return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                    logical_type);
}

unique_ptr<FunctionData>
CSRFunctionData::CSRBind(ClientContext &context, ScalarFunction &bound_function,
                         vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }
  Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
  return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                    LogicalType::BOOLEAN);
}

// Helper function to create a JoinRef
unique_ptr<JoinRef> CreateJoin(const string &fk_column, const string &pk_column,
                               const shared_ptr<PropertyGraphTable> &fk_table,
                               const shared_ptr<PropertyGraphTable> &pk_table) {
  auto join = make_uniq<JoinRef>(JoinRefType::REGULAR);
  join->left = fk_table->CreateBaseTableRef();
  join->right = pk_table->CreateBaseTableRef();
  join->condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL,
      make_uniq<ColumnRefExpression>(fk_column, fk_table->table_name),
      make_uniq<ColumnRefExpression>(pk_column, pk_table->table_name));
  return join;
}

// Helper function to setup SelectNode
void SetupSelectNode(unique_ptr<SelectNode> &select_node,
                     const shared_ptr<PropertyGraphTable> &edge_table,
                     bool reverse) {
  select_node = make_uniq<SelectNode>();

  select_node->select_list.emplace_back(CreateColumnRefExpression(
      "rowid", edge_table->source_reference, "dense_id"));

  if (!reverse) {
    select_node->select_list.emplace_back(CreateColumnRefExpression(
        edge_table->source_fk[0], edge_table->table_name, "outgoing_edges"));
    select_node->select_list.emplace_back(
        CreateColumnRefExpression(edge_table->destination_fk[0],
                                  edge_table->table_name, "incoming_edges"));
    select_node->from_table =
        CreateJoin(edge_table->source_fk[0], edge_table->source_pk[0],
                   edge_table, edge_table->source_pg_table);
  } else {
    select_node->select_list.emplace_back(
        CreateColumnRefExpression(edge_table->destination_fk[0],
                                  edge_table->table_name, "outgoing_edges"));
    select_node->select_list.emplace_back(CreateColumnRefExpression(
        edge_table->source_fk[0], edge_table->table_name, "incoming_edges"));
    select_node->from_table =
        CreateJoin(edge_table->destination_fk[0], edge_table->source_pk[0],
                   edge_table, edge_table->source_pg_table);
  }
}

// Function to create a subquery expression for counting table entries
unique_ptr<SubqueryExpression>
GetCountTable(const shared_ptr<PropertyGraphTable> &table,
              const string &table_alias, const string &primary_key) {
  auto select_count = make_uniq<SelectStatement>();
  auto select_inner = make_uniq<SelectNode>();
  auto ref = table->CreateBaseTableRef(table_alias);
  select_inner->from_table = std::move(ref);

  vector<unique_ptr<ParsedExpression>> children;
  children.push_back(make_uniq<ColumnRefExpression>(primary_key, table_alias));

  auto count_function =
      make_uniq<FunctionExpression>("count", std::move(children));
  select_inner->select_list.push_back(std::move(count_function));
  select_count->node = std::move(select_inner);

  auto result = make_uniq<SubqueryExpression>();
  result->subquery = std::move(select_count);
  result->subquery_type = SubqueryType::SCALAR;
  return result;
}

unique_ptr<JoinRef> GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table,
                               const string &edge_binding,
                               const string &prev_binding,
                               const string &next_binding) {
  auto first_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
  first_join_ref->type = JoinType::INNER;

  auto second_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
  second_join_ref->type = JoinType::INNER;

  second_join_ref->left = edge_table->CreateBaseTableRef(edge_binding);
  second_join_ref->right =
      edge_table->source_pg_table->CreateBaseTableRef(prev_binding);
  auto t_from_ref =
      make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_binding);
  auto src_cid_ref =
      make_uniq<ColumnRefExpression>(edge_table->source_pk[0], prev_binding);
  second_join_ref->condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL, std::move(t_from_ref),
      std::move(src_cid_ref));
  first_join_ref->left = std::move(second_join_ref);
  first_join_ref->right =
      edge_table->destination_pg_table->CreateBaseTableRef(next_binding);

  auto t_to_ref = make_uniq<ColumnRefExpression>(edge_table->destination_fk[0],
                                                 edge_binding);
  auto dst_cid_ref = make_uniq<ColumnRefExpression>(
      edge_table->destination_pk[0], next_binding);
  first_join_ref->condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL, std::move(t_to_ref),
      std::move(dst_cid_ref));
  return first_join_ref;
}

unique_ptr<SubqueryExpression> CreateDirectedCSRVertexSubquery(
    const shared_ptr<PropertyGraphTable> &edge_table,
    const string &prev_binding) {
  auto count_create_vertex_expr = GetCountTable(
      edge_table->source_pg_table, prev_binding, edge_table->source_pk[0]);

  vector<unique_ptr<ParsedExpression>> csr_vertex_children;
  csr_vertex_children.push_back(
      make_uniq<ConstantExpression>(Value::INTEGER(0)));
  csr_vertex_children.push_back(std::move(count_create_vertex_expr));
  csr_vertex_children.push_back(
      make_uniq<ColumnRefExpression>("dense_id", "sub"));
  csr_vertex_children.push_back(make_uniq<ColumnRefExpression>("cnt", "sub"));
  auto create_vertex_function = make_uniq<FunctionExpression>(
      "create_csr_vertex", std::move(csr_vertex_children));

  vector<unique_ptr<ParsedExpression>> sum_children;
  sum_children.push_back(std::move(create_vertex_function));
  auto sum_function =
      make_uniq<FunctionExpression>("sum", std::move(sum_children));

  auto inner_select_statement = make_uniq<SelectStatement>();
  auto inner_select_node = make_uniq<SelectNode>();

  inner_select_node->select_list.emplace_back(
      CreateColumnRefExpression("rowid", prev_binding, "dense_id"));
  auto edge_src_colref = make_uniq<ColumnRefExpression>(
      edge_table->source_fk[0], edge_table->table_name);
  vector<unique_ptr<ParsedExpression>> count_children;
  count_children.push_back(std::move(edge_src_colref));
  auto count_function =
      make_uniq<FunctionExpression>("count", std::move(count_children));
  count_function->alias = "cnt";
  inner_select_node->select_list.emplace_back(std::move(count_function));

  auto left_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
  left_join_ref->type = JoinType::LEFT;
  left_join_ref->left =
      edge_table->source_pg_table->CreateBaseTableRef(prev_binding);
  left_join_ref->right =
      edge_table->CreateBaseTableRef(edge_table->table_name_alias);

  auto join_condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL,
      make_uniq<ColumnRefExpression>(edge_table->source_fk[0],
                                     edge_table->table_name),
      make_uniq<ColumnRefExpression>(edge_table->source_pk[0], prev_binding));
  left_join_ref->condition = std::move(join_condition);
  inner_select_node->from_table = std::move(left_join_ref);

  auto dense_id_colref = make_uniq<ColumnRefExpression>("dense_id");
  inner_select_node->groups.group_expressions.push_back(
      std::move(dense_id_colref));
  GroupingSet grouping_set = {0};
  inner_select_node->groups.grouping_sets.push_back(grouping_set);

  inner_select_statement->node = std::move(inner_select_node);

  auto inner_from_subquery =
      make_uniq<SubqueryRef>(std::move(inner_select_statement), "sub");

  auto cast_select_node = make_uniq<SelectNode>();
  cast_select_node->from_table = std::move(inner_from_subquery);
  cast_select_node->select_list.push_back(std::move(sum_function));

  auto cast_select_stmt = make_uniq<SelectStatement>();
  cast_select_stmt->node = std::move(cast_select_node);

  auto cast_subquery_expr = make_uniq<SubqueryExpression>();
  cast_subquery_expr->subquery = std::move(cast_select_stmt);
  cast_subquery_expr->subquery_type = SubqueryType::SCALAR;

  return cast_subquery_expr;
}

// Helper function to create CSR Vertex Subquery
unique_ptr<SubqueryExpression> CreateUndirectedCSRVertexSubquery(
    const shared_ptr<PropertyGraphTable> &edge_table, const string &binding) {
  auto count_create_vertex_expr = GetCountTable(
      edge_table->source_pg_table, binding, edge_table->source_pk[0]);

  vector<unique_ptr<ParsedExpression>> csr_vertex_children;
  csr_vertex_children.push_back(
      make_uniq<ConstantExpression>(Value::INTEGER(0)));
  csr_vertex_children.push_back(std::move(count_create_vertex_expr));
  csr_vertex_children.push_back(
      make_uniq<ColumnRefExpression>("dense_id", "sub"));
  csr_vertex_children.push_back(make_uniq<ColumnRefExpression>("cnt", "sub"));

  auto create_vertex_function = make_uniq<FunctionExpression>(
      "create_csr_vertex", std::move(csr_vertex_children));
  vector<unique_ptr<ParsedExpression>> sum_children;
  sum_children.push_back(std::move(create_vertex_function));
  auto sum_function =
      make_uniq<FunctionExpression>("sum", std::move(sum_children));

  vector<unique_ptr<ParsedExpression>> multiply_csr_vertex_children;
  auto two_constant = make_uniq<ConstantExpression>(Value::INTEGER(2));
  multiply_csr_vertex_children.push_back(std::move(two_constant));
  multiply_csr_vertex_children.push_back(std::move(sum_function));

  auto multiply_function = make_uniq<FunctionExpression>(
      "multiply", std::move(multiply_csr_vertex_children));

  auto inner_select_statement = make_uniq<SelectStatement>();
  auto inner_select_node = make_uniq<SelectNode>();

  auto dense_id_ref = make_uniq<ColumnRefExpression>("dense_id");

  auto count_create_inner_expr = make_uniq<SubqueryExpression>();
  count_create_inner_expr->subquery_type = SubqueryType::SCALAR;
  auto outgoing_edges_ref = make_uniq<ColumnRefExpression>("outgoing_edges");
  vector<unique_ptr<ParsedExpression>> inner_count_children;
  inner_count_children.push_back(std::move(outgoing_edges_ref));
  auto inner_count_function =
      make_uniq<FunctionExpression>("count", std::move(inner_count_children));
  inner_count_function->alias = "cnt";

  inner_select_node->select_list.push_back(std::move(dense_id_ref));
  inner_select_node->select_list.push_back(std::move(inner_count_function));

  auto dense_id_colref = make_uniq<ColumnRefExpression>("dense_id");
  inner_select_node->groups.group_expressions.push_back(
      std::move(dense_id_colref));
  GroupingSet grouping_set = {0};
  inner_select_node->groups.grouping_sets.push_back(grouping_set);

  unique_ptr<SelectNode> unique_edges_select_node,
      unique_edges_select_node_reverse;

  SetupSelectNode(unique_edges_select_node, edge_table, false);
  SetupSelectNode(unique_edges_select_node_reverse, edge_table, true);

  auto union_all_node = make_uniq<SetOperationNode>();
  union_all_node->setop_type = SetOperationType::UNION_BY_NAME;
  union_all_node->left = std::move(unique_edges_select_node);
  union_all_node->right = std::move(unique_edges_select_node_reverse);

  auto subquery_select_statement = make_uniq<SelectStatement>();
  subquery_select_statement->node = std::move(union_all_node);
  auto unique_edges_subquery = make_uniq<SubqueryRef>(
      std::move(subquery_select_statement), "unique_edges");

  inner_select_node->from_table = std::move(unique_edges_subquery);
  inner_select_statement->node = std::move(inner_select_node);

  auto inner_from_subquery =
      make_uniq<SubqueryRef>(std::move(inner_select_statement), "sub");

  auto cast_select_node = make_uniq<SelectNode>();
  cast_select_node->from_table = std::move(inner_from_subquery);
  cast_select_node->select_list.push_back(std::move(multiply_function));

  auto cast_select_stmt = make_uniq<SelectStatement>();
  cast_select_stmt->node = std::move(cast_select_node);

  auto cast_subquery_expr = make_uniq<SubqueryExpression>();
  cast_subquery_expr->subquery = std::move(cast_select_stmt);
  cast_subquery_expr->subquery_type = SubqueryType::SCALAR;

  return cast_subquery_expr;
}

// Helper function to create outer select edges node
unique_ptr<SelectNode> CreateOuterSelectEdgesNode() {
  auto outer_select_edges_node = make_uniq<SelectNode>();
  outer_select_edges_node->select_list.push_back(
      make_uniq<ColumnRefExpression>("src"));
  outer_select_edges_node->select_list.push_back(
      make_uniq<ColumnRefExpression>("dst"));

  vector<unique_ptr<ParsedExpression>> any_value_children;
  any_value_children.push_back(make_uniq<ColumnRefExpression>("edges"));
  auto any_value_function =
      make_uniq<FunctionExpression>("any_value", std::move(any_value_children));
  any_value_function->alias = "edge";
  outer_select_edges_node->select_list.push_back(std::move(any_value_function));

  outer_select_edges_node->groups.group_expressions.push_back(
      make_uniq<ColumnRefExpression>("src"));
  outer_select_edges_node->groups.group_expressions.push_back(
      make_uniq<ColumnRefExpression>("dst"));
  GroupingSet outer_grouping_set = {0, 1};
  outer_select_edges_node->groups.grouping_sets.push_back(outer_grouping_set);

  return outer_select_edges_node;
}

// Helper function to create outer select node
unique_ptr<SelectNode>
CreateOuterSelectNode(unique_ptr<FunctionExpression> create_csr_edge_function) {
  auto outer_select_node = make_uniq<SelectNode>();
  create_csr_edge_function->alias = "temp";
  outer_select_node->select_list.push_back(std::move(create_csr_edge_function));
  return outer_select_node;
}

// Function to create the CTE for the edges
unique_ptr<CommonTableExpressionInfo>
MakeEdgesCTE(const shared_ptr<PropertyGraphTable> &edge_table) {
  std::vector<unique_ptr<ParsedExpression>> select_expression;
  auto src_col_ref = make_uniq<ColumnRefExpression>("rowid", "src_table");
  src_col_ref->alias = "src";

  select_expression.emplace_back(std::move(src_col_ref));

  auto dst_col_ref = make_uniq<ColumnRefExpression>("rowid", "dst_table");
  dst_col_ref->alias = "dst";
  select_expression.emplace_back(std::move(dst_col_ref));

  auto edge_col_ref =
      make_uniq<ColumnRefExpression>("rowid", edge_table->table_name);
  edge_col_ref->alias = "edges";
  select_expression.emplace_back(std::move(edge_col_ref));

  auto select_node = make_uniq<SelectNode>();
  select_node->select_list = std::move(select_expression);

  auto join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
  auto first_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
  first_join_ref->type = JoinType::INNER;
  first_join_ref->left = edge_table->CreateBaseTableRef();
  first_join_ref->right =
      edge_table->source_pg_table->CreateBaseTableRef("src_table");

  auto edge_from_ref = make_uniq<ColumnRefExpression>(edge_table->source_fk[0],
                                                      edge_table->table_name);
  auto src_cid_ref =
      make_uniq<ColumnRefExpression>(edge_table->source_pk[0], "src_table");
  first_join_ref->condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL, std::move(edge_from_ref),
      std::move(src_cid_ref));

  auto second_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
  second_join_ref->type = JoinType::INNER;
  second_join_ref->left = std::move(first_join_ref);
  second_join_ref->right =
      edge_table->destination_pg_table->CreateBaseTableRef("dst_table");

  auto edge_to_ref = make_uniq<ColumnRefExpression>(
      edge_table->destination_fk[0], edge_table->table_name);
  auto dst_cid_ref = make_uniq<ColumnRefExpression>(
      edge_table->destination_pk[0], "dst_table");
  second_join_ref->condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL, std::move(edge_to_ref),
      std::move(dst_cid_ref));

  select_node->from_table = std::move(second_join_ref);

  auto select_statement = make_uniq<SelectStatement>();
  select_statement->node = std::move(select_node);

  auto result = make_uniq<CommonTableExpressionInfo>();
  result->query = std::move(select_statement);
  return result;
}

// Function to create the CTE for the Undirected CSR
unique_ptr<CommonTableExpressionInfo>
CreateUndirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                       const unique_ptr<SelectNode> &select_node) {
  if (select_node->cte_map.map.find("edges_cte") ==
      select_node->cte_map.map.end()) {
    select_node->cte_map.map["edges_cte"] = MakeEdgesCTE(edge_table);
  }

  auto csr_edge_id_constant = make_uniq<ConstantExpression>(Value::INTEGER(0));
  auto count_create_edge_select =
      GetCountTable(edge_table->source_pg_table, edge_table->source_reference,
                    edge_table->source_pk[0]);

  auto count_edges_subquery = GetCountUndirectedEdgeTable();

  auto cast_subquery_expr = CreateUndirectedCSRVertexSubquery(
      edge_table, edge_table->source_reference);

  auto src_rowid_colref = make_uniq<ColumnRefExpression>("src");
  auto dst_rowid_colref = make_uniq<ColumnRefExpression>("dst");
  auto edge_rowid_colref = make_uniq<ColumnRefExpression>("edge");

  auto cast_expression = make_uniq<CastExpression>(
      LogicalType::BIGINT, std::move(cast_subquery_expr));

  vector<unique_ptr<ParsedExpression>> csr_edge_children;
  csr_edge_children.push_back(std::move(csr_edge_id_constant));
  csr_edge_children.push_back(std::move(count_create_edge_select));
  csr_edge_children.push_back(std::move(cast_expression));
  csr_edge_children.push_back(std::move(count_edges_subquery));
  csr_edge_children.push_back(std::move(src_rowid_colref));
  csr_edge_children.push_back(std::move(dst_rowid_colref));
  csr_edge_children.push_back(std::move(edge_rowid_colref));

  auto create_csr_edge_function = make_uniq<FunctionExpression>(
      "create_csr_edge", std::move(csr_edge_children));
  auto outer_select_node =
      CreateOuterSelectNode(std::move(create_csr_edge_function));

  auto outer_select_edges_node = CreateOuterSelectEdgesNode();

  auto outer_union_all_node = make_uniq<SetOperationNode>();
  outer_union_all_node->setop_all = true;
  outer_union_all_node->setop_type = SetOperationType::UNION;

  auto src_dst_select_node = make_uniq<SelectNode>();

  src_dst_select_node->from_table = std::move(CreateBaseTableRef("edges_cte"));
  src_dst_select_node->select_list.push_back(
      make_uniq<ColumnRefExpression>("src"));
  src_dst_select_node->select_list.push_back(
      make_uniq<ColumnRefExpression>("dst"));
  src_dst_select_node->select_list.push_back(
      make_uniq<ColumnRefExpression>("edges"));

  auto dst_src_select_node = make_uniq<SelectNode>();
  dst_src_select_node->from_table = std::move(CreateBaseTableRef("edges_cte"));
  dst_src_select_node->select_list.push_back(
      make_uniq<ColumnRefExpression>("dst"));
  dst_src_select_node->select_list.push_back(
      make_uniq<ColumnRefExpression>("src"));
  dst_src_select_node->select_list.push_back(
      make_uniq<ColumnRefExpression>("edges"));

  outer_union_all_node->left = std::move(src_dst_select_node);
  outer_union_all_node->right = std::move(dst_src_select_node);

  auto outer_union_select_statement = make_uniq<SelectStatement>();
  outer_union_select_statement->node = std::move(outer_union_all_node);
  outer_select_edges_node->from_table =
      make_uniq<SubqueryRef>(std::move(outer_union_select_statement));

  auto outer_select_edges_select_statement = make_uniq<SelectStatement>();
  outer_select_edges_select_statement->node =
      std::move(outer_select_edges_node);
  outer_select_node->from_table =
      make_uniq<SubqueryRef>(std::move(outer_select_edges_select_statement));

  auto outer_select_statement = make_uniq<SelectStatement>();
  outer_select_statement->node = std::move(outer_select_node);
  auto info = make_uniq<CommonTableExpressionInfo>();
  info->query = std::move(outer_select_statement);
  return info;
}

unique_ptr<SubqueryExpression> GetCountUndirectedEdgeTable() {
  auto count_edges_select_statement = make_uniq<SelectStatement>();
  auto count_edges_select_node = make_uniq<SelectNode>();
  vector<unique_ptr<ParsedExpression>> count_children;
  auto count_function =
      make_uniq<FunctionExpression>("count", std::move(count_children));
  vector<unique_ptr<ParsedExpression>> multiply_children;
  auto constant_two = make_uniq<ConstantExpression>(Value::BIGINT(2));
  multiply_children.push_back(std::move(constant_two));
  multiply_children.push_back(std::move(count_function));
  auto multiply_function =
      make_uniq<FunctionExpression>("multiply", std::move(multiply_children));
  count_edges_select_node->select_list.emplace_back(
      std::move(multiply_function));

  auto inner_select_statement = make_uniq<SelectStatement>();

  auto src_dst_select_node = make_uniq<SelectNode>();
  src_dst_select_node->select_list.emplace_back(
      CreateColumnRefExpression("src"));
  src_dst_select_node->select_list.emplace_back(
      CreateColumnRefExpression("dst"));

  src_dst_select_node->from_table = std::move(CreateBaseTableRef("edges_cte"));

  auto dst_src_select_node = make_uniq<SelectNode>();
  dst_src_select_node->select_list.emplace_back(
      CreateColumnRefExpression("dst", "", "src"));
  dst_src_select_node->select_list.emplace_back(
      CreateColumnRefExpression("src", "", "dst"));
  dst_src_select_node->from_table = CreateBaseTableRef("edges_cte");

  auto union_by_name_node = make_uniq<SetOperationNode>();
  union_by_name_node->setop_all = false;
  union_by_name_node->setop_type = SetOperationType::UNION_BY_NAME;
  union_by_name_node->left = std::move(src_dst_select_node);
  union_by_name_node->right = std::move(dst_src_select_node);
  inner_select_statement->node = std::move(union_by_name_node);
  auto inner_from_subquery =
      make_uniq<SubqueryRef>(std::move(inner_select_statement));
  count_edges_select_node->from_table = std::move(inner_from_subquery);
  count_edges_select_statement->node = std::move(count_edges_select_node);
  auto result = make_uniq<SubqueryExpression>();
  result->subquery = std::move(count_edges_select_statement);
  result->subquery_type = SubqueryType::SCALAR;
  return result;
}

unique_ptr<SubqueryExpression>
GetCountEdgeTable(const shared_ptr<PropertyGraphTable> &edge_table) {
  auto result = make_uniq<SubqueryExpression>();
  auto outer_select_statement = make_uniq<SelectStatement>();
  auto outer_select_node = make_uniq<SelectNode>();
  vector<unique_ptr<ParsedExpression>> count_children;
  outer_select_node->select_list.push_back(
      make_uniq<FunctionExpression>("count", std::move(count_children)));
  auto inner_select_node = make_uniq<SelectNode>();

  auto first_join = make_uniq<JoinRef>(JoinRefType::REGULAR);
  first_join->left = edge_table->CreateBaseTableRef();
  first_join->right = edge_table->source_pg_table->CreateBaseTableRef("src");
  first_join->condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL,
      make_uniq<ColumnRefExpression>(edge_table->source_fk[0],
                                     edge_table->table_name),
      make_uniq<ColumnRefExpression>(edge_table->source_pk[0], "src"));
  auto second_join = make_uniq<JoinRef>(JoinRefType::REGULAR);
  second_join->left = std::move(first_join);
  second_join->right =
      edge_table->destination_pg_table->CreateBaseTableRef("dst");
  second_join->condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL,
      make_uniq<ColumnRefExpression>(edge_table->destination_fk[0],
                                     edge_table->table_name),
      make_uniq<ColumnRefExpression>(edge_table->destination_pk[0], "dst"));
  outer_select_node->from_table = std::move(second_join);
  outer_select_statement->node = std::move(outer_select_node);
  result->subquery = std::move(outer_select_statement);
  result->subquery_type = SubqueryType::SCALAR;
  return result;
}

// Function to create the CTE for the Directed CSR
unique_ptr<CommonTableExpressionInfo>
CreateDirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                     const string &prev_binding, const string &edge_binding,
                     const string &next_binding) {
  auto csr_edge_id_constant = make_uniq<ConstantExpression>(Value::INTEGER(0));
  auto count_create_edge_select = GetCountTable(
      edge_table->source_pg_table, prev_binding, edge_table->source_pk[0]);

  auto cast_subquery_expr =
      CreateDirectedCSRVertexSubquery(edge_table, prev_binding);
  auto count_edge_table =
      GetCountEdgeTable(edge_table); // Count the number of edges

  auto src_rowid_colref = make_uniq<ColumnRefExpression>("rowid", prev_binding);
  auto dst_rowid_colref = make_uniq<ColumnRefExpression>("rowid", next_binding);
  auto edge_rowid_colref =
      make_uniq<ColumnRefExpression>("rowid", edge_binding);

  auto cast_expression = make_uniq<CastExpression>(
      LogicalType::BIGINT, std::move(cast_subquery_expr));

  vector<unique_ptr<ParsedExpression>> csr_edge_children;
  csr_edge_children.push_back(std::move(csr_edge_id_constant));
  csr_edge_children.push_back(std::move(count_create_edge_select));
  csr_edge_children.push_back(std::move(cast_expression));
  csr_edge_children.push_back(std::move(count_edge_table));
  csr_edge_children.push_back(std::move(src_rowid_colref));
  csr_edge_children.push_back(std::move(dst_rowid_colref));
  csr_edge_children.push_back(std::move(edge_rowid_colref));

  auto create_csr_edge_function = make_uniq<FunctionExpression>(
      "create_csr_edge", std::move(csr_edge_children));
  auto outer_select_node =
      CreateOuterSelectNode(std::move(create_csr_edge_function));

  outer_select_node->from_table =
      GetJoinRef(edge_table, edge_binding, prev_binding, next_binding);

  auto outer_select_statement = make_uniq<SelectStatement>();
  outer_select_statement->node = std::move(outer_select_node);

  auto info = make_uniq<CommonTableExpressionInfo>();
  info->query = std::move(outer_select_statement);
  return info;
}

// Function to create a subquery for counting with CTE
unique_ptr<SubqueryRef> CreateCountCTESubquery() {
  auto temp_cte_select_node = make_uniq<SelectNode>();

  auto cte_table_ref = make_uniq<BaseTableRef>();
  cte_table_ref->table_name = "csr_cte";
  temp_cte_select_node->from_table = std::move(cte_table_ref);

  vector<unique_ptr<ParsedExpression>> children;
  children.push_back(make_uniq<ColumnRefExpression>("temp", "csr_cte"));

  auto count_function =
      make_uniq<FunctionExpression>("count", std::move(children));
  auto zero = make_uniq<ConstantExpression>(Value::INTEGER((int32_t)0));

  vector<unique_ptr<ParsedExpression>> multiply_children;
  multiply_children.push_back(std::move(zero));
  multiply_children.push_back(std::move(count_function));

  auto multiply_function =
      make_uniq<FunctionExpression>("multiply", std::move(multiply_children));
  multiply_function->alias = "temp";
  temp_cte_select_node->select_list.push_back(std::move(multiply_function));

  auto temp_cte_select_statement = make_uniq<SelectStatement>();
  temp_cte_select_statement->node = std::move(temp_cte_select_node);

  return make_uniq<SubqueryRef>(std::move(temp_cte_select_statement), "__x");
}

} // namespace core

} // namespace duckpgq