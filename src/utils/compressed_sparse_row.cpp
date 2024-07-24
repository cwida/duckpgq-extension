#include "duckpgq/utils/compressed_sparse_row.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/execution/expression_executor.hpp"


namespace duckdb {
string CSR::ToString() const {
  string result;
  if (initialized_v) {
    result += "v: ";
    for (size_t i = 0; i < vsize; i++) {
      result += std::to_string(v[i].load()) + " ";
    }
  }
  result += "\n";
  if (initialized_e) {
    result += "e: ";
    for (size_t i = 0; i < e.size(); i++) {
      result += std::to_string(e[i]) + " ";
    }
  }
  result += "\n";
  if (initialized_w) {
    result += "w: ";
    for (size_t i = 0; i < w.size(); i++) {
      result += std::to_string(w[i]) + " ";
    }
  }
  return result;
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
  if (arguments.size() == 7) {
    return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                      arguments[6]->return_type);
  } else {
    auto logical_type = LogicalType::SQLNULL;
    return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(),
                                      logical_type);
  }
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

// Helper function to create a ColumnRefExpression with alias
unique_ptr<ColumnRefExpression> CreateColumnRef(const std::string &column_name, const std::string &table_name, const std::string &alias) {
  auto col_ref = make_uniq<ColumnRefExpression>(column_name, table_name);
  col_ref->alias = alias;
  return col_ref;
}

// Helper function to create a JoinRef
unique_ptr<JoinRef> CreateJoin(const std::string &fk_column, const std::string &pk_column, const std::string &table_name, const std::string &source_reference) {
  auto join = make_uniq<JoinRef>(JoinRefType::REGULAR);

  auto left_side = make_uniq<BaseTableRef>();
  left_side->table_name = source_reference;
  join->left = std::move(left_side);

  auto right_side = make_uniq<BaseTableRef>();
  right_side->table_name = table_name;
  join->right = std::move(right_side);

  join->type = JoinType::INNER;
  join->condition = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, make_uniq<ColumnRefExpression>(fk_column, table_name), make_uniq<ColumnRefExpression>(pk_column, source_reference));

  return join;
}


// Helper function to setup SelectNode
void SetupSelectNode(unique_ptr<SelectNode> &select_node, const shared_ptr<PropertyGraphTable> &edge_table, bool reverse) {
  select_node = make_uniq<SelectNode>();

  select_node->select_list.emplace_back(CreateColumnRef("rowid", edge_table->source_reference, "dense_id"));

  if (!reverse) {
    select_node->select_list.emplace_back(CreateColumnRef(edge_table->source_fk[0], edge_table->table_name, "outgoing_edges"));
    select_node->select_list.emplace_back(CreateColumnRef(edge_table->destination_fk[0], edge_table->table_name, "incoming_edges"));
    select_node->from_table = CreateJoin(edge_table->source_fk[0], edge_table->source_pk[0], edge_table->table_name, edge_table->source_reference);
  } else {
    select_node->select_list.emplace_back(CreateColumnRef(edge_table->destination_fk[0], edge_table->table_name, "outgoing_edges"));
    select_node->select_list.emplace_back(CreateColumnRef(edge_table->source_fk[0], edge_table->table_name, "incoming_edges"));
    select_node->from_table = CreateJoin(edge_table->destination_fk[0], edge_table->source_pk[0], edge_table->table_name, edge_table->source_reference);
  }
}


// Function to create a subquery expression for counting table entries
unique_ptr<SubqueryExpression> GetCountTable(const shared_ptr<PropertyGraphTable> &edge_table, const string &prev_binding) {
  auto select_count = make_uniq<SelectStatement>();
  auto select_inner = make_uniq<SelectNode>();
  auto ref = make_uniq<BaseTableRef>();

  ref->table_name = edge_table->source_reference;
  ref->alias = prev_binding;
  select_inner->from_table = std::move(ref);

  vector<unique_ptr<ParsedExpression>> children;
  children.push_back(make_uniq<ColumnRefExpression>(edge_table->source_pk[0], prev_binding));

  auto count_function = make_uniq<FunctionExpression>("count", std::move(children));
  select_inner->select_list.push_back(std::move(count_function));
  select_count->node = std::move(select_inner);

  auto result = make_uniq<SubqueryExpression>();
  result->subquery = std::move(select_count);
  result->subquery_type = SubqueryType::SCALAR;
  return result;
}

unique_ptr<JoinRef>
GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table,
                             const string &edge_binding,
                             const string &prev_binding,
                             const string &next_binding) {
  auto first_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
  first_join_ref->type = JoinType::INNER;

  auto second_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
  second_join_ref->type = JoinType::INNER;

  auto edge_base_ref = make_uniq<BaseTableRef>();
  edge_base_ref->table_name = edge_table->table_name;
  edge_base_ref->alias = edge_binding;
  auto src_base_ref = make_uniq<BaseTableRef>();
  src_base_ref->table_name = edge_table->source_reference;
  src_base_ref->alias = prev_binding;
  second_join_ref->left = std::move(edge_base_ref);
  second_join_ref->right = std::move(src_base_ref);
  auto t_from_ref =
      make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_binding);
  auto src_cid_ref =
      make_uniq<ColumnRefExpression>(edge_table->source_pk[0], prev_binding);
  second_join_ref->condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL, std::move(t_from_ref),
      std::move(src_cid_ref));
  auto dst_base_ref = make_uniq<BaseTableRef>();
  dst_base_ref->table_name = edge_table->destination_reference;
  dst_base_ref->alias = next_binding;
  first_join_ref->left = std::move(second_join_ref);
  first_join_ref->right = std::move(dst_base_ref);

  auto t_to_ref = make_uniq<ColumnRefExpression>(edge_table->destination_fk[0],
                                                 edge_binding);
  auto dst_cid_ref = make_uniq<ColumnRefExpression>(
      edge_table->destination_pk[0], next_binding);
  first_join_ref->condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL, std::move(t_to_ref),
      std::move(dst_cid_ref));
  return first_join_ref;
}

// Function to create the CTE for the CSR
unique_ptr<CommonTableExpressionInfo> CreateUndirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table) {
    auto csr_edge_id_constant = make_uniq<ConstantExpression>(Value::INTEGER(0));
    auto count_create_edge_select = GetCountTable(edge_table, edge_table->source_reference);

    auto cast_subquery_expr = make_uniq<SubqueryExpression>();
    auto cast_select_node = make_uniq<SelectNode>();

    vector<unique_ptr<ParsedExpression>> csr_vertex_children;
    csr_vertex_children.push_back(make_uniq<ConstantExpression>(Value::INTEGER(0)));
    csr_vertex_children.push_back(GetCountTable(edge_table, edge_table->source_reference));
    csr_vertex_children.push_back(make_uniq<ColumnRefExpression>("dense_id", "sub"));
    csr_vertex_children.push_back(make_uniq<ColumnRefExpression>("cnt", "sub"));

    auto create_vertex_function = make_uniq<FunctionExpression>("create_csr_vertex", std::move(csr_vertex_children));
    vector<unique_ptr<ParsedExpression>> sum_children;
    sum_children.push_back(std::move(create_vertex_function));
    auto sum_function = make_uniq<FunctionExpression>("sum", std::move(sum_children));

    vector<unique_ptr<ParsedExpression>> multiply_csr_vertex_children;
    auto two_constant = make_uniq<ConstantExpression>(Value::INTEGER(2));
    multiply_csr_vertex_children.push_back(std::move(two_constant));
    multiply_csr_vertex_children.push_back(std::move(sum_function));

    auto multiply_function = make_uniq<FunctionExpression>("multiply", std::move(multiply_csr_vertex_children));

    auto inner_select_statement = make_uniq<SelectStatement>();
    auto inner_select_node = make_uniq<SelectNode>();

    auto dense_id_ref = make_uniq<ColumnRefExpression>("dense_id");

    auto count_create_inner_expr = make_uniq<SubqueryExpression>();
    count_create_inner_expr->subquery_type = SubqueryType::SCALAR;
    auto outgoing_edges_ref = make_uniq<ColumnRefExpression>("outgoing_edges");
    vector<unique_ptr<ParsedExpression>> inner_count_children;
    inner_count_children.push_back(std::move(outgoing_edges_ref));
    auto inner_count_function = make_uniq<FunctionExpression>("count", std::move(inner_count_children));
    inner_count_function->alias = "cnt";

    inner_select_node->select_list.push_back(std::move(dense_id_ref));
    inner_select_node->select_list.push_back(std::move(inner_count_function));

    auto dense_id_colref = make_uniq<ColumnRefExpression>("dense_id");
    inner_select_node->groups.group_expressions.push_back(std::move(dense_id_colref));
    GroupingSet grouping_set = {0};
    inner_select_node->groups.grouping_sets.push_back(grouping_set);

    unique_ptr<SelectNode> unique_edges_select_node, unique_edges_select_node_reverse;

    SetupSelectNode(unique_edges_select_node, edge_table);
    SetupSelectNode(unique_edges_select_node_reverse, edge_table, true);

    auto union_all_node = make_uniq<SetOperationNode>();
    union_all_node->setop_type = SetOperationType::UNION_BY_NAME;
    union_all_node->left = std::move(unique_edges_select_node);
    union_all_node->right = std::move(unique_edges_select_node_reverse);

    auto subquery_select_statement = make_uniq<SelectStatement>();
    subquery_select_statement->node = std::move(union_all_node);
    auto unique_edges_subquery = make_uniq<SubqueryRef>(std::move(subquery_select_statement), "unique_edges");

    inner_select_node->from_table = std::move(unique_edges_subquery);
    inner_select_statement->node = std::move(inner_select_node);

    auto inner_from_subquery = make_uniq<SubqueryRef>(std::move(inner_select_statement), "sub");

    cast_select_node->from_table = std::move(inner_from_subquery);
    cast_select_node->select_list.push_back(std::move(multiply_function));

    auto cast_select_stmt = make_uniq<SelectStatement>();
    cast_select_stmt->node = std::move(cast_select_node);
    cast_subquery_expr->subquery = std::move(cast_select_stmt);
    cast_subquery_expr->subquery_type = SubqueryType::SCALAR;

    auto src_rowid_colref = make_uniq<ColumnRefExpression>("src");
    auto dst_rowid_colref = make_uniq<ColumnRefExpression>("dst");
    auto edge_rowid_colref = make_uniq<ColumnRefExpression>("edge");

    auto cast_expression = make_uniq<CastExpression>(LogicalType::BIGINT, std::move(cast_subquery_expr));

    vector<unique_ptr<ParsedExpression>> csr_edge_children;
    csr_edge_children.push_back(std::move(csr_edge_id_constant));
    csr_edge_children.push_back(std::move(count_create_edge_select));
    csr_edge_children.push_back(std::move(cast_expression));
    csr_edge_children.push_back(std::move(src_rowid_colref));
    csr_edge_children.push_back(std::move(dst_rowid_colref));
    csr_edge_children.push_back(std::move(edge_rowid_colref));

    auto outer_select_node = make_uniq<SelectNode>();

    auto create_csr_edge_function = make_uniq<FunctionExpression>("create_csr_edge", std::move(csr_edge_children));
    create_csr_edge_function->alias = "temp";

    outer_select_node->select_list.push_back(std::move(create_csr_edge_function));

    auto outer_select_edges_node = make_uniq<SelectNode>();
    outer_select_edges_node->select_list.push_back(make_uniq<ColumnRefExpression>("src"));
    outer_select_edges_node->select_list.push_back(make_uniq<ColumnRefExpression>("dst"));

    vector<unique_ptr<ParsedExpression>> any_value_children;
    any_value_children.push_back(make_uniq<ColumnRefExpression>("edges"));
    auto any_value_function = make_uniq<FunctionExpression>("any_value", std::move(any_value_children));
    any_value_function->alias = "edge";
    outer_select_edges_node->select_list.push_back(std::move(any_value_function));

    outer_select_edges_node->groups.group_expressions.push_back(make_uniq<ColumnRefExpression>("src"));
    outer_select_edges_node->groups.group_expressions.push_back(make_uniq<ColumnRefExpression>("dst"));
    GroupingSet outer_grouping_set = {0, 1};
    outer_select_edges_node->groups.grouping_sets.push_back(outer_grouping_set);

    auto outer_union_all_node = make_uniq<SetOperationNode>();
    outer_union_all_node->setop_all = true;
    outer_union_all_node->setop_type = SetOperationType::UNION;

    auto src_dst_select_node = make_uniq<SelectNode>();
    auto edges_cte_tableref = make_uniq<BaseTableRef>();
    edges_cte_tableref->table_name = "edges_cte";
    src_dst_select_node->from_table = std::move(edges_cte_tableref);
    src_dst_select_node->select_list.push_back(make_uniq<ColumnRefExpression>("src"));
    src_dst_select_node->select_list.push_back(make_uniq<ColumnRefExpression>("dst"));
    src_dst_select_node->select_list.push_back(make_uniq<ColumnRefExpression>("edges"));

    auto dst_src_select_node = make_uniq<SelectNode>();
    auto dst_edges_cte_tableref = make_uniq<BaseTableRef>();
    dst_edges_cte_tableref->table_name = "edges_cte";
    dst_src_select_node->from_table = std::move(dst_edges_cte_tableref);
    dst_src_select_node->select_list.push_back(make_uniq<ColumnRefExpression>("dst"));
    dst_src_select_node->select_list.push_back(make_uniq<ColumnRefExpression>("src"));
    dst_src_select_node->select_list.push_back(make_uniq<ColumnRefExpression>("edges"));

    outer_union_all_node->left = std::move(src_dst_select_node);
    outer_union_all_node->right = std::move(dst_src_select_node);

    auto outer_union_select_statement = make_uniq<SelectStatement>();
    outer_union_select_statement->node = std::move(outer_union_all_node);
    outer_select_edges_node->from_table = make_uniq<SubqueryRef>(std::move(outer_union_select_statement));

    auto outer_select_edges_select_statement = make_uniq<SelectStatement>();
    outer_select_edges_select_statement->node = std::move(outer_select_edges_node);
    outer_select_node->from_table = make_uniq<SubqueryRef>(std::move(outer_select_edges_select_statement));

    auto outer_select_statement = make_uniq<SelectStatement>();
    outer_select_statement->node = std::move(outer_select_node);

    auto info = make_uniq<CommonTableExpressionInfo>();
    info->query = std::move(outer_select_statement);
    return info;
}

unique_ptr<CommonTableExpressionInfo>
CreateDirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                               const string &prev_binding,
                               const string &edge_binding,
                               const string &next_binding) {
  auto csr_edge_id_constant = make_uniq<ConstantExpression>(Value::INTEGER(0));
  auto count_create_edge_select = GetCountTable(edge_table, prev_binding);

  auto cast_subquery_expr = make_uniq<SubqueryExpression>();
  auto cast_select_node = make_uniq<SelectNode>();

  vector<unique_ptr<ParsedExpression>> csr_vertex_children;
  csr_vertex_children.push_back(
      make_uniq<ConstantExpression>(Value::INTEGER(0)));

  auto count_create_vertex_expr = GetCountTable(edge_table, prev_binding);

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

  auto source_rowid_colref =
      make_uniq<ColumnRefExpression>("rowid", prev_binding);
  source_rowid_colref->alias = "dense_id";

  auto count_create_inner_expr = make_uniq<SubqueryExpression>();
  count_create_inner_expr->subquery_type = SubqueryType::SCALAR;
  auto edge_src_colref =
      make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_binding);
  vector<unique_ptr<ParsedExpression>> inner_count_children;
  inner_count_children.push_back(std::move(edge_src_colref));
  auto inner_count_function =
      make_uniq<FunctionExpression>("count", std::move(inner_count_children));
  inner_count_function->alias = "cnt";

  inner_select_node->select_list.push_back(std::move(source_rowid_colref));
  inner_select_node->select_list.push_back(std::move(inner_count_function));
  auto source_rowid_colref_1 =
      make_uniq<ColumnRefExpression>("rowid", prev_binding);
  expression_map_t<idx_t> grouping_expression_map;
  inner_select_node->groups.group_expressions.push_back(
      std::move(source_rowid_colref_1));
  GroupingSet grouping_set = {0};
  inner_select_node->groups.grouping_sets.push_back(grouping_set);

  auto inner_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
  inner_join_ref->type = JoinType::LEFT;
  auto left_base_ref = make_uniq<BaseTableRef>();
  left_base_ref->table_name = edge_table->source_reference;
  left_base_ref->alias = prev_binding;
  auto right_base_ref = make_uniq<BaseTableRef>();
  right_base_ref->table_name = edge_table->table_name;
  right_base_ref->alias = edge_binding;
  inner_join_ref->left = std::move(left_base_ref);
  inner_join_ref->right = std::move(right_base_ref);

  auto edge_join_colref =
      make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_binding);
  auto vertex_join_colref =
      make_uniq<ColumnRefExpression>(edge_table->source_pk[0], prev_binding);

  inner_join_ref->condition = make_uniq<ComparisonExpression>(
      ExpressionType::COMPARE_EQUAL, std::move(edge_join_colref),
      std::move(vertex_join_colref));
  inner_select_node->from_table = std::move(inner_join_ref);
  inner_select_statement->node = std::move(inner_select_node);

  auto inner_from_subquery =
      make_uniq<SubqueryRef>(std::move(inner_select_statement), "sub");

  cast_select_node->from_table = std::move(inner_from_subquery);

  cast_select_node->select_list.push_back(std::move(sum_function));
  auto cast_select_stmt = make_uniq<SelectStatement>();
  cast_select_stmt->node = std::move(cast_select_node);
  cast_subquery_expr->subquery = std::move(cast_select_stmt);
  cast_subquery_expr->subquery_type = SubqueryType::SCALAR;

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
  csr_edge_children.push_back(std::move(src_rowid_colref));
  csr_edge_children.push_back(std::move(dst_rowid_colref));
  csr_edge_children.push_back(std::move(edge_rowid_colref));

  auto outer_select_node = make_uniq<SelectNode>();

  auto create_csr_edge_function = make_uniq<FunctionExpression>(
      "create_csr_edge", std::move(csr_edge_children));
  create_csr_edge_function->alias = "temp";

  outer_select_node->select_list.push_back(std::move(create_csr_edge_function));
  outer_select_node->from_table =
      GetJoinRef(edge_table, edge_binding, prev_binding, next_binding);
  auto outer_select_statement = make_uniq<SelectStatement>();

  outer_select_statement->node = std::move(outer_select_node);
  auto info = make_uniq<CommonTableExpressionInfo>();
  info->query = std::move(outer_select_statement);
  return info;
}


} // namespace duckdb