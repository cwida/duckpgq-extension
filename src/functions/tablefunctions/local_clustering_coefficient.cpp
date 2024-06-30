#include "duckpgq/functions/tablefunctions/local_clustering_coefficient.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckpgq_extension.hpp"
#include "duckpgq/utils/duckpgq_utils.hpp"

#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <duckdb/parser/tableref/joinref.hpp>

namespace duckdb {

// Utility function to transform a string to lowercase
std::string ToLowerCase(const std::string &input) {
    std::string result = input;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

// Function to get PropertyGraphInfo from DuckPGQState
CreatePropertyGraphInfo* GetPropertyGraphInfo(DuckPGQState *duckpgq_state, const std::string &pg_name) {
    auto property_graph = duckpgq_state->registered_property_graphs.find(pg_name);
    if (property_graph == duckpgq_state->registered_property_graphs.end()) {
        throw Exception(ExceptionType::INVALID, "Property graph " + pg_name + " not found");
    }
    return dynamic_cast<CreatePropertyGraphInfo*>(property_graph->second.get());
}

// Function to validate the source node and edge table
shared_ptr<PropertyGraphTable> ValidateSourceNodeAndEdgeTable(CreatePropertyGraphInfo *pg_info, const std::string &node_table, const std::string &edge_table) {
    auto source_node_pg_entry = pg_info->GetTable(node_table);
    if (!source_node_pg_entry->is_vertex_table) {
        throw Exception(ExceptionType::INVALID, node_table + " is an edge table, expected a vertex table");
    }
    auto edge_pg_entry = pg_info->GetTable(edge_table);
    if (edge_pg_entry->is_vertex_table) {
        throw Exception(ExceptionType::INVALID, edge_table + " is a vertex table, expected an edge table");
    }
    if (!edge_pg_entry->IsSourceTable(node_table)) {
        throw Exception(ExceptionType::INVALID, "Vertex table " + node_table + " is not a source of edge table " + edge_table);
    }
    return edge_pg_entry;
}

// Function to create a subquery for counting with CTE
unique_ptr<SubqueryRef> CreateCountCTESubquery() {
    auto temp_cte_select_node = make_uniq<SelectNode>();

    auto cte_table_ref = make_uniq<BaseTableRef>();
    cte_table_ref->table_name = "csr_cte";
    temp_cte_select_node->from_table = std::move(cte_table_ref);

    vector<unique_ptr<ParsedExpression>> children;
    children.push_back(make_uniq<ColumnRefExpression>("temp", "csr_cte"));

    auto count_function = make_uniq<FunctionExpression>("count", std::move(children));
    auto zero = make_uniq<ConstantExpression>(Value::INTEGER((int32_t)0));

    vector<unique_ptr<ParsedExpression>> multiply_children;
    multiply_children.push_back(std::move(zero));
    multiply_children.push_back(std::move(count_function));

    auto multiply_function = make_uniq<FunctionExpression>("multiply", std::move(multiply_children));
    multiply_function->alias = "temp";
    temp_cte_select_node->select_list.push_back(std::move(multiply_function));

    auto temp_cte_select_statement = make_uniq<SelectStatement>();
    temp_cte_select_statement->node = std::move(temp_cte_select_node);

    return make_uniq<SubqueryRef>(std::move(temp_cte_select_statement), "__x");
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
void SetupSelectNode(unique_ptr<SelectNode> &select_node, const shared_ptr<PropertyGraphTable> &edge_table, bool reverse = false) {
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

// Function to create the CTE for the CSR
unique_ptr<CommonTableExpressionInfo> CreateCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table) {
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

// Function to create the SELECT node
unique_ptr<SelectNode> CreateSelectNode(const shared_ptr<PropertyGraphTable> &edge_pg_entry) {
    auto select_node = make_uniq<SelectNode>();
    std::vector<unique_ptr<ParsedExpression>> select_expression;

    select_expression.emplace_back(make_uniq<ColumnRefExpression>(edge_pg_entry->source_pk[0], edge_pg_entry->source_reference));

    auto cte_col_ref = make_uniq<ColumnRefExpression>("temp", "__x");

    vector<unique_ptr<ParsedExpression>> lcc_children;
    lcc_children.push_back(make_uniq<ConstantExpression>(Value::INTEGER(0)));
    lcc_children.push_back(make_uniq<ColumnRefExpression>("rowid", edge_pg_entry->source_reference));

    auto lcc_function = make_uniq<FunctionExpression>("local_clustering_coefficient", std::move(lcc_children));

    std::vector<unique_ptr<ParsedExpression>> addition_children;
    addition_children.emplace_back(std::move(cte_col_ref));
    addition_children.emplace_back(std::move(lcc_function));

    auto addition_function = make_uniq<FunctionExpression>("add", std::move(addition_children));
    addition_function->alias = "local_clustering_coefficient";
    select_expression.emplace_back(std::move(addition_function));
    select_node->select_list = std::move(select_expression);

    auto src_base_ref = make_uniq<BaseTableRef>();
    src_base_ref->table_name = edge_pg_entry->source_reference;

    auto temp_cte_select_subquery = CreateCountCTESubquery();

    auto cross_join_ref = make_uniq<JoinRef>(JoinRefType::CROSS);
    cross_join_ref->left = std::move(src_base_ref);
    cross_join_ref->right = std::move(temp_cte_select_subquery);

    select_node->from_table = std::move(cross_join_ref);

    return select_node;
}

// Function to create the CTE for the edges
unique_ptr<CommonTableExpressionInfo> MakeEdgesCTE(const shared_ptr<PropertyGraphTable> &edge_pg_entry) {
    std::vector<unique_ptr<ParsedExpression>> select_expression;
    auto src_col_ref = make_uniq<ColumnRefExpression>("rowid", "src_table");
    src_col_ref->alias = "src";

    select_expression.emplace_back(std::move(src_col_ref));

    auto dst_col_ref = make_uniq<ColumnRefExpression>("rowid", "dst_table");
    dst_col_ref->alias = "dst";
    select_expression.emplace_back(std::move(dst_col_ref));

    auto edge_col_ref = make_uniq<ColumnRefExpression>("rowid", edge_pg_entry->table_name);
    edge_col_ref->alias = "edges";
    select_expression.emplace_back(std::move(edge_col_ref));

    auto select_node = make_uniq<SelectNode>();
    select_node->select_list = std::move(select_expression);

    auto edge_table_ref = make_uniq<BaseTableRef>();
    edge_table_ref->table_name = edge_pg_entry->table_name;

    auto src_table_ref = make_uniq<BaseTableRef>();
    src_table_ref->table_name = edge_pg_entry->source_reference;
    src_table_ref->alias = "src_table";

    auto join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);

    auto first_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
    first_join_ref->type = JoinType::INNER;
    first_join_ref->left = std::move(edge_table_ref);
    first_join_ref->right = std::move(src_table_ref);

    auto edge_from_ref = make_uniq<ColumnRefExpression>(edge_pg_entry->source_fk[0], edge_pg_entry->table_name);
    auto src_cid_ref = make_uniq<ColumnRefExpression>(edge_pg_entry->source_pk[0], "src_table");
    first_join_ref->condition = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(edge_from_ref), std::move(src_cid_ref));

    auto dst_table_ref = make_uniq<BaseTableRef>();
    dst_table_ref->table_name = edge_pg_entry->destination_reference;
    dst_table_ref->alias = "dst_table";

    auto second_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
    second_join_ref->type = JoinType::INNER;
    second_join_ref->left = std::move(first_join_ref);
    second_join_ref->right = std::move(dst_table_ref);

    auto edge_to_ref = make_uniq<ColumnRefExpression>(edge_pg_entry->destination_fk[0], edge_pg_entry->table_name);
    auto dst_cid_ref = make_uniq<ColumnRefExpression>(edge_pg_entry->destination_pk[0], "dst_table");
    second_join_ref->condition = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(edge_to_ref), std::move(dst_cid_ref));

    select_node->from_table = std::move(second_join_ref);

    auto select_statement = make_uniq<SelectStatement>();
    select_statement->node = std::move(select_node);

    auto result = make_uniq<CommonTableExpressionInfo>();
    result->query = std::move(select_statement);
    return result;
}

// Main binding function
unique_ptr<TableRef> LocalClusteringCoefficientFunction::LocalClusteringCoefficientBindReplace(ClientContext &context, TableFunctionBindInput &input) {
    auto pg_name = ToLowerCase(StringValue::Get(input.inputs[0]));
    auto node_table = ToLowerCase(StringValue::Get(input.inputs[1]));
    auto edge_table = ToLowerCase(StringValue::Get(input.inputs[2]));

    auto duckpgq_state = GetDuckPGQState(context);
    auto pg_info = GetPropertyGraphInfo(duckpgq_state, pg_name);
    auto edge_pg_entry = ValidateSourceNodeAndEdgeTable(pg_info, node_table, edge_table);

    auto select_node = CreateSelectNode(edge_pg_entry);

    select_node->cte_map.map["edges_cte"] = MakeEdgesCTE(edge_pg_entry);
    select_node->cte_map.map["csr_cte"] = CreateCSRCTE(edge_pg_entry);

    auto subquery = make_uniq<SelectStatement>();
    subquery->node = std::move(select_node);

    auto result = make_uniq<SubqueryRef>(std::move(subquery));
    result->alias = "lcc";
    return result;
}

} // namespace duckdb
