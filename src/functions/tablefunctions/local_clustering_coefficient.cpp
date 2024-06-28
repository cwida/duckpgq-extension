#include "duckpgq/functions/tablefunctions/local_clustering_coefficient.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckpgq_extension.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
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

// Function to get DuckPGQState
DuckPGQState *GetDuckPGQState(ClientContext &context) {
    auto lookup = context.registered_state.find("duckpgq");
    if (lookup == context.registered_state.end()) {
        throw Exception(ExceptionType::INVALID, "Registered DuckPGQ state not found");
    }
    return dynamic_cast<DuckPGQState *>(lookup->second.get());
}

// Function to get PropertyGraphInfo
CreatePropertyGraphInfo *GetPropertyGraphInfo(DuckPGQState *duckpgq_state, const std::string &pg_name) {
    auto property_graph = duckpgq_state->registered_property_graphs.find(pg_name);
    if (property_graph == duckpgq_state->registered_property_graphs.end()) {
        throw Exception(ExceptionType::INVALID, "Property graph " + pg_name + " not found");
    }
    return dynamic_cast<CreatePropertyGraphInfo *>(property_graph->second.get());
}

// Function to validate the source node and edge table
shared_ptr<PropertyGraphTable> ValidateSourceNodeAndEdgeTable(CreatePropertyGraphInfo *pg_info, const std::string &node_table, const std::string &edge_table) {
    auto source_node_pg_entry = pg_info->GetTable(node_table);
    auto edge_pg_entry = pg_info->GetTable(edge_table);
    if (edge_pg_entry->source_reference != node_table) {
        throw Exception(ExceptionType::INVALID, "Vertex table " + node_table + " is not a source of edge table " + edge_table);
    }
    return edge_pg_entry;
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
    select_expression.emplace_back(std::move(addition_function));
    select_node->select_list = std::move(select_expression);

    auto src_base_ref = make_uniq<BaseTableRef>();
    src_base_ref->table_name = edge_pg_entry->source_reference;
    select_node->from_table = std::move(src_base_ref);

    return select_node;
}

// Function to create the CTE for the edges
unique_ptr<CommonTableExpressionInfo> MakeEdgesCTE(const shared_ptr<PropertyGraphTable> &edge_pg_entry) {
    std::vector<unique_ptr<ParsedExpression>> select_expression;
    auto src_col_ref = make_uniq<ColumnRefExpression>("rowid", edge_pg_entry->source_reference);
    src_col_ref->alias = "src";
    select_expression.emplace_back(std::move(src_col_ref));

    auto dst_col_ref = make_uniq<ColumnRefExpression>("rowid", edge_pg_entry->destination_reference);
    dst_col_ref->alias = "dst";
    select_expression.emplace_back(std::move(dst_col_ref));

    auto edge_col_ref = make_uniq<ColumnRefExpression>("rowid", edge_pg_entry->table_name);
    dst_col_ref->alias = "edges";
    select_expression.emplace_back(std::move(dst_col_ref));
    auto select_node = make_uniq<SelectNode>();
    select_node->select_list = std::move(select_expression);

    auto edge_table_ref = make_uniq<BaseTableRef>();
    edge_table_ref->table_name = edge_pg_entry->table_name;

    auto src_table_ref = make_uniq<BaseTableRef>();
    src_table_ref->table_name = edge_pg_entry->source_reference;

    auto join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);

    auto first_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
    first_join_ref->type = JoinType::INNER;

    first_join_ref->left = std::move(edge_table_ref);
    first_join_ref->right = std::move(src_table_ref);

    auto edge_from_ref =
        make_uniq<ColumnRefExpression>(edge_pg_entry->source_fk[0], edge_pg_entry->table_name);
    auto src_cid_ref =
        make_uniq<ColumnRefExpression>(edge_pg_entry->source_pk[0], edge_pg_entry->source_reference);
    first_join_ref->condition = make_uniq<ComparisonExpression>(
        ExpressionType::COMPARE_EQUAL, std::move(edge_from_ref),
        std::move(src_cid_ref));

    auto dst_table_ref = make_uniq<BaseTableRef>();
    dst_table_ref->table_name = edge_pg_entry->destination_reference;

    auto second_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
    second_join_ref->type = JoinType::INNER;

    second_join_ref->left = std::move(first_join_ref);
    second_join_ref->right = std::move(dst_table_ref);

    auto edge_to_ref =
        make_uniq<ColumnRefExpression>(edge_pg_entry->destination_fk[0], edge_pg_entry->table_name);
    auto dst_cid_ref =
        make_uniq<ColumnRefExpression>(edge_pg_entry->destination_pk[0], edge_pg_entry->destination_reference);
    second_join_ref->condition = make_uniq<ComparisonExpression>(
        ExpressionType::COMPARE_EQUAL, std::move(edge_to_ref),
        std::move(dst_cid_ref));

    select_node->from_table = std::move(second_join_ref);

    auto src_base_ref = make_uniq<BaseTableRef>();
    src_base_ref->table_name = edge_pg_entry->main_label;
    select_node->from_table = std::move(src_base_ref);

    auto select_statement = make_uniq<SelectStatement>();
    select_statement->node = std::move(select_node);

    auto result = make_uniq<CommonTableExpressionInfo>();
    result->query = std::move(select_statement);

    return result;
}

// Main binding function
unique_ptr<TableRef> LocalClusteringCoefficientFunction::LocalClusteringCoefficientBindReplace(
    ClientContext &context, TableFunctionBindInput &input) {

    auto pg_name = ToLowerCase(StringValue::Get(input.inputs[0]));
    auto node_table = ToLowerCase(StringValue::Get(input.inputs[1]));
    auto edge_table = ToLowerCase(StringValue::Get(input.inputs[2]));

    auto duckpgq_state = GetDuckPGQState(context);
    auto pg_info = GetPropertyGraphInfo(duckpgq_state, pg_name);
    auto edge_pg_entry = ValidateSourceNodeAndEdgeTable(pg_info, node_table, edge_table);

    auto select_node = CreateSelectNode(edge_pg_entry);


    select_node->cte_map["edges_cte"] = MakeEdgesCTE(edge_pg_entry);
    auto subquery = make_uniq<SelectStatement>();
    subquery->node = std::move(select_node);

    auto result = make_uniq<SubqueryRef>(std::move(subquery));
    result->Print(); // debug print

    return result;
}

} // namespace duckdb
