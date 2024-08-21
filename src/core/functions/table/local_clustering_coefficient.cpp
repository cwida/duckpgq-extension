#include "duckpgq/core/functions/table/local_clustering_coefficient.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckpgq_extension.hpp"
#include "duckpgq/core/utils/duckpgq_utils.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/joinref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <duckpgq/core/functions/table.hpp>

namespace duckpgq {

namespace core {

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

// Main binding function
unique_ptr<TableRef> LocalClusteringCoefficientFunction::LocalClusteringCoefficientBindReplace(ClientContext &context, TableFunctionBindInput &input) {
    auto pg_name = ToLowerCase(StringValue::Get(input.inputs[0]));
    auto node_table = ToLowerCase(StringValue::Get(input.inputs[1]));
    auto edge_table = ToLowerCase(StringValue::Get(input.inputs[2]));

    auto duckpgq_state = GetDuckPGQState(context);
    auto pg_info = GetPropertyGraphInfo(duckpgq_state, pg_name);
    auto edge_pg_entry = ValidateSourceNodeAndEdgeTable(pg_info, node_table, edge_table);

    auto select_node = CreateSelectNode(edge_pg_entry);

    select_node->cte_map.map["csr_cte"] = CreateUndirectedCSRCTE(edge_pg_entry, select_node);

    auto subquery = make_uniq<SelectStatement>();
    subquery->node = std::move(select_node);

    auto result = make_uniq<SubqueryRef>(std::move(subquery));
    result->alias = "lcc";
    return std::move(result);
}
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterLocalClusteringCoefficientTableFunction(DatabaseInstance &db) {
    ExtensionUtil::RegisterFunction(db, LocalClusteringCoefficientFunction());
}

} // namespace core

} // namespace duckdb
