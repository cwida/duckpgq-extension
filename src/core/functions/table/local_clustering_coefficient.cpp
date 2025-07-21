#include "duckpgq/core/functions/table/local_clustering_coefficient.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckpgq_extension.hpp"
#include "duckpgq/core/utils/duckpgq_utils.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <duckpgq/core/functions/table.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

namespace duckpgq {

namespace core {

// Main binding function
unique_ptr<TableRef>
LocalClusteringCoefficientFunction::LocalClusteringCoefficientBindReplace(
    ClientContext &context, TableFunctionBindInput &input) {
  auto pg_name = StringUtil::Lower(StringValue::Get(input.inputs[0]));
  auto node_label = StringUtil::Lower(StringValue::Get(input.inputs[1]));
  auto edge_label = StringUtil::Lower(StringValue::Get(input.inputs[2]));

  auto duckpgq_state = GetDuckPGQState(context);
  auto pg_info = GetPropertyGraphInfo(duckpgq_state, pg_name);
  auto edge_pg_entry =
      ValidateSourceNodeAndEdgeTable(pg_info, node_label, edge_label);

  auto select_node =
      CreateSelectNode(edge_pg_entry, "local_clustering_coefficient",
                       "local_clustering_coefficient");

  select_node->cte_map.map["csr_cte"] =
      CreateUndirectedCSRCTE(edge_pg_entry, select_node);

  auto subquery = make_uniq<SelectStatement>();
  subquery->node = std::move(select_node);

  auto result = make_uniq<SubqueryRef>(std::move(subquery));
  result->alias = "lcc";
  // input.ref.alias = "lcc";
  return std::move(result);
}
//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterLocalClusteringCoefficientTableFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(db, LocalClusteringCoefficientFunction());
}

} // namespace core

} // namespace duckpgq
