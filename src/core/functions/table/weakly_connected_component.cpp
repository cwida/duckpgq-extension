#include "duckpgq/core/functions/table/weakly_connected_component.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

#include <duckpgq/core/functions/table.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckpgq {
namespace core {

// Main binding function
unique_ptr<TableRef>
WeaklyConnectedComponentFunction::WeaklyConnectedComponentBindReplace(
    ClientContext &context, TableFunctionBindInput &input) {
  auto pg_name = StringUtil::Lower(StringValue::Get(input.inputs[0]));
  auto node_table = StringUtil::Lower(StringValue::Get(input.inputs[1]));
  auto edge_table = StringUtil::Lower(StringValue::Get(input.inputs[2]));

  auto duckpgq_state = GetDuckPGQState(context);
  auto pg_info = GetPropertyGraphInfo(duckpgq_state, pg_name);
  auto edge_pg_entry =
      ValidateSourceNodeAndEdgeTable(pg_info, node_table, edge_table);

  auto select_node = CreateSelectNode(
      edge_pg_entry, "weakly_connected_component", "componentId");

  select_node->cte_map.map["csr_cte"] =
      CreateUndirectedCSRCTE(edge_pg_entry, select_node);

  auto subquery = make_uniq<SelectStatement>();
  subquery->node = std::move(select_node);

  auto result = make_uniq<SubqueryRef>(std::move(subquery));
  result->alias = "wcc";
  return std::move(result);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterWeaklyConnectedComponentTableFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(db, WeaklyConnectedComponentFunction());
}

} // namespace core
} // namespace duckpgq
