#include "duckpgq/functions/tablefunctions/local_clustering_coefficient.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckpgq_extension.hpp"

#include <duckdb/parser/tableref/subqueryref.hpp>

namespace duckdb {

unique_ptr<TableRef>
LocalClusteringCoefficientFunction::LocalClusteringCoefficientBindReplace(
    ClientContext &context, TableFunctionBindInput &input) {
  auto pg_name = StringValue::Get(input.inputs[0]);
  auto node_table = StringValue::Get(input.inputs[1]);
  auto edge_table = StringValue::Get(input.inputs[2]);

  auto lookup = context.registered_state.find("duckpgq");
  if (lookup == context.registered_state.end()) {
    throw Exception(ExceptionType::INVALID,
                    "Registered DuckPGQ state not found");
  }
  const auto duckpgq_state = dynamic_cast<DuckPGQState *>(lookup->second.get());
  auto property_graph = duckpgq_state->registered_property_graphs.find(pg_name);
  if (property_graph == duckpgq_state->registered_property_graphs.end()) {
    throw Exception(ExceptionType::INVALID,
                    "Property graph " + pg_name + " not found");
  }
  auto pg_info =
      dynamic_cast<CreatePropertyGraphInfo *>(property_graph->second.get());
  auto source_node_pg_entry = pg_info->GetTable(node_table);
  auto edge_pg_entry = pg_info->GetTable(edge_table);
  if (edge_pg_entry->source_reference != node_table) {
    throw Exception(ExceptionType::INVALID,
                    "Vertex table " + node_table +
                        " is not a source of edge table " + edge_table);
  }

  return nullptr;
}

} // namespace duckdb
