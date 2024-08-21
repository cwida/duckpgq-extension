#include "duckpgq/core/utils/duckpgq_utils.hpp"
#include "duckpgq/common.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"

#include "duckpgq/core/functions/table/describe_property_graph.hpp"
#include "duckpgq/core/functions/table/drop_property_graph.hpp"

namespace duckpgq {

namespace core {
// Function to get DuckPGQState from ClientContext
DuckPGQState * GetDuckPGQState(ClientContext &context) {
  auto lookup = context.registered_state.find("duckpgq");
  if (lookup == context.registered_state.end()) {
    throw Exception(ExceptionType::INVALID, "Registered DuckPGQ state not found");
  }
  return dynamic_cast<DuckPGQState*>(lookup->second.get());
}

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




} // namespace core
} // namespace duckpgq