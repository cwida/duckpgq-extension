#include "duckpgq/functions/tablefunctions/describe_property_graph.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include <duckpgq_extension.hpp>

namespace duckdb {
unique_ptr<FunctionData>
DescribePropertyGraphFunction::DescribePropertyGraphBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) {
  auto lookup = context.registered_state.find("duckpgq");
  if (lookup == context.registered_state.end()) {
    throw Exception(ExceptionType::INVALID,
                    "Registered DuckPGQ state not found");
  }
  const auto duckpgq_state = dynamic_cast<DuckPGQState *>(lookup->second.get());
  const auto duckpgq_parse_data =
      dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());

  if (!duckpgq_parse_data) {
    return {};
  }
  auto statement =
      dynamic_cast<SelectStatement *>(duckpgq_parse_data->statement.get());
  auto select_node = dynamic_cast<SelectNode *>(statement->node.get());
  auto show_ref = dynamic_cast<ShowRef *>(select_node->from_table.get());

  auto pg_table =
      duckpgq_state->registered_property_graphs.find(show_ref->table_name);
  if (pg_table == duckpgq_state->registered_property_graphs.end() ) {
    throw Exception(ExceptionType::INVALID, "Property graph " + show_ref->table_name + " does not exist.");
  }
  auto property_graph = dynamic_cast<CreatePropertyGraphInfo *>(pg_table->second.get());

  names.emplace_back("table_name");
  return_types.emplace_back(LogicalType::VARCHAR);

  return make_uniq<DescribePropertyGraphBindData>(property_graph);
}

unique_ptr<GlobalTableFunctionState>
DescribePropertyGraphFunction::DescribePropertyGraphInit(
    ClientContext &context, TableFunctionInitInput &input) {
  return make_uniq<DescribePropertyGraphGlobalData>();
}

void DescribePropertyGraphFunction::DescribePropertyGraphFunc(
    ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bind_data = data_p.bind_data->Cast<DescribePropertyGraphBindData>();
  auto &data = data_p.global_state->Cast<DescribePropertyGraphGlobalData>();
  if (data.done) {
    return;
  }
  auto result_vector = Vector(LogicalType::VARCHAR);
  auto pg_info = bind_data.describe_pg_info;
  for (const auto& vertex_table : pg_info->vertex_tables) {
    output.SetValue(0, 0, Value(vertex_table->table_name));
  }
  output.SetCardinality(1);
  data.done = true;
}
}; // namespace duckdb