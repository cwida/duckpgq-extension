#include "duckpgq/functions/tablefunctions/drop_property_graph.hpp"

#include "duckdb/parser/parsed_data/drop_property_graph_info.hpp"
#include <duckpgq/utils/duckpgq_utils.hpp>
#include <duckpgq_extension.hpp>

namespace duckdb {
duckdb::unique_ptr<FunctionData>
DropPropertyGraphFunction::DropPropertyGraphBind(
    ClientContext &context, TableFunctionBindInput &,
    vector<LogicalType> &return_types, vector<string> &names) {
  names.emplace_back("success");
  return_types.emplace_back(LogicalType::VARCHAR);
  auto duckpgq_state = GetDuckPGQState(context);

  auto duckpgq_parse_data =
      dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());

  if (!duckpgq_parse_data) {
    return {};
  }
  auto statement =
      dynamic_cast<DropStatement *>(duckpgq_parse_data->statement.get());
  auto info = dynamic_cast<DropPropertyGraphInfo *>(statement->info.get());
  return make_uniq<DropPropertyGraphBindData>(info);
}

duckdb::unique_ptr<GlobalTableFunctionState>
DropPropertyGraphFunction::DropPropertyGraphInit(ClientContext &,
                                                 TableFunctionInitInput &) {
  return make_uniq<DropPropertyGraphGlobalData>();
}

void DropPropertyGraphFunction::DropPropertyGraphFunc(
    ClientContext &context, TableFunctionInput &data_p, DataChunk &) {
  auto &bind_data = data_p.bind_data->Cast<DropPropertyGraphBindData>();

  auto pg_info = bind_data.drop_pg_info;
  auto duckpgq_state = GetDuckPGQState(context);

  auto registered_pg = duckpgq_state->registered_property_graphs.find(
      pg_info->property_graph_name);
  if (registered_pg == duckpgq_state->registered_property_graphs.end()) {
    throw BinderException("Property graph %s does not exist.",
                          pg_info->property_graph_name);
  }
  duckpgq_state->registered_property_graphs.erase(registered_pg);
}
} // namespace duckdb
