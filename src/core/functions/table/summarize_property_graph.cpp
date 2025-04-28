#include "duckpgq/core/functions/table/summarize_property_graph.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include <duckpgq/core/functions/table.hpp>
#include <duckpgq/core/parser/duckpgq_parser.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <duckpgq_extension.hpp>

namespace duckpgq {
namespace core {

unique_ptr<FunctionData>
SummarizePropertyGraphFunction::SummarizePropertyGraphBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) {
  auto duckpgq_state = GetDuckPGQState(context);

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
  if (pg_table == duckpgq_state->registered_property_graphs.end()) {
    throw Exception(ExceptionType::INVALID, "Property graph " +
                                                show_ref->table_name +
                                                " does not exist.");
  }
  auto property_graph =
      dynamic_cast<CreatePropertyGraphInfo *>(pg_table->second.get());

  return make_uniq<SummarizePropertyGraphBindData>(property_graph);
}

unique_ptr<GlobalTableFunctionState>
SummarizePropertyGraphFunction::SummarizePropertyGraphInit(
    ClientContext &context, TableFunctionInitInput &input) {
  return make_uniq<SummarizePropertyGraphGlobalData>();
}

void SummarizePropertyGraphFunction::SummarizePropertyGraphFunc(
    ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bind_data = data_p.bind_data->Cast<SummarizePropertyGraphBindData>();
  auto &data = data_p.global_state->Cast<SummarizePropertyGraphGlobalData>();
  if (data.done) {
    return;
  }
  auto pg_info = bind_data.summarize_pg_info;
  data.done = true;
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterSummarizePropertyGraphTableFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(db, SummarizePropertyGraphFunction());
}

} // namespace core

} // namespace duckpgq
