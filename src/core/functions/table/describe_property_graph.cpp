#include "duckpgq/core/functions/table/describe_property_graph.hpp"
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
DescribePropertyGraphFunction::DescribePropertyGraphBind(
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
  names.emplace_back("property_graph");
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("table_name");
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("label");
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("is_vertex_table");
  return_types.emplace_back(LogicalType::BOOLEAN);
  names.emplace_back("source_table");
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("source_pk");
  return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
  names.emplace_back("source_fk");
  return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
  names.emplace_back("destination_table");
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("destination_pk");
  return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
  names.emplace_back("destination_fk");
  return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
  names.emplace_back("discriminator");
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("sub_labels");
  return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
  names.emplace_back("catalog");
  return_types.emplace_back(LogicalType::VARCHAR);
  names.emplace_back("schema");
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
  auto pg_info = bind_data.describe_pg_info;
  idx_t vector_idx = 0;
  for (const auto &vertex_table : pg_info->vertex_tables) {
    output.SetValue(0, vector_idx, Value(pg_info->property_graph_name));
    output.SetValue(1, vector_idx, Value(vertex_table->table_name));
    output.SetValue(2, vector_idx, Value(vertex_table->main_label));
    output.SetValue(3, vector_idx, Value(vertex_table->is_vertex_table));
    output.SetValue(4, vector_idx, Value());
    output.SetValue(5, vector_idx, Value());
    output.SetValue(6, vector_idx, Value());
    output.SetValue(7, vector_idx, Value());
    output.SetValue(8, vector_idx, Value());
    output.SetValue(9, vector_idx, Value());
    if (!vertex_table->discriminator.empty()) {
      output.SetValue(10, vector_idx, Value(vertex_table->discriminator));
      vector<Value> sub_labels;
      for (const auto &label : vertex_table->sub_labels) {
        sub_labels.push_back(Value(label));
      }
      output.SetValue(11, vector_idx,
                      Value::LIST(LogicalType::VARCHAR, sub_labels));
    } else {
      output.SetValue(10, vector_idx, Value());
      output.SetValue(11, vector_idx, Value());
    }
    if (vertex_table->catalog_name.empty()) {
      output.SetValue(12, vector_idx, Value());
    } else {
      output.SetValue(12, vector_idx, Value(vertex_table->catalog_name));
    }
    output.SetValue(13, vector_idx, Value(vertex_table->schema_name));
    vector_idx++;
  }
  for (const auto &edge_table : pg_info->edge_tables) {
    output.SetValue(0, vector_idx, Value(pg_info->property_graph_name));
    output.SetValue(1, vector_idx, Value(edge_table->table_name));
    output.SetValue(2, vector_idx, Value(edge_table->main_label));
    output.SetValue(3, vector_idx, Value(edge_table->is_vertex_table));
    output.SetValue(4, vector_idx, Value(edge_table->source_reference));
    vector<Value> source_pk_list;
    for (const auto &col : edge_table->source_pk) {
      source_pk_list.push_back(Value(col));
    }
    output.SetValue(5, vector_idx,
                    Value::LIST(LogicalType::VARCHAR, source_pk_list));
    vector<Value> source_fk_list;
    for (const auto &col : edge_table->source_fk) {
      source_fk_list.push_back(Value(col));
    }
    output.SetValue(6, vector_idx,
                    Value::LIST(LogicalType::VARCHAR, source_fk_list));
    output.SetValue(7, vector_idx, Value(edge_table->destination_reference));
    vector<Value> destination_pk_list;
    for (const auto &col : edge_table->destination_pk) {
      destination_pk_list.push_back(Value(col));
    }
    output.SetValue(8, vector_idx,
                    Value::LIST(LogicalType::VARCHAR, destination_pk_list));
    vector<Value> destination_fk_list;
    for (const auto &col : edge_table->destination_fk) {
      destination_fk_list.push_back(Value(col));
    }
    output.SetValue(9, vector_idx,
                    Value::LIST(LogicalType::VARCHAR, destination_fk_list));
    if (!edge_table->discriminator.empty()) {
      output.SetValue(10, vector_idx, Value(edge_table->discriminator));
      vector<Value> sub_labels;
      for (const auto &label : edge_table->sub_labels) {
        sub_labels.push_back(Value(label));
      }
      output.SetValue(11, vector_idx,
                      Value::LIST(LogicalType::VARCHAR, sub_labels));
    } else {
      output.SetValue(10, vector_idx, Value());
      output.SetValue(11, vector_idx, Value());
    }
    if (edge_table->catalog_name.empty()) {
      output.SetValue(12, vector_idx, Value());
    } else {
      output.SetValue(12, vector_idx, Value(edge_table->catalog_name));
    }
    output.SetValue(13, vector_idx, Value(edge_table->schema_name));
    vector_idx++;
  }
  output.SetCardinality(vector_idx);
  data.done = true;
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterDescribePropertyGraphTableFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(db, DescribePropertyGraphFunction());
}

} // namespace core

} // namespace duckpgq
