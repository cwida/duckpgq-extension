#include "duckpgq_state.hpp"

namespace duckdb {

DuckPGQState::DuckPGQState(shared_ptr<ClientContext> context) {
  auto new_conn = make_shared_ptr<ClientContext>(context->db);
  auto query = new_conn->Query("CREATE TABLE IF NOT EXISTS __duckpgq_internal ("
                               "property_graph varchar, "
                               "table_name varchar, "
                               "label varchar, "
                               "is_vertex_table boolean, "
                               "source_table varchar, "
                               "source_pk varchar[], "
                               "source_fk varchar[], "
                               "destination_table varchar, "
                               "destination_pk varchar[], "
                               "destination_fk varchar[], "
                               "discriminator varchar, "
                               "sub_labels varchar[], "
                               "catalog varchar, "
                               "schema varchar,"
                               "source_catalog varchar, "
                               "source_schema varchar, "
                               "destination_catalog varchar, "
                               "destination_schema varchar, "
                               "properties varchar[], "
                               "column_aliases varchar[]"
                               ")",
                               false);
  if (query->HasError()) {
    throw TransactionException(query->GetError());
  }

  RetrievePropertyGraphs(new_conn);
}

void DuckPGQState::RetrievePropertyGraphs(
    const shared_ptr<ClientContext> &context) {
  // Retrieve and process vertex property graphs
  auto vertex_property_graphs = context->Query(
      "SELECT * FROM __duckpgq_internal WHERE is_vertex_table", false);
  ProcessPropertyGraphs(vertex_property_graphs, true);

  // Retrieve and process edge property graphs
  auto edge_property_graphs = context->Query(
      "SELECT * FROM __duckpgq_internal WHERE NOT is_vertex_table", false);
  ProcessPropertyGraphs(edge_property_graphs, false);
}

void DuckPGQState::ProcessPropertyGraphs(
    unique_ptr<QueryResult> &property_graphs, bool is_vertex) {
  if (!property_graphs ||
      property_graphs->type != QueryResultType::MATERIALIZED_RESULT) {
    throw std::runtime_error(
        "Failed to fetch property graphs or invalid result type.");
  }

  auto &materialized_result = property_graphs->Cast<MaterializedQueryResult>();
  auto row_count = materialized_result.RowCount();
  if (row_count == 0) {
    return; // No results
  }

  auto chunk = materialized_result.Fetch();
  for (idx_t i = 0; i < row_count; i++) {
    auto table = make_shared_ptr<PropertyGraphTable>();

    // Extract and validate common properties
    table->table_name = chunk->GetValue(1, i).GetValue<string>();
    table->main_label = chunk->GetValue(2, i).GetValue<string>();
    table->is_vertex_table = chunk->GetValue(3, i).GetValue<bool>();

    // Handle discriminator and sub-labels
    const auto &discriminator = chunk->GetValue(10, i).GetValue<string>();
    if (discriminator != "NULL") {
      table->discriminator = discriminator;
      auto sublabels = ListValue::GetChildren(chunk->GetValue(11, i));
      for (const auto &sublabel : sublabels) {
        table->sub_labels.push_back(sublabel.GetValue<string>());
      }
    }

    // Extract catalog and schema names
    if (chunk->ColumnCount() > 12) {
      table->catalog_name = chunk->GetValue(12, i).GetValue<string>();
      table->schema_name = chunk->GetValue(13, i).GetValue<string>();
    } else {
      table->catalog_name = "";
      table->schema_name = DEFAULT_SCHEMA;
    }
    if (chunk->ColumnCount() > 14) {
      table->source_catalog = chunk->GetValue(14, i).GetValue<string>();
      table->source_schema = chunk->GetValue(15, i).GetValue<string>();
      table->destination_catalog = chunk->GetValue(16, i).GetValue<string>();
      table->destination_schema = chunk->GetValue(17, i).GetValue<string>();
    } else {
      table->source_catalog = "";
      table->schema_name = DEFAULT_SCHEMA;
      table->destination_catalog = "";
      table->destination_schema = DEFAULT_SCHEMA;
    }
    if (chunk->ColumnCount() > 18) {
      // read properties
      auto properties = ListValue::GetChildren(chunk->GetValue(18, i));
      for (const auto &property: properties) {
        table->column_names.push_back(property.GetValue<string>());
      }
      auto column_aliases = ListValue::GetChildren(chunk->GetValue(19, i));
      for (const auto &alias: column_aliases) {
        table->column_aliases.push_back(alias.GetValue<string>());
      }
    } else {
      table->all_columns = true;
    }

    // Additional edge-specific handling
    if (!is_vertex) {
      PopulateEdgeSpecificFields(chunk, i, *table);
    }

    RegisterPropertyGraph(table, chunk->GetValue(0, i).GetValue<string>(),
                          is_vertex);
  }
}

void DuckPGQState::PopulateEdgeSpecificFields(unique_ptr<DataChunk> &chunk,
                                              idx_t row_idx,
                                              PropertyGraphTable &table) {
  table.source_reference = chunk->GetValue(4, row_idx).GetValue<string>();
  ExtractListValues(chunk->GetValue(5, row_idx), table.source_pk);
  ExtractListValues(chunk->GetValue(6, row_idx), table.source_fk);
  table.destination_reference = chunk->GetValue(7, row_idx).GetValue<string>();
  ExtractListValues(chunk->GetValue(8, row_idx), table.destination_pk);
  ExtractListValues(chunk->GetValue(9, row_idx), table.destination_fk);
}

void DuckPGQState::ExtractListValues(const Value &list_value,
                                     vector<string> &output) {
  auto children = ListValue::GetChildren(list_value);
  output.reserve(output.size() + children.size());
  for (const auto &child : children) {
    output.push_back(child.GetValue<string>());
  }
}

void DuckPGQState::RegisterPropertyGraph(
    const shared_ptr<PropertyGraphTable> &table, const string &graph_name,
    bool is_vertex) {
  // Ensure the property graph exists in the registry
  if (registered_property_graphs.find(graph_name) ==
      registered_property_graphs.end()) {
    registered_property_graphs[graph_name] =
        make_uniq<CreatePropertyGraphInfo>(graph_name);
  }

  auto &pg_info =
      registered_property_graphs[graph_name]->Cast<CreatePropertyGraphInfo>();
  pg_info.label_map[table->main_label] = table;

  if (!table->discriminator.empty()) {
    for (const auto &label : table->sub_labels) {
      pg_info.label_map[label] = table;
    }
  }

  if (is_vertex) {
    pg_info.vertex_tables.push_back(table);
  } else {
    table->source_pg_table = pg_info.GetTableByName(table->source_catalog, table->source_schema, table->source_reference);
    D_ASSERT(table->source_pg_table);
    table->destination_pg_table =
        pg_info.GetTableByName(table->destination_catalog, table->destination_schema, table->destination_reference);
    D_ASSERT(table->destination_pg_table);
    pg_info.edge_tables.push_back(table);
  }
}

void DuckPGQState::QueryEnd() {
  parse_data.reset();
  transform_expression.clear();
  match_index = 0;              // Reset the index
  for (const auto &csr_id : csr_to_delete) {
    csr_list.erase(csr_id);
  }
  csr_to_delete.clear();
}

CreatePropertyGraphInfo *DuckPGQState::GetPropertyGraph(const string &pg_name) {
  auto pg_table_entry = registered_property_graphs.find(pg_name);
  if (pg_table_entry == registered_property_graphs.end()) {
    throw BinderException("Property graph %s does not exist", pg_name);
  }
  return reinterpret_cast<CreatePropertyGraphInfo *>(
      pg_table_entry->second.get());
}

duckpgq::core::CSR *DuckPGQState::GetCSR(int32_t id) {
  auto csr_entry = csr_list.find(id);
  if (csr_entry == csr_list.end()) {
    throw ConstraintException("CSR not found with ID %d", id);
  }
  return csr_entry->second.get();
}

} // namespace duckdb