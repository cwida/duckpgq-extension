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
                  "sub_labels varchar[])", false);
  if (query->HasError()) {
    throw TransactionException(query->GetError());
  }


  RetrievePropertyGraphs(new_conn);
}

void DuckPGQState::RetrievePropertyGraphs(shared_ptr<ClientContext> context) {
  auto retrieve_pg = context->Query("SELECT * FROM __duckpgq_internal", false);

  auto &materialized_result = retrieve_pg->Cast<MaterializedQueryResult>();
  auto row_count = materialized_result.RowCount();
  if (row_count == 0) {
    return; // no results
  }
  auto chunk = materialized_result.Fetch();
  for (idx_t i = 0; i < row_count; i++) {
    auto table = make_shared_ptr<PropertyGraphTable>();
    string property_graph_name = chunk->GetValue(0, i).GetValue<string>();
    table->table_name = chunk->GetValue(1, i).GetValue<string>();
    table->main_label = chunk->GetValue(2, i).GetValue<string>();
    table->is_vertex_table = chunk->GetValue(3, i).GetValue<bool>();
    table->all_columns = true; // TODO Be stricter on properties
    if (!table->is_vertex_table) {
      // Handle edge table related things.
      table->source_reference = chunk->GetValue(4, i).GetValue<string>();
      auto source_pk_chunk = ListValue::GetChildren(chunk->GetValue(5, i));
      for (const auto& source_pk: source_pk_chunk) {
        table->source_pk.push_back(source_pk.GetValue<string>());
      }
      auto source_fk_chunk = ListValue::GetChildren(chunk->GetValue(6, i));
      for (const auto& source_fk: source_fk_chunk) {
        table->source_fk.push_back(source_fk.GetValue<string>());
      }
      table->destination_reference = chunk->GetValue(7, i).GetValue<string>();
      auto destination_pk_chunk = ListValue::GetChildren(chunk->GetValue(8, i));
      for (const auto& dest_pk: destination_pk_chunk) {
        table->destination_pk.push_back(dest_pk.GetValue<string>());
      }
      auto destination_fk_chunk = ListValue::GetChildren(chunk->GetValue(9, i));
      for (const auto& dest_fk: destination_fk_chunk) {
        table->destination_fk.push_back(dest_fk.GetValue<string>());
      }
    }
    string discriminator = chunk->GetValue(10, i).GetValue<string>();
    if (discriminator != "NULL") {
      table->discriminator = discriminator;
      auto sublabels = ListValue::GetChildren(chunk->GetValue(11, i));
      for (const auto& sublabel : sublabels) {
        table->sub_labels.push_back(sublabel.GetValue<string>());
      }
    }

    if (registered_property_graphs.find(property_graph_name) == registered_property_graphs.end()) {
      registered_property_graphs[property_graph_name] = make_uniq<CreatePropertyGraphInfo>(property_graph_name);
    }
    auto &pg_info = registered_property_graphs[property_graph_name]->Cast<CreatePropertyGraphInfo>();
    pg_info.label_map[table->main_label] = table;
    if (!table->discriminator.empty()) {
      for (const auto &label : table->sub_labels) {
        pg_info.label_map[label] = table;
      }
    }
    if (table->is_vertex_table) {
      pg_info.vertex_tables.push_back(std::move(table));
    } else {
      pg_info.edge_tables.push_back(std::move(table));
    }
  }
}

void DuckPGQState::QueryEnd() {
  parse_data.reset();
  transform_expression.clear();
  match_index = 0;              // Reset the index
  unnamed_graphtable_index = 1; // Reset the index
  for (const auto &csr_id : csr_to_delete) {
    csr_list.erase(csr_id);
  }
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