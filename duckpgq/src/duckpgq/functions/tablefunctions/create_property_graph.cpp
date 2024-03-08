#include "duckpgq/functions/tablefunctions/create_property_graph.hpp"
#include <duckpgq_extension.hpp>

namespace duckdb {

void CreatePropertyGraphFunction::CheckPropertyGraphTableLabels(
    const shared_ptr<PropertyGraphTable> &pg_table, TableCatalogEntry &table) {
  if (!pg_table->discriminator.empty()) {
    if (!table.ColumnExists(pg_table->discriminator)) {
      throw Exception(ExceptionType::INVALID, "Column " + pg_table->discriminator +
        " not found in table " + pg_table->table_name);
    }
    auto &column = table.GetColumn(pg_table->discriminator);
    if (!(column.GetType() == LogicalType::BIGINT ||
          column.GetType() == LogicalType::INTEGER)) {
      throw Exception(ExceptionType::INVALID, "The discriminator column " +
        pg_table->discriminator + " of table " +
        pg_table->table_name + " should be of type BIGINT or INTEGER");
    }
  }
}

void CreatePropertyGraphFunction::CheckPropertyGraphTableColumns(
    const shared_ptr<PropertyGraphTable> &pg_table, TableCatalogEntry &table) {
  if (pg_table->no_columns) {
    return;
  }

  if (pg_table->all_columns) {
    for (auto &except_column : pg_table->except_columns) {
      if (!table.ColumnExists(except_column)) {
        throw Exception(ExceptionType::INVALID, "Except column " + except_column +
          " not found in table " + pg_table->table_name);
      }
    }

    auto columns_of_table = table.GetColumns().GetColumnNames();

    std::sort(std::begin(columns_of_table), std::end(columns_of_table));
    std::sort(std::begin(pg_table->except_columns),
              std::end(pg_table->except_columns));
    std::set_difference(
        columns_of_table.begin(), columns_of_table.end(),
        pg_table->except_columns.begin(), pg_table->except_columns.end(),
        std::inserter(pg_table->column_names, pg_table->column_names.begin()));
    pg_table->column_aliases = pg_table->column_names;
    return;
  }

  for (auto &column : pg_table->column_names) {
    if (!table.ColumnExists(column)) {
      throw Exception(ExceptionType::INVALID, "Column " + column +
        " not found in table " + pg_table->table_name);
    }
  }
}

unique_ptr<FunctionData>
CreatePropertyGraphFunction::CreatePropertyGraphBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) {
  names.emplace_back("Success");
  return_types.emplace_back(LogicalType::BOOLEAN);
  auto lookup = context.registered_state.find("duckpgq");
  if (lookup == context.registered_state.end()) {
    throw Exception(ExceptionType::INVALID,
                    "Registered DuckPGQ state not found");
  }
  const auto duckpgq_state = (DuckPGQState *)lookup->second.get();
  const auto duckpgq_parse_data =
      dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());

  if (!duckpgq_parse_data) {
    return {};
  }
  auto statement =
      dynamic_cast<CreateStatement *>(duckpgq_parse_data->statement.get());
  auto info = dynamic_cast<CreatePropertyGraphInfo *>(statement->info.get());
  auto pg_table =
      duckpgq_state->registered_property_graphs.find(info->property_graph_name);

  if (pg_table != duckpgq_state->registered_property_graphs.end() &&
      info->on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
    throw Exception(ExceptionType::INVALID, "Property graph table with name " + info->property_graph_name + " already exists");
  }

  auto &catalog = Catalog::GetCatalog(context, info->catalog);
  case_insensitive_set_t v_table_names;
  for (auto &vertex_table : info->vertex_tables) {
    auto &table = catalog.GetEntry<TableCatalogEntry>(context, info->schema,
                                                      vertex_table->table_name);

    CheckPropertyGraphTableColumns(vertex_table, table);
    CheckPropertyGraphTableLabels(vertex_table, table);

    v_table_names.insert(vertex_table->table_name);
    if (vertex_table->hasTableNameAlias()) {
      v_table_names.insert(vertex_table->table_name_alias);
    }
  }

  for (auto &edge_table : info->edge_tables) {
    auto &table = catalog.GetEntry<TableCatalogEntry>(context, info->schema,
                                                      edge_table->table_name);

    CheckPropertyGraphTableColumns(edge_table, table);
    CheckPropertyGraphTableLabels(edge_table, table);

    if (v_table_names.find(edge_table->source_reference) ==
        v_table_names.end()) {
      throw Exception(ExceptionType::INVALID, "Referenced vertex table " + edge_table->source_reference + " does not exist.");
    }

    auto &pk_source_table = catalog.GetEntry<TableCatalogEntry>(
        context, info->schema, edge_table->source_reference);
    for (auto &pk : edge_table->source_pk) {
      if (!pk_source_table.ColumnExists(pk)) {
        throw Exception(ExceptionType::INVALID, "Primary key " + pk + " does not exist in table " + edge_table->source_reference);
      }
    }

    if (v_table_names.find(edge_table->source_reference) ==
        v_table_names.end()) {
      throw Exception(ExceptionType::INVALID, "Referenced vertex table " + edge_table->source_reference + " does not exist");
    }

    auto &pk_destination_table = catalog.GetEntry<TableCatalogEntry>(
        context, info->schema, edge_table->destination_reference);

    for (auto &pk : edge_table->destination_pk) {
      if (!pk_destination_table.ColumnExists(pk)) {
        throw Exception(ExceptionType::INVALID,"Primary key " + pk + " does not exist in table " + edge_table->destination_reference);
      }
    }

    for (auto &fk : edge_table->source_fk) {
      if (!table.ColumnExists(fk)) {
        throw Exception(ExceptionType::INVALID,"Foreign key " + fk + " does not exist in table " + edge_table->table_name);
      }
    }

    for (auto &fk : edge_table->destination_fk) {
      if (!table.ColumnExists(fk)) {
        throw Exception(ExceptionType::INVALID,"Foreign key " + fk + " does not exist in table " + edge_table->table_name);
      }
    }
  }
  return make_uniq<CreatePropertyGraphBindData>(info);
}

unique_ptr<GlobalTableFunctionState>
CreatePropertyGraphFunction::CreatePropertyGraphInit(
    ClientContext &context, TableFunctionInitInput &input) {
  return make_uniq<CreatePropertyGraphGlobalData>();
}

void CreatePropertyGraphFunction::CreatePropertyGraphFunc(
    ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bind_data = data_p.bind_data->Cast<CreatePropertyGraphBindData>();

  auto pg_info = bind_data.create_pg_info;
  auto lookup = context.registered_state.find("duckpgq");
  if (lookup == context.registered_state.end()) {
    throw Exception(ExceptionType::INVALID,"Registered DuckPGQ state not found");
  }
  auto duckpgq_state = (DuckPGQState *)lookup->second.get();
  duckpgq_state->registered_property_graphs[pg_info->property_graph_name] =
    pg_info->Copy();
}
}; // namespace duckdb
