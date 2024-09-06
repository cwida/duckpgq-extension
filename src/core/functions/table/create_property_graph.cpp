#include "duckpgq/core/functions/table/create_property_graph.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckpgq/common.hpp"
#include <duckpgq/core/functions/table.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <duckpgq_extension.hpp>

#include <duckpgq/core/parser/duckpgq_parser.hpp>

namespace duckpgq {
namespace core {
void CreatePropertyGraphFunction::CheckPropertyGraphTableLabels(
    const shared_ptr<PropertyGraphTable> &pg_table, TableCatalogEntry &table) {
  if (!pg_table->discriminator.empty()) {
    if (!table.ColumnExists(pg_table->discriminator)) {
      throw Exception(ExceptionType::INVALID,
                      "Column " + pg_table->discriminator +
                          " not found in table " + pg_table->table_name);
    }
    auto &column = table.GetColumn(pg_table->discriminator);
    if (!(column.GetType() == LogicalType::BIGINT ||
          column.GetType() == LogicalType::INTEGER)) {
      throw Exception(ExceptionType::INVALID,
                      "The discriminator column " + pg_table->discriminator +
                          " of table " + pg_table->table_name +
                          " should be of type BIGINT or INTEGER");
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
        throw Exception(ExceptionType::INVALID,
                        "EXCEPT column " + except_column +
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
                                                  " not found in table " +
                                                  pg_table->table_name);
    }
  }
}

// Helper function to validate source/destination keys
void CreatePropertyGraphFunction::ValidateKeys(shared_ptr<PropertyGraphTable> &edge_table, const string &reference, const string &key_type,
                  vector<string> &pk_columns, vector<string> &fk_columns,
                  const vector<unique_ptr<Constraint>> &table_constraints) {
  if (fk_columns.empty() && pk_columns.empty()) {
    if (table_constraints.empty()) {
      throw Exception(ExceptionType::INVALID,
                      "No primary key - foreign key relationship found in " +
                          edge_table->table_name + " with " + StringUtil::Upper(key_type) +
                          " table " + reference);
    }

    for (const auto &constraint : table_constraints) {
      if (constraint->type == ConstraintType::FOREIGN_KEY) {
        auto fk_constraint = constraint->Cast<ForeignKeyConstraint>();
        if (fk_constraint.info.table != reference) {
          continue;
        }
        // If a PK-FK relationship was found earlier, throw an ambiguity exception
        if (!pk_columns.empty() && !fk_columns.empty()) {
          throw Exception(
              ExceptionType::INVALID,
              "Multiple primary key - foreign key relationships detected between " + edge_table->table_name + " and " + reference + ". "
              "Please explicitly define the primary key and foreign key columns using `" +
                  StringUtil::Upper(key_type) + " KEY <primary key> REFERENCES " + reference + " <foreign key>`");
        }
        pk_columns = fk_constraint.pk_columns;
        fk_columns = fk_constraint.fk_columns;
      }
    }

    if (pk_columns.empty()) {
      throw Exception(ExceptionType::INVALID,
                      "The primary key for the " + StringUtil::Upper(key_type) + " table " + reference +
                          " is not defined in the edge table " + edge_table->table_name);
    }

    if (fk_columns.empty()) {
      throw Exception(ExceptionType::INVALID,
                      "The foreign key for the " + StringUtil::Upper(key_type) + " table " + reference +
                          " is not defined in the edge table " + edge_table->table_name);
    }
  }
}

void CreatePropertyGraphFunction::ValidateForeignKeyColumns(shared_ptr<PropertyGraphTable> &edge_table, const vector<string> &fk_columns, optional_ptr<TableCatalogEntry> &table) {
  for (const auto &fk : fk_columns) {
    if (!table->ColumnExists(fk)) {
      throw Exception(ExceptionType::INVALID,
                      "Foreign key " + fk + " does not exist in table " + edge_table->table_name);
    }
  }
}

// Helper function to check if the vertex table is registered
void CreatePropertyGraphFunction::ValidateVertexTableRegistration(const string &reference, const case_insensitive_set_t &v_table_names) {
  if (v_table_names.find(reference) == v_table_names.end()) {
    throw Exception(ExceptionType::INVALID,
                    "Referenced vertex table " + reference +
                    " is not registered in the vertex tables.");
  }
}

// Helper function to validate primary keys in the source or destination tables
void CreatePropertyGraphFunction::ValidatePrimaryKeyInTable(Catalog &catalog, ClientContext &context, const string &schema,
                               const string &reference, const vector<string> &pk_columns) {
  auto &table_entry = catalog.GetEntry<TableCatalogEntry>(context, schema, reference);

  for (const auto &pk : pk_columns) {
    if (!table_entry.ColumnExists(pk)) {
      throw Exception(ExceptionType::INVALID,
                      "Primary key " + pk + " does not exist in table " + reference);
    }
  }
}

unique_ptr<FunctionData> CreatePropertyGraphFunction::CreatePropertyGraphBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) {
  names.emplace_back("Success");
  return_types.emplace_back(LogicalType::BOOLEAN);
  auto duckpgq_state = GetDuckPGQState(context);

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
    throw Exception(ExceptionType::INVALID, "Property graph table with name " +
                                                info->property_graph_name +
                                                " already exists");
  }

  auto &catalog = Catalog::GetCatalog(context, info->catalog);
  case_insensitive_set_t v_table_names;
  for (auto &vertex_table : info->vertex_tables) {
      auto table = catalog.GetEntry<TableCatalogEntry>(
          context, info->schema, vertex_table->table_name, OnEntryNotFound::RETURN_NULL);
      if (!table) {
        throw Exception(ExceptionType::INVALID,
                        "Table " + vertex_table->table_name + " not found");
      }

      CheckPropertyGraphTableColumns(vertex_table, *table);
      CheckPropertyGraphTableLabels(vertex_table, *table);

    v_table_names.insert(vertex_table->table_name);
    if (vertex_table->hasTableNameAlias()) {
      v_table_names.insert(vertex_table->table_name_alias);
    }
  }

  for (auto &edge_table : info->edge_tables) {
    auto table = catalog.GetEntry<TableCatalogEntry>(context, info->schema,
                                                      edge_table->table_name, OnEntryNotFound::RETURN_NULL);
    if (!table) {
      throw Exception(ExceptionType::INVALID,
                      "Table " + edge_table->table_name + " not found");
    }

    CheckPropertyGraphTableColumns(edge_table, *table);
    CheckPropertyGraphTableLabels(edge_table, *table);
    auto &table_constraints = table->GetConstraints();

    ValidateKeys(edge_table, edge_table->source_reference, "source",
                   edge_table->source_pk, edge_table->source_fk, table_constraints);

    // Check source foreign key columns exist in the table
    ValidateForeignKeyColumns(edge_table, edge_table->source_fk, table);

    // Validate destination keys
    ValidateKeys(edge_table, edge_table->destination_reference, "destination",
                 edge_table->destination_pk, edge_table->destination_fk, table_constraints);

    // Check destination foreign key columns exist in the table
    ValidateForeignKeyColumns(edge_table, edge_table->destination_fk, table);

    // Validate source table registration
    ValidateVertexTableRegistration(edge_table->source_reference, v_table_names);

    // Validate primary keys in the source table
    ValidatePrimaryKeyInTable(catalog, context, info->schema, edge_table->source_reference, edge_table->source_pk);

    // Validate destination table registration
    ValidateVertexTableRegistration(edge_table->destination_reference, v_table_names);

    // Validate primary keys in the destination table
    ValidatePrimaryKeyInTable(catalog, context, info->schema, edge_table->destination_reference, edge_table->destination_pk);
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
  auto duckpgq_state = GetDuckPGQState(context);

  duckpgq_state->registered_property_graphs[pg_info->property_graph_name] =
      pg_info->Copy();
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterCreatePropertyGraphTableFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(db, CreatePropertyGraphFunction());
}
} // namespace core

} // namespace duckpgq