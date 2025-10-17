#include "duckpgq/core/functions/table/create_property_graph.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckpgq/common.hpp"
#include <duckpgq/core/functions/table.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include "duckdb/main/connection_manager.hpp"
#include <duckpgq/core/parser/duckpgq_parser.hpp>
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

void CreatePropertyGraphFunction::CheckPropertyGraphTableLabels(const shared_ptr<PropertyGraphTable> &pg_table,
                                                                optional_ptr<TableCatalogEntry> &table) {
	if (!pg_table->discriminator.empty()) {
		if (!table->ColumnExists(pg_table->discriminator)) {
			throw Exception(ExceptionType::INVALID,
			                "Column " + pg_table->discriminator + " not found in table " + pg_table->table_name);
		}
		auto &column = table->GetColumn(pg_table->discriminator);
		if (!(column.GetType() == LogicalType::BIGINT || column.GetType() == LogicalType::INTEGER)) {
			throw Exception(ExceptionType::INVALID, "The discriminator column " + pg_table->discriminator +
			                                            " of table " + pg_table->table_name +
			                                            " should be of type BIGINT or INTEGER");
		}
	}
}

void CreatePropertyGraphFunction::CheckPropertyGraphTableColumns(const shared_ptr<PropertyGraphTable> &pg_table,
                                                                 optional_ptr<TableCatalogEntry> &table) {
	if (pg_table->no_columns) {
		return;
	}

	if (pg_table->all_columns) {
		for (auto &except_column : pg_table->except_columns) {
			if (!table->ColumnExists(except_column)) {
				throw Exception(ExceptionType::INVALID,
				                "EXCEPT column " + except_column + " not found in table " + pg_table->table_name);
			}
		}

		auto columns_of_table = table->GetColumns().GetColumnNames();

		std::sort(std::begin(columns_of_table), std::end(columns_of_table));
		std::sort(std::begin(pg_table->except_columns), std::end(pg_table->except_columns));
		std::set_difference(columns_of_table.begin(), columns_of_table.end(), pg_table->except_columns.begin(),
		                    pg_table->except_columns.end(),
		                    std::inserter(pg_table->column_names, pg_table->column_names.begin()));
		pg_table->column_aliases = pg_table->column_names;
		return;
	}

	for (auto &column : pg_table->column_names) {
		if (!table->ColumnExists(column)) {
			throw Exception(ExceptionType::INVALID, "Column " + column + " not found in table " + pg_table->table_name);
		}
	}
}

// Helper function to validate source/destination keys
void CreatePropertyGraphFunction::ValidateKeys(shared_ptr<PropertyGraphTable> &edge_table, const string &reference,
                                               const string &key_type, vector<string> &pk_columns,
                                               vector<string> &fk_columns,
                                               const vector<unique_ptr<Constraint>> &table_constraints) {
	// todo(dtenwolde) add test case for attached databases or different schema that has pk-fk relationships
	if (fk_columns.empty() && pk_columns.empty()) {
		if (table_constraints.empty()) {
			throw Exception(ExceptionType::INVALID, "No primary key - foreign key relationship found in " +
			                                            edge_table->table_name + " with " +
			                                            StringUtil::Upper(key_type) + " table " + reference);
		}

		for (const auto &constraint : table_constraints) {
			if (constraint->type == ConstraintType::FOREIGN_KEY) {
				auto fk_constraint = constraint->Cast<ForeignKeyConstraint>();
				if (fk_constraint.info.table != reference) {
					continue;
				}
				// If a PK-FK relationship was found earlier, throw an ambiguity
				// exception
				if (!pk_columns.empty() && !fk_columns.empty()) {
					throw Exception(ExceptionType::INVALID, "Multiple primary key - foreign key relationships "
					                                        "detected between " +
					                                            edge_table->table_name + " and " + reference +
					                                            ". "
					                                            "Please explicitly define the primary key and "
					                                            "foreign key columns using `" +
					                                            StringUtil::Upper(key_type) +
					                                            " KEY <primary key> REFERENCES " + reference +
					                                            " <foreign key>`");
				}
				pk_columns = fk_constraint.pk_columns;
				fk_columns = fk_constraint.fk_columns;
			}
		}

		if (pk_columns.empty()) {
			throw Exception(ExceptionType::INVALID, "The primary key for the " + StringUtil::Upper(key_type) +
			                                            " table " + reference + " is not defined in the edge table " +
			                                            edge_table->table_name);
		}

		if (fk_columns.empty()) {
			throw Exception(ExceptionType::INVALID, "The foreign key for the " + StringUtil::Upper(key_type) +
			                                            " table " + reference + " is not defined in the edge table " +
			                                            edge_table->table_name);
		}
	}
}

void CreatePropertyGraphFunction::ValidateForeignKeyColumns(shared_ptr<PropertyGraphTable> &edge_table,
                                                            const vector<string> &fk_columns,
                                                            optional_ptr<TableCatalogEntry> &table) {
	for (const auto &fk : fk_columns) {
		if (!table->ColumnExists(fk)) {
			throw Exception(ExceptionType::INVALID,
			                "Foreign key " + fk + " does not exist in table " + edge_table->table_name);
		}
	}
}

// Helper function to check if the vertex table is registered
void CreatePropertyGraphFunction::ValidateVertexTableRegistration(shared_ptr<PropertyGraphTable> &pg_table,
                                                                  const case_insensitive_set_t &v_table_names) {
	if (v_table_names.find(pg_table->FullTableName()) == v_table_names.end()) {
		throw Exception(ExceptionType::INVALID, "Referenced vertex table " + pg_table->FullTableName() +
		                                            " is not registered in the vertex tables.");
	}
}

// Helper function to validate primary keys in the source or destination tables
void CreatePropertyGraphFunction::ValidatePrimaryKeyInTable(ClientContext &context,
                                                            shared_ptr<PropertyGraphTable> &pg_table,
                                                            const vector<string> &pk_columns) {
	auto table = Catalog::GetEntry<TableCatalogEntry>(context, pg_table->catalog_name, pg_table->schema_name,
	                                                  pg_table->table_name, OnEntryNotFound::RETURN_NULL);
	if (!table) {
		throw Exception(ExceptionType::INVALID, "Table with name " + pg_table->table_name + " does not exist");
	}

	for (const auto &pk : pk_columns) {
		if (!table->ColumnExists(pk)) {
			throw Exception(ExceptionType::INVALID,
			                "Primary key " + pk + " does not exist in table " + pg_table->table_name);
		}
	}
}

unique_ptr<FunctionData> CreatePropertyGraphFunction::CreatePropertyGraphBind(ClientContext &context,
                                                                              TableFunctionBindInput &input,
                                                                              vector<LogicalType> &return_types,
                                                                              vector<string> &names) {
	names.emplace_back("Success");
	return_types.emplace_back(LogicalType::BOOLEAN);
	auto duckpgq_state = GetDuckPGQState(context);

	const auto duckpgq_parse_data = dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());

	if (!duckpgq_parse_data) {
		return {};
	}
	auto statement = dynamic_cast<CreateStatement *>(duckpgq_parse_data->statement.get());
	auto info = dynamic_cast<CreatePropertyGraphInfo *>(statement->info.get());
	auto pg_table = duckpgq_state->registered_property_graphs.find(info->property_graph_name);

	if (pg_table != duckpgq_state->registered_property_graphs.end() &&
	    info->on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
		throw Exception(ExceptionType::INVALID,
		                "Property graph table with name " + info->property_graph_name + " already exists");
	}

	case_insensitive_set_t v_table_names;
	for (auto &vertex_table : info->vertex_tables) {
		try {
			Binder::BindSchemaOrCatalog(context, vertex_table->catalog_name, vertex_table->schema_name);

			auto table =
			    Catalog::GetEntry<TableCatalogEntry>(context, vertex_table->catalog_name, vertex_table->schema_name,
			                                         vertex_table->table_name, OnEntryNotFound::RETURN_NULL);
			if (!table) {
				throw Exception(ExceptionType::INVALID,
				                "Table with name " + vertex_table->table_name + " does not exist");
			}
			CheckPropertyGraphTableColumns(vertex_table, table);
			CheckPropertyGraphTableLabels(vertex_table, table);
		} catch (CatalogException &e) {
			auto table =
			    Catalog::GetEntry<ViewCatalogEntry>(context, vertex_table->catalog_name, vertex_table->schema_name,
			                                        vertex_table->table_name, OnEntryNotFound::RETURN_NULL);
			if (table) {
				throw Exception(ExceptionType::INVALID, "Found a view with name " + vertex_table->table_name +
				                                            ". Creating property graph tables over views is "
				                                            "currently not supported.");
			}
			throw Exception(ExceptionType::INVALID, e.what());
		} catch (BinderException &e) {
			throw Exception(ExceptionType::INVALID, "Catalog '" + vertex_table->catalog_name + "' does not exist!");
		}
		v_table_names.insert(vertex_table->FullTableName());
		if (vertex_table->hasTableNameAlias()) {
			v_table_names.insert(vertex_table->table_name_alias);
		}
	}

	for (auto &edge_table : info->edge_tables) {
		try {
			Binder::BindSchemaOrCatalog(context, edge_table->catalog_name, edge_table->schema_name);
			auto table =
			    Catalog::GetEntry<TableCatalogEntry>(context, edge_table->catalog_name, edge_table->schema_name,
			                                         edge_table->table_name, OnEntryNotFound::RETURN_NULL);
			if (!table) {
				throw Exception(ExceptionType::INVALID,
				                "Table with name " + edge_table->table_name + " does not exist");
			}
			CheckPropertyGraphTableColumns(edge_table, table);
			CheckPropertyGraphTableLabels(edge_table, table);
			Binder::BindSchemaOrCatalog(context, edge_table->source_catalog, edge_table->source_schema);
			Binder::BindSchemaOrCatalog(context, edge_table->destination_catalog, edge_table->destination_schema);

			auto &table_constraints = table->GetConstraints();

			ValidateKeys(edge_table, edge_table->source_reference, "source", edge_table->source_pk,
			             edge_table->source_fk, table_constraints);

			// Check source foreign key columns exist in the table
			ValidateForeignKeyColumns(edge_table, edge_table->source_fk, table);

			// Validate destination keys
			ValidateKeys(edge_table, edge_table->destination_reference, "destination", edge_table->destination_pk,
			             edge_table->destination_fk, table_constraints);

			// Check destination foreign key columns exist in the table
			ValidateForeignKeyColumns(edge_table, edge_table->destination_fk, table);

			// Validate source table registration
			ValidateVertexTableRegistration(edge_table->source_pg_table, v_table_names);

			// Validate primary keys in the source table
			ValidatePrimaryKeyInTable(context, edge_table->source_pg_table, edge_table->source_pk);

			// Validate destination table registration
			ValidateVertexTableRegistration(edge_table->destination_pg_table, v_table_names);

			// Validate primary keys in the destination table
			ValidatePrimaryKeyInTable(context, edge_table->destination_pg_table, edge_table->destination_pk);
		} catch (CatalogException &e) {
			auto table = Catalog::GetEntry<ViewCatalogEntry>(context, edge_table->catalog_name, edge_table->schema_name,
			                                                 edge_table->table_name, OnEntryNotFound::RETURN_NULL);
			if (table) {
				throw Exception(ExceptionType::INVALID, "Found a view with name " + edge_table->table_name +
				                                            ". Creating property graph tables over views is "
				                                            "currently not supported.");
			}
			throw Exception(ExceptionType::INVALID, e.what());
		} catch (BinderException &e) {
			throw Exception(ExceptionType::INVALID, "Catalog '" + edge_table->catalog_name + "' does not exist!");
		}
	}
	return make_uniq<CreatePropertyGraphBindData>(info);
}

unique_ptr<GlobalTableFunctionState>
CreatePropertyGraphFunction::CreatePropertyGraphInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<CreatePropertyGraphGlobalData>();
}

void CreatePropertyGraphFunction::CreatePropertyGraphFunc(ClientContext &context, TableFunctionInput &data_p,
                                                          DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CreatePropertyGraphBindData>();
	auto pg_info = bind_data.create_pg_info;
	auto duckpgq_state = GetDuckPGQState(context);

	for (auto &local_client_context : ConnectionManager::Get(*context.db).GetConnectionList()) {
		auto local_state = GetDuckPGQState(*local_client_context);
		local_state->registered_property_graphs[pg_info->property_graph_name] = pg_info->Copy();
	}

	duckpgq_state->InitializeInternalTable(context);
	auto new_conn = make_shared_ptr<Connection>(*context.db);
	auto retrieve_query = new_conn->Query("SELECT * FROM __duckpgq_internal where property_graph = '" +
	                                      pg_info->property_graph_name + "';");
	if (retrieve_query->HasError()) {
		throw TransactionException(retrieve_query->GetError());
	}
	auto &query_result = retrieve_query->Cast<MaterializedQueryResult>();
	if (query_result.RowCount() > 0) {
		if (pg_info->on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw Exception(ExceptionType::INVALID,
			                "Property graph " + pg_info->property_graph_name + " is already registered");
		}
		if (pg_info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return; // Do nothing and silently return
		}
		if (pg_info->on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			// DELETE the old property graph and insert new one.
			new_conn->Query("DELETE FROM __duckpgq_internal WHERE property_graph = '" + pg_info->property_graph_name +
			                "';");
		}
	}

	string insert_info = "INSERT INTO __duckpgq_internal VALUES ";
	for (const auto &v_table : pg_info->vertex_tables) {
		insert_info += "(";
		insert_info += "'" + pg_info->property_graph_name + "', ";
		insert_info += "'" + v_table->table_name + "', ";
		insert_info += "'" + v_table->main_label + "', ";
		insert_info += "true, "; // is_vertex_table
		insert_info += "NULL, "; // source_table
		insert_info += "NULL, "; // source_pk
		insert_info += "NULL, "; // source_fk
		insert_info += "NULL, "; // destination_table
		insert_info += "NULL, "; // destination_pk
		insert_info += "NULL, "; // destination_fk
		insert_info += v_table->discriminator.empty() ? "NULL, " : "'" + v_table->discriminator + "', ";
		if (!v_table->discriminator.empty()) {
			insert_info += "[";
			for (idx_t i = 0; i < v_table->sub_labels.size(); i++) {
				insert_info += "'" + v_table->sub_labels[i] + (i == v_table->sub_labels.size() - 1 ? "'" : "', ");
			}
			insert_info += "],";
		} else {
			insert_info += "NULL,";
		}
		insert_info += "'" + v_table->catalog_name + "', ";
		insert_info += "'" + v_table->schema_name + "', ";
		insert_info += "NULL,"; // source table catalog
		insert_info += "NULL,"; // source table schema
		insert_info += "NULL,"; // destination table catalog
		insert_info += "NULL,"; // destination table schema
		insert_info += "[";     // Start of column names
		for (idx_t i = 0; i < v_table->column_names.size(); i++) {
			insert_info += "'" + v_table->column_names[i] + (i == v_table->column_names.size() - 1 ? "'" : "', ");
		}
		insert_info += "],"; // End of column names
		insert_info += "[";  // Start of column aliases
		for (idx_t i = 0; i < v_table->column_aliases.size(); i++) {
			insert_info += "'" + v_table->column_aliases[i] + (i == v_table->column_aliases.size() - 1 ? "'" : "', ");
		}
		insert_info += "]"; // End of column aliases
		insert_info += "), ";
	}

	for (const auto &e_table : pg_info->edge_tables) {
		insert_info += "(";
		insert_info += "'" + pg_info->property_graph_name + "', ";
		insert_info += "'" + e_table->table_name + "', ";
		insert_info += "'" + e_table->main_label + "', ";
		insert_info += "false, "; // is_vertex_table
		insert_info += "'" + e_table->source_reference + "', ";
		insert_info += "[";
		for (const auto &source_pk : e_table->source_pk) {
			insert_info += "'" + source_pk + "', ";
		}
		insert_info += "], ";
		insert_info += "[";
		for (const auto &source_fk : e_table->source_fk) {
			insert_info += "'" + source_fk + "', ";
		}
		insert_info += "], ";

		insert_info += "'" + e_table->destination_reference + "', ";
		insert_info += "[";
		for (const auto &destination_pk : e_table->destination_pk) {
			insert_info += "'" + destination_pk + "', ";
		}
		insert_info += "], ";
		insert_info += "[";
		for (const auto &destination_fk : e_table->destination_fk) {
			insert_info += "'" + destination_fk + "', ";
		}
		insert_info += "], ";

		insert_info += e_table->discriminator.empty() ? "NULL, " : "'" + e_table->discriminator + "', ";
		if (!e_table->discriminator.empty()) {
			insert_info += "[";
			for (idx_t i = 0; i < e_table->sub_labels.size(); i++) {
				insert_info += "'" + e_table->sub_labels[i] + (i == e_table->sub_labels.size() - 1 ? "'" : "', ");
			}
			insert_info += "], ";
		} else {
			insert_info += "NULL, ";
		}
		insert_info += "'" + e_table->catalog_name + "', ";
		insert_info += "'" + e_table->schema_name + "', ";
		insert_info += "'" + e_table->source_catalog + "', ";
		insert_info += "'" + e_table->source_schema + "', ";
		insert_info += "'" + e_table->destination_catalog + "', ";
		insert_info += "'" + e_table->destination_schema + "', ";
		insert_info += "["; // Start of column names
		for (idx_t i = 0; i < e_table->column_names.size(); i++) {
			insert_info += "'" + e_table->column_names[i] + (i == e_table->column_names.size() - 1 ? "'" : "', ");
		}
		insert_info += "],"; // End of column names
		insert_info += "[";  // Start of column aliases
		for (idx_t i = 0; i < e_table->column_aliases.size(); i++) {
			insert_info += "'" + e_table->column_aliases[i] + (i == e_table->column_aliases.size() - 1 ? "'" : "', ");
		}
		insert_info += "]"; // End of column aliases
		insert_info += "), ";
	}
	auto insert_query = new_conn->Query(insert_info);
	if (insert_query->HasError()) {
		throw TransactionException(insert_query->GetError());
	}
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterCreatePropertyGraphTableFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(CreatePropertyGraphFunction());
}

} // namespace duckdb
