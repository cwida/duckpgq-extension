#include "duckpgq_state.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

void DuckPGQState::InitializeInternalTable(ClientContext &context) {
	auto connection = make_shared_ptr<Connection>(*context.db);
	auto query = connection->Query("CREATE TABLE IF NOT EXISTS __duckpgq_internal ("
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
	                               "column_aliases varchar[], "
	                               "graph_catalog varchar, "
	                               "graph_schema varchar, "
	                               "verified boolean"
	                               ")");
	if (query->HasError()) {
		throw TransactionException(query->GetError());
	}
	// Databases written before graphs recorded their qualification only have the first 20 columns, and databases
	// written before the incomplete-edge check have no verified column (NULL: not verified).
	for (auto column : {"graph_catalog VARCHAR", "graph_schema VARCHAR", "verified BOOLEAN"}) {
		auto alter =
		    connection->Query(StringUtil::Format("ALTER TABLE __duckpgq_internal ADD COLUMN IF NOT EXISTS %s", column));
		if (alter->HasError()) {
			throw TransactionException(alter->GetError());
		}
	}
}

void DuckPGQState::RetrievePropertyGraphs(const shared_ptr<Connection> &connection) {
	// Retrieve and process vertex property graphs
	auto vertex_property_graphs = connection->Query("SELECT * FROM __duckpgq_internal WHERE is_vertex_table");
	ProcessPropertyGraphs(*connection->context, vertex_property_graphs, true);

	// Retrieve and process edge property graphs
	auto edge_property_graphs = connection->Query("SELECT * FROM __duckpgq_internal WHERE NOT is_vertex_table");
	ProcessPropertyGraphs(*connection->context, edge_property_graphs, false);
}

void DuckPGQState::ProcessPropertyGraphs(ClientContext &context, unique_ptr<QueryResult> &property_graphs,
                                         bool is_vertex) {
	if (!property_graphs || property_graphs->HasError()) {
		throw std::runtime_error("Failed to fetch property graphs or invalid result type.");
	}

	while (auto chunk = property_graphs->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto table = make_shared_ptr<PropertyGraphTable>();

			// Extract and validate common properties
			table->table_name = Identifier(chunk->GetValue(1, i).GetValue<string>());
			table->main_label = Identifier(chunk->GetValue(2, i).GetValue<string>());
			table->is_vertex_table = chunk->GetValue(3, i).GetValue<bool>();

			// Handle discriminator and sub-labels
			const auto &discriminator = chunk->GetValue(10, i).GetValue<string>();
			if (discriminator != "NULL") {
				table->discriminator = Identifier(discriminator);
				auto sublabels = ListValue::GetChildren(chunk->GetValue(11, i));
				for (const auto &sublabel : sublabels) {
					table->sub_labels.emplace_back(sublabel.GetValue<string>());
				}
			}

			// Extract catalog and schema names
			if (chunk->ColumnCount() > 12) {
				table->catalog_name = Identifier(chunk->GetValue(12, i).GetValue<string>());
				table->schema_name = Identifier(chunk->GetValue(13, i).GetValue<string>());
			} else {
				table->catalog_name = Identifier();
				table->schema_name = Identifier::DefaultSchema();
			}
			if (chunk->ColumnCount() > 14) {
				table->source_catalog = Identifier(chunk->GetValue(14, i).GetValue<string>());
				table->source_schema = Identifier(chunk->GetValue(15, i).GetValue<string>());
				table->destination_catalog = Identifier(chunk->GetValue(16, i).GetValue<string>());
				table->destination_schema = Identifier(chunk->GetValue(17, i).GetValue<string>());
			} else {
				table->source_catalog = Identifier();
				table->source_schema = Identifier::DefaultSchema();
				table->destination_catalog = Identifier();
				table->destination_schema = Identifier::DefaultSchema();
			}
			if (chunk->ColumnCount() > 18) {
				// read properties
				auto properties = ListValue::GetChildren(chunk->GetValue(18, i));
				for (const auto &property : properties) {
					table->column_names.emplace_back(property.GetValue<string>());
				}
				auto column_aliases = ListValue::GetChildren(chunk->GetValue(19, i));
				for (const auto &alias : column_aliases) {
					table->column_aliases.emplace_back(alias.GetValue<string>());
				}
			} else {
				table->all_columns = true;
			}

			// Additional edge-specific handling
			if (!is_vertex) {
				PopulateEdgeSpecificFields(chunk, i, *table);
			}

			// Rows written before graphs recorded their qualification resolve to the defaults.
			Identifier graph_catalog;
			Identifier graph_schema;
			if (chunk->ColumnCount() > 21) {
				auto catalog_value = chunk->GetValue(20, i);
				auto schema_value = chunk->GetValue(21, i);
				if (!catalog_value.IsNull()) {
					graph_catalog = Identifier(catalog_value.GetValue<string>());
				}
				if (!schema_value.IsNull()) {
					graph_schema = Identifier(schema_value.GetValue<string>());
				}
			}
			bool verified = false;
			if (chunk->ColumnCount() > 22) {
				auto verified_value = chunk->GetValue(22, i);
				verified = !verified_value.IsNull() && verified_value.GetValue<bool>();
			}
			QualifiedName graph_name(graph_catalog, graph_schema, Identifier(chunk->GetValue(0, i).GetValue<string>()));
			RegisterPropertyGraph(table, ResolvePropertyGraphName(context, graph_name), is_vertex, verified);
		}
	}
}

void DuckPGQState::PopulateEdgeSpecificFields(unique_ptr<DataChunk> &chunk, idx_t row_idx, PropertyGraphTable &table) {
	table.source_reference = Identifier(chunk->GetValue(4, row_idx).GetValue<string>());
	ExtractListValues(chunk->GetValue(5, row_idx), table.source_pk);
	ExtractListValues(chunk->GetValue(6, row_idx), table.source_fk);
	table.destination_reference = Identifier(chunk->GetValue(7, row_idx).GetValue<string>());
	ExtractListValues(chunk->GetValue(8, row_idx), table.destination_pk);
	ExtractListValues(chunk->GetValue(9, row_idx), table.destination_fk);
}

void DuckPGQState::ExtractListValues(const Value &list_value, vector<Identifier> &output) {
	auto children = ListValue::GetChildren(list_value);
	output.reserve(output.size() + children.size());
	for (const auto &child : children) {
		output.emplace_back(child.GetValue<string>());
	}
}

void DuckPGQState::RegisterPropertyGraph(const shared_ptr<PropertyGraphTable> &table, const QualifiedName &graph_name,
                                         bool is_vertex, bool verified) {
	auto name = graph_name.Name().GetIdentifierName();
	// Ensure the property graph exists in the registry
	if (registered_property_graphs.find(name) == registered_property_graphs.end()) {
		auto info = make_uniq<CreatePropertyGraphInfo>(name);
		info->SetQualifiedName(graph_name);
		info->verified = true;
		registered_property_graphs[name] = std::move(info);
	}

	auto &pg_info = registered_property_graphs[name]->Cast<CreatePropertyGraphInfo>();
	// A graph is verified only if every persisted row says so.
	pg_info.verified = pg_info.verified && verified;
	pg_info.label_map[table->main_label.GetIdentifierName()] = table;

	if (!table->discriminator.empty()) {
		for (const auto &label : table->sub_labels) {
			pg_info.label_map[label.GetIdentifierName()] = table;
		}
	}

	if (is_vertex) {
		pg_info.vertex_tables.push_back(table);
	} else {
		table->source_pg_table =
		    pg_info.GetTableByName(table->source_catalog.GetIdentifierName(), table->source_schema.GetIdentifierName(),
		                           table->source_reference.GetIdentifierName());
		D_ASSERT(table->source_pg_table);
		table->destination_pg_table = pg_info.GetTableByName(table->destination_catalog.GetIdentifierName(),
		                                                     table->destination_schema.GetIdentifierName(),
		                                                     table->destination_reference.GetIdentifierName());
		D_ASSERT(table->destination_pg_table);
		pg_info.edge_tables.push_back(table);
	}
}

void DuckPGQState::QueryEnd() {
	parse_data.reset();
	for (const auto &csr_id : csr_to_delete) {
		csr_list.erase(csr_id);
	}
	csr_to_delete.clear();
}

QualifiedName DuckPGQState::ResolvePropertyGraphName(ClientContext &context, const QualifiedName &pg_name) {
	auto resolved = pg_name;
	// A lone qualifier ("x.g") is a schema unless x names a catalog, like DuckDB does for tables.
	Binder::BindSchemaOrCatalog(context, resolved);
	auto catalog = resolved.Catalog();
	auto schema = resolved.Schema();
	if (catalog.empty()) {
		catalog = DatabaseManager::GetDefaultDatabase(context);
	}
	if (schema.empty()) {
		schema = Identifier::DefaultSchema();
	}
	return QualifiedName(std::move(catalog), std::move(schema), resolved.Name());
}

optional_ptr<CreatePropertyGraphInfo> DuckPGQState::TryGetPropertyGraph(ClientContext &context,
                                                                        const QualifiedName &pg_name) {
	auto entry = registered_property_graphs.find(pg_name.Name().GetIdentifierName());
	if (entry == registered_property_graphs.end()) {
		return nullptr;
	}
	auto &info = entry->second->Cast<CreatePropertyGraphInfo>();
	if (pg_name.Path().size() > 1) {
		// Graph names are unique per database, but a qualifier must match where the graph was created.
		auto requested = ResolvePropertyGraphName(context, pg_name);
		auto &stored = info.GetQualifiedName();
		if (requested.Catalog() != stored.Catalog() || requested.Schema() != stored.Schema()) {
			return nullptr;
		}
	}
	return &info;
}

CreatePropertyGraphInfo *DuckPGQState::GetPropertyGraph(ClientContext &context, const QualifiedName &pg_name) {
	auto info = TryGetPropertyGraph(context, pg_name);
	if (!info) {
		throw BinderException("Property graph %s does not exist", pg_name.ToString());
	}
	return info.get();
}

CreatePropertyGraphInfo *DuckPGQState::GetPropertyGraph(const string &pg_name) {
	auto pg_table_entry = registered_property_graphs.find(pg_name);
	if (pg_table_entry == registered_property_graphs.end()) {
		throw BinderException("Property graph %s does not exist", pg_name);
	}
	return reinterpret_cast<CreatePropertyGraphInfo *>(pg_table_entry->second.get());
}

CSR *DuckPGQState::GetCSR(int32_t id) {
	auto csr_entry = csr_list.find(id);
	if (csr_entry == csr_list.end()) {
		throw ConstraintException("CSR not found with ID %d", id);
	}
	return csr_entry->second.get();
}

} // namespace duckdb
