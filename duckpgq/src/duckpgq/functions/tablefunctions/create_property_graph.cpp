#include "duckpgq/functions/tablefunctions/create_property_graph.hpp"
#include <duckpgq_extension.hpp>

namespace duckdb {

    void CreatePropertyGraphFunction::CheckPropertyGraphTableLabels(shared_ptr<PropertyGraphTable> &pg_table,
                                                                    TableCatalogEntry &table) {
        if (!pg_table->discriminator.empty()) {
            if (!table.ColumnExists(pg_table->discriminator)) {
                throw BinderException("Column %s not found in table %s", pg_table->discriminator, pg_table->table_name);
            }
            auto &column = table.GetColumn(pg_table->discriminator);
            if (!(column.GetType() == LogicalType::BIGINT || column.GetType() == LogicalType::INTEGER)) {
                throw BinderException("The discriminator column %s for table %s should be of type BIGINT or INTEGER",
                                      pg_table->discriminator, pg_table->table_name);
            }
        }
    }

    void CreatePropertyGraphFunction::CheckPropertyGraphTableColumns(shared_ptr<PropertyGraphTable> &pg_table,
                                                                     TableCatalogEntry &table) {
        if (pg_table->no_columns) {
            return;
        }

        if (pg_table->all_columns) {
            for (auto &except_column: pg_table->except_columns) {
                if (!table.ColumnExists(except_column)) {
                    throw BinderException("Except column %s not found in table %s", except_column,
                                          pg_table->table_name);
                }
            }

            auto columns_of_table = table.GetColumns().GetColumnNames();

            std::sort(std::begin(columns_of_table), std::end(columns_of_table));
            std::sort(std::begin(pg_table->except_columns), std::end(pg_table->except_columns));
            std::set_difference(columns_of_table.begin(), columns_of_table.end(), pg_table->except_columns.begin(),
                                pg_table->except_columns.end(),
                                std::inserter(pg_table->column_names, pg_table->column_names.begin()));
            pg_table->column_aliases = pg_table->column_names;
            return;
        }

        for (auto &column: pg_table->column_names) {
            if (!table.ColumnExists(column)) {
                throw BinderException("Column %s not found in table %s", column, pg_table->table_name);
            }
        }
    }


    duckdb::unique_ptr<FunctionData>
    CreatePropertyGraphFunction::CreatePropertyGraphBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
        names.emplace_back("success");
        return_types.emplace_back(LogicalType::VARCHAR);
        auto lookup = context.registered_state.find("duckpgq");
        if (lookup == context.registered_state.end()) {
            throw BinderException("Registered DuckPGQ state not found");
        }
        auto duckpgq_state = (DuckPGQState *) lookup->second.get();
        auto duckpgq_parse_data = dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());

        if (!duckpgq_parse_data) {
            return {};
        }
        auto statement = dynamic_cast<CreateStatement *>(duckpgq_parse_data->statement.get());
        auto info = dynamic_cast<CreatePropertyGraphInfo *>(statement->info.get());
        auto pg_table = duckpgq_state->registered_property_graphs.find(info->property_graph_name);

        if (pg_table != duckpgq_state->registered_property_graphs.end()) {
            throw BinderException("Property graph table with name %s already exists", info->property_graph_name);
        }

        auto &catalog = Catalog::GetCatalog(context, info->catalog);

        case_insensitive_set_t v_table_names;
        for (auto &vertex_table: info->vertex_tables) {
            auto &table = catalog.GetEntry<TableCatalogEntry>(context, info->schema, vertex_table->table_name);

            CheckPropertyGraphTableColumns(vertex_table, table);
            CheckPropertyGraphTableLabels(vertex_table, table);

            v_table_names.insert(vertex_table->table_name);
        }

        for (auto &edge_table: info->edge_tables) {
            auto &table = catalog.GetEntry<TableCatalogEntry>(context, info->schema, edge_table->table_name);

            CheckPropertyGraphTableColumns(edge_table, table);
            CheckPropertyGraphTableLabels(edge_table, table);

            if (v_table_names.find(edge_table->source_reference) == v_table_names.end()) {
                throw BinderException("Referenced vertex table %s does not exist.", edge_table->source_reference);
            }

            auto &pk_source_table = catalog.GetEntry<TableCatalogEntry>(context, info->schema,
                                                                        edge_table->source_reference);
            for (auto &pk: edge_table->source_pk) {
                if (!pk_source_table.ColumnExists(pk)) {
                    throw BinderException("Primary key %s does not exist in table %s", pk,
                                          edge_table->source_reference);
                }
            }

            if (v_table_names.find(edge_table->source_reference) == v_table_names.end()) {
                throw BinderException("Referenced vertex table %s does not exist.", edge_table->source_reference);
            }

            auto &pk_destination_table =
                    catalog.GetEntry<TableCatalogEntry>(context, info->schema, edge_table->destination_reference);

            for (auto &pk: edge_table->destination_pk) {
                if (!pk_destination_table.ColumnExists(pk)) {
                    throw BinderException("Primary key %s does not exist in table %s", pk,
                                          edge_table->destination_reference);
                }
            }

            for (auto &fk: edge_table->source_fk) {
                if (!table.ColumnExists(fk)) {
                    throw BinderException("Foreign key %s does not exist in table %s", fk, edge_table->table_name);
                }
            }

            for (auto &fk: edge_table->destination_fk) {
                if (!table.ColumnExists(fk)) {
                    throw BinderException("Foreign key %s does not exist in table %s", fk, edge_table->table_name);
                }
            }
        }
        return make_uniq<CreatePropertyGraphBindData>(info);
    }

    duckdb::unique_ptr<GlobalTableFunctionState>
    CreatePropertyGraphFunction::CreatePropertyGraphInit(ClientContext &context,
                                                         TableFunctionInitInput &input) {
        return make_uniq<CreatePropertyGraphGlobalData>();
    }

    void CreatePropertyGraphFunction::CreatePropertyGraphFunc(ClientContext &context, TableFunctionInput &data_p,
                                                              DataChunk &output) {
        auto &bind_data = data_p.bind_data->Cast<CreatePropertyGraphBindData>();

        auto pg_info = bind_data.create_pg_info;
        auto lookup = context.registered_state.find("duckpgq");
        if (lookup == context.registered_state.end()) {
            throw BinderException("Registered DuckPGQ state not found");
        }
        auto duckpgq_state = (DuckPGQState *) lookup->second.get();
        auto pg_lookup = duckpgq_state->registered_property_graphs.find(pg_info->property_graph_name);
        if (pg_lookup == duckpgq_state->registered_property_graphs.end()) {
            duckpgq_state->registered_property_graphs[pg_info->property_graph_name] = pg_info->Copy();
        } else {
            throw BinderException("A property graph with name %s already exists.", pg_info->property_graph_name);
        }
    }
};
