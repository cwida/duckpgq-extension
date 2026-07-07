#include "duckpgq/third_party/duckdb_peg_parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/path_element.hpp"
#include "duckdb/parser/path_pattern.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/parser/parsed_data/drop_property_graph_info.hpp"
#include "duckdb/parser/tableref/matchref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {
namespace duckpgq_peg {

static string PGQIdentifierName(const Identifier &identifier) {
	return identifier.GetIdentifierName();
}

static vector<string> PGQIdentifierNames(const vector<Identifier> &identifiers) {
	vector<string> result;
	result.reserve(identifiers.size());
	for (auto &identifier : identifiers) {
		result.push_back(PGQIdentifierName(identifier));
	}
	return result;
}

static vector<Identifier> PGQIdentifiers(const vector<Identifier> &identifiers) {
	vector<Identifier> result;
	result.reserve(identifiers.size());
	for (auto &identifier : identifiers) {
		result.push_back(identifier);
	}
	return result;
}

static void PGQApplyBaseTableName(PropertyGraphTable &table, const BaseTableRef &base_table_name) {
	auto &qualified_name = base_table_name.GetQualifiedName();
	table.catalog_name = PGQIdentifierName(qualified_name.Catalog());
	table.schema_name = PGQIdentifierName(qualified_name.Schema());
	table.table_name = PGQIdentifierName(qualified_name.Name());
}

static void PGQApplyTableAlias(PropertyGraphTable &table, const optional<TableAlias> &table_alias) {
	if (table_alias) {
		table.table_name_alias = PGQIdentifierName(table_alias->name);
	}
}

static void PGQApplyProperties(PropertyGraphTable &table, const optional<PropertyGraphProperties> &properties) {
	if (!properties) {
		table.all_columns = true;
		return;
	}
	table.column_names = PGQIdentifierNames(properties->columns);
	table.all_columns = properties->all_columns;
	table.no_columns = properties->no_columns;
}

static void PGQApplyLabel(PropertyGraphTable &table, const optional<PropertyGraphLabel> &label) {
	if (label) {
		table.main_label = PGQIdentifierName(label->main_label);
		if (label->sub_labels) {
			table.discriminator = PGQIdentifierName(label->sub_labels->discriminator);
			table.sub_labels = PGQIdentifiers(label->sub_labels->labels);
		}
	} else {
		table.main_label = table.table_name_alias.empty() ? table.table_name : table.table_name_alias;
	}
}

static void PGQApplyReference(PropertyGraphTable &edge_table, PropertyGraphTableReference &reference, bool source) {
	if (!reference.table) {
		return;
	}
	auto &qualified_name = reference.table->GetQualifiedName();
	auto &catalog = source ? edge_table.source_catalog : edge_table.destination_catalog;
	auto &schema = source ? edge_table.source_schema : edge_table.destination_schema;
	auto &table = source ? edge_table.source_reference : edge_table.destination_reference;
	auto &foreign_keys = source ? edge_table.source_fk : edge_table.destination_fk;
	auto &primary_keys = source ? edge_table.source_pk : edge_table.destination_pk;

	catalog = PGQIdentifierName(qualified_name.Catalog());
	schema = PGQIdentifierName(qualified_name.Schema());
	table = PGQIdentifierName(qualified_name.Name());
	foreign_keys = PGQIdentifierNames(reference.foreign_keys);
	primary_keys = PGQIdentifierNames(reference.primary_keys);
}

static bool PGQMatchesVertexReference(const shared_ptr<PropertyGraphTable> &vertex_table, const string &catalog_name,
                                      const string &schema_name, const string &table_name) {
	if (!catalog_name.empty() && !StringUtil::CIEquals(vertex_table->catalog_name, catalog_name)) {
		return false;
	}
	if (!schema_name.empty() && !StringUtil::CIEquals(vertex_table->schema_name, schema_name)) {
		return false;
	}
	return StringUtil::CIEquals(vertex_table->table_name, table_name) ||
	       (!vertex_table->table_name_alias.empty() && StringUtil::CIEquals(vertex_table->table_name_alias, table_name));
}

static shared_ptr<PropertyGraphTable> PGQFindVertexTable(const vector<shared_ptr<PropertyGraphTable>> &vertex_tables,
                                                         const string &catalog_name, const string &schema_name,
                                                         const string &table_name) {
	for (auto &vertex_table : vertex_tables) {
		if (PGQMatchesVertexReference(vertex_table, catalog_name, schema_name, table_name)) {
			return vertex_table;
		}
	}
	return nullptr;
}

static void PGQLinkEdgeReferences(CreatePropertyGraphInfo &info) {
	for (auto &edge_table : info.edge_tables) {
		edge_table->source_pg_table = PGQFindVertexTable(info.vertex_tables, edge_table->source_catalog,
		                                                 edge_table->source_schema, edge_table->source_reference);
		edge_table->destination_pg_table =
		    PGQFindVertexTable(info.vertex_tables, edge_table->destination_catalog, edge_table->destination_schema,
		                       edge_table->destination_reference);
	}
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreatePropertyGraphStmt(
    PEGTransformer &transformer, const optional<bool> &if_not_exists, const QualifiedName &qualified_name,
    vector<shared_ptr<PropertyGraphTable>> vertex_tables_clause,
    optional<vector<shared_ptr<PropertyGraphTable>>> edge_tables_clause) {
	auto result = make_uniq<CreateStatement>();
	if (qualified_name.Name().empty()) {
		throw ParserException("Empty property graph name not supported");
	}
	auto info = make_uniq<CreatePropertyGraphInfo>(PGQIdentifierName(qualified_name.Name()));
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->vertex_tables = std::move(vertex_tables_clause);
	if (edge_tables_clause) {
		info->edge_tables = std::move(*edge_tables_clause);
	}
	PGQLinkEdgeReferences(*info);
	for (auto &vertex_table : info->vertex_tables) {
		info->label_map[vertex_table->main_label] = vertex_table;
		for (auto &label : vertex_table->sub_labels) {
			info->label_map[label.GetIdentifierName()] = vertex_table;
		}
	}
	for (auto &edge_table : info->edge_tables) {
		info->label_map[edge_table->main_label] = edge_table;
		for (auto &label : edge_table->sub_labels) {
			info->label_map[label.GetIdentifierName()] = edge_table;
		}
	}
	result->info = std::move(info);
	return result;
}

unique_ptr<DropStatement> PEGTransformerFactory::TransformDropPropertyGraph(PEGTransformer &transformer,
                                                                            const optional<bool> &if_exists,
                                                                            const QualifiedName &qualified_name) {
	auto result = make_uniq<DropStatement>();
	auto info = make_uniq<DropPropertyGraphInfo>(qualified_name.Name().GetIdentifierName(), if_exists.has_value());
	result->info = std::move(info);
	return result;
}

shared_ptr<PropertyGraphTable> PEGTransformerFactory::TransformPropertyGraphVertexTable(
    PEGTransformer &transformer, unique_ptr<BaseTableRef> base_table_name, const optional<TableAlias> &table_alias_as,
    optional<PropertyGraphProperties> property_graph_properties, optional<PropertyGraphLabel> property_graph_label) {
	auto result = make_shared_ptr<PropertyGraphTable>();
	result->is_vertex_table = true;
	PGQApplyBaseTableName(*result, *base_table_name);
	PGQApplyTableAlias(*result, table_alias_as);
	PGQApplyProperties(*result, property_graph_properties);
	PGQApplyLabel(*result, property_graph_label);
	return result;
}

shared_ptr<PropertyGraphTable> PEGTransformerFactory::TransformPropertyGraphEdgeTable(
    PEGTransformer &transformer, unique_ptr<BaseTableRef> base_table_name, const optional<TableAlias> &table_alias_as,
    PropertyGraphTableReference source_key_reference, PropertyGraphTableReference destination_key_reference,
    optional<PropertyGraphProperties> property_graph_properties, optional<PropertyGraphLabel> property_graph_label) {
	auto result = make_shared_ptr<PropertyGraphTable>();
	result->is_vertex_table = false;
	PGQApplyBaseTableName(*result, *base_table_name);
	PGQApplyTableAlias(*result, table_alias_as);
	PGQApplyReference(*result, source_key_reference, true);
	PGQApplyReference(*result, destination_key_reference, false);
	PGQApplyProperties(*result, property_graph_properties);
	PGQApplyLabel(*result, property_graph_label);
	return result;
}

PropertyGraphProperties PEGTransformerFactory::TransformPropertyGraphAllProperties(PEGTransformer &transformer) {
	PropertyGraphProperties result;
	result.all_columns = true;
	return result;
}

PropertyGraphProperties PEGTransformerFactory::TransformPropertyGraphNoProperties(PEGTransformer &transformer) {
	PropertyGraphProperties result;
	result.no_columns = true;
	return result;
}

PropertyGraphProperties PEGTransformerFactory::TransformPropertyGraphPropertyList(PEGTransformer &transformer,
                                                                                  const vector<Identifier> &col_id) {
	PropertyGraphProperties result;
	result.columns = col_id;
	return result;
}

PropertyGraphLabel PEGTransformerFactory::TransformPropertyGraphLabel(
    PEGTransformer &transformer, const Identifier &col_id, optional<PropertyGraphSubLabels> property_graph_sub_labels) {
	PropertyGraphLabel result;
	result.main_label = col_id;
	result.sub_labels = std::move(property_graph_sub_labels);
	return result;
}

PropertyGraphSubLabels PEGTransformerFactory::TransformPropertyGraphSubLabels(PEGTransformer &transformer,
                                                                              const Identifier &identifier,
                                                                              const vector<Identifier> &col_id) {
	PropertyGraphSubLabels result;
	result.discriminator = identifier;
	result.labels = col_id;
	return result;
}

PropertyGraphTableReference
PEGTransformerFactory::TransformSourceTableReference(PEGTransformer &transformer, unique_ptr<BaseTableRef> base_table_name) {
	PropertyGraphTableReference result;
	result.table = std::move(base_table_name);
	return result;
}

PropertyGraphTableReference PEGTransformerFactory::TransformDestinationTableReference(
    PEGTransformer &transformer, unique_ptr<BaseTableRef> base_table_name) {
	PropertyGraphTableReference result;
	result.table = std::move(base_table_name);
	return result;
}

PropertyGraphTableReference PEGTransformerFactory::TransformPropertyGraphKeyReference(
    PEGTransformer &transformer, const vector<Identifier> &col_id, unique_ptr<BaseTableRef> base_table_name,
    const vector<Identifier> &col_id_1) {
	PropertyGraphTableReference result;
	result.foreign_keys = col_id;
	result.table = std::move(base_table_name);
	result.primary_keys = col_id_1;
	return result;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformGraphTableRef(
    PEGTransformer &transformer, string graph_table_keyword, const QualifiedName &qualified_name,
    unique_ptr<PathPattern> graph_path_pattern, optional<unique_ptr<ParsedExpression>> where_clause,
    vector<unique_ptr<ParsedExpression>> target_list, const optional<TableAlias> &table_alias) {
	if (!StringUtil::CIEquals(graph_table_keyword, "graph_table") &&
	    !StringUtil::CIEquals(graph_table_keyword, "graph table")) {
		throw ParserException("Expected GRAPH_TABLE or GRAPH TABLE");
	}

	auto match_expression = make_uniq<MatchExpression>();
	match_expression->pg_name = PGQIdentifierName(qualified_name.Name());
	match_expression->alias = table_alias ? table_alias->name.GetIdentifierName() : "graph_table";
	match_expression->where_clause = std::move(where_clause).value_or(nullptr);
	match_expression->column_list = std::move(target_list);
	match_expression->path_patterns.push_back(std::move(graph_path_pattern));

	vector<FunctionArgument> arguments;
	arguments.emplace_back(std::move(match_expression));

	auto result = make_uniq<TableFunctionRef>();
	result->function = make_uniq<FunctionExpression>(Identifier("duckpgq_match"), std::move(arguments));
	if (table_alias) {
		result->alias = table_alias->name;
		result->column_name_alias = table_alias->column_name_alias;
	}
	return std::move(result);
}

string PEGTransformerFactory::TransformGraphTableUnderscoreKeyword(PEGTransformer &transformer,
                                                                   const Identifier &identifier) {
	auto result = PGQIdentifierName(identifier);
	if (!StringUtil::CIEquals(result, "graph_table")) {
		throw ParserException("Expected GRAPH_TABLE");
	}
	return result;
}

string PEGTransformerFactory::TransformGraphTableSpacedKeyword(PEGTransformer &transformer) {
	return "graph table";
}

unique_ptr<PathPattern> PEGTransformerFactory::TransformGraphPathPattern(
    PEGTransformer &transformer, unique_ptr<PathElement> graph_vertex_pattern,
    optional<vector<vector<unique_ptr<PathReference>>>> graph_edge_vertex_pattern) {
	auto result = make_uniq<PathPattern>();
	result->path_elements.push_back(std::move(graph_vertex_pattern));
	if (graph_edge_vertex_pattern) {
		for (auto &edge_vertex : *graph_edge_vertex_pattern) {
			for (auto &path_reference : edge_vertex) {
				result->path_elements.push_back(std::move(path_reference));
			}
		}
	}
	return result;
}

vector<unique_ptr<PathReference>> PEGTransformerFactory::TransformGraphEdgeVertexPattern(
    PEGTransformer &transformer, unique_ptr<PathElement> graph_edge_pattern, unique_ptr<PathElement> graph_vertex_pattern) {
	vector<unique_ptr<PathReference>> result;
	result.push_back(std::move(graph_edge_pattern));
	result.push_back(std::move(graph_vertex_pattern));
	return result;
}

unique_ptr<PathElement> PEGTransformerFactory::TransformGraphVertexPattern(PEGTransformer &transformer,
                                                                           const Identifier &identifier,
                                                                           optional<Identifier> graph_table_label) {
	auto result = make_uniq<PathElement>(PGQPathReferenceType::PATH_ELEMENT);
	result->match_type = PGQMatchType::MATCH_VERTEX;
	result->variable_binding = PGQIdentifierName(identifier);
	result->label = graph_table_label ? PGQIdentifierName(*graph_table_label) : result->variable_binding;
	return result;
}

unique_ptr<PathElement> PEGTransformerFactory::TransformGraphEdgePattern(PEGTransformer &transformer,
                                                                         string graph_edge_left_endpoint,
                                                                         unique_ptr<PathElement> graph_edge_body,
                                                                         string graph_edge_right_endpoint) {
	if (graph_edge_left_endpoint == "-" && graph_edge_right_endpoint == "->") {
		graph_edge_body->match_type = PGQMatchType::MATCH_EDGE_RIGHT;
	} else if (graph_edge_left_endpoint == "<-" && graph_edge_right_endpoint == "-") {
		graph_edge_body->match_type = PGQMatchType::MATCH_EDGE_LEFT;
	} else if (graph_edge_left_endpoint == "-" && graph_edge_right_endpoint == "-") {
		graph_edge_body->match_type = PGQMatchType::MATCH_EDGE_ANY;
	} else if (graph_edge_left_endpoint == "<-" && graph_edge_right_endpoint == "->") {
		graph_edge_body->match_type = PGQMatchType::MATCH_EDGE_LEFT_RIGHT;
	} else {
		throw ParserException("Unsupported graph edge direction");
	}
	return graph_edge_body;
}

unique_ptr<PathElement> PEGTransformerFactory::TransformGraphEdgeBody(PEGTransformer &transformer,
                                                                      const Identifier &identifier,
                                                                      optional<Identifier> graph_table_label) {
	auto result = make_uniq<PathElement>(PGQPathReferenceType::PATH_ELEMENT);
	result->variable_binding = PGQIdentifierName(identifier);
	result->label = graph_table_label ? PGQIdentifierName(*graph_table_label) : result->variable_binding;
	return result;
}

string PEGTransformerFactory::TransformGraphEdgeLeftArrow(PEGTransformer &transformer) {
	return "<-";
}

string PEGTransformerFactory::TransformGraphEdgeRightArrow(PEGTransformer &transformer) {
	return "->";
}

string PEGTransformerFactory::TransformGraphEdgeDash(PEGTransformer &transformer) {
	return "-";
}

Identifier PEGTransformerFactory::TransformGraphTableLabel(PEGTransformer &transformer, const Identifier &col_id) {
	return col_id;
}

} // namespace duckpgq_peg
} // namespace duckdb
