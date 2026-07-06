#include "duckpgq/third_party/duckdb_peg_parser/peg/ast/column_constraint_entry.hpp"
#include "duckpgq/third_party/duckdb_peg_parser/peg/ast/column_constraints.hpp"
#include "duckpgq/third_party/duckdb_peg_parser/peg/ast/column_elements.hpp"
#include "duckpgq/third_party/duckdb_peg_parser/peg/ast/create_table_column_element.hpp"
#include "duckpgq/third_party/duckdb_peg_parser/peg/ast/create_table_definition.hpp"
#include "duckpgq/third_party/duckdb_peg_parser/peg/ast/generated_column_definition.hpp"
#include "duckpgq/third_party/duckdb_peg_parser/peg/ast/key_actions.hpp"
#include "duckpgq/third_party/duckdb_peg_parser/peg/ast/partition_sorted_options.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckpgq/third_party/duckdb_peg_parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/type_expression.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/common/string_util.hpp"

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
			table.sub_labels = PGQIdentifierNames(label->sub_labels->labels);
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

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformCreateStatement(PEGTransformer &transformer, const optional<bool> &or_replace,
                                                const optional<SecretPersistType> &temporary,
                                                unique_ptr<CreateStatement> create_statement_variation) {
	auto result = std::move(create_statement_variation);
	auto &conflict_policy = result->info->on_conflict;
	if (or_replace) {
		if (conflict_policy == OnCreateConflict::IGNORE_ON_CONFLICT) {
			throw ParserException("Cannot specify both OR REPLACE and IF NOT EXISTS within single create statement");
		}
		conflict_policy = OnCreateConflict::REPLACE_ON_CONFLICT;
	}
	if (result->info->type == CatalogType::SECRET_ENTRY) {
		auto &secret_info = result->info->Cast<CreateSecretInfo>();
		secret_info.persist_type = temporary ? *temporary : SecretPersistType::DEFAULT;
	}
	result->info->temporary = temporary && *temporary == SecretPersistType::TEMPORARY;
	return std::move(result);
}

SecretPersistType PEGTransformerFactory::TransformPersistent(PEGTransformer &transformer) {
	return SecretPersistType::PERSISTENT;
}

SecretPersistType PEGTransformerFactory::TransformTempPersistent(PEGTransformer &transformer) {
	return SecretPersistType::TEMPORARY;
}

SecretPersistType PEGTransformerFactory::TransformTemporaryPersistent(PEGTransformer &transformer) {
	return SecretPersistType::TEMPORARY;
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateTableStmt(
    PEGTransformer &transformer, const optional<bool> &if_not_exists, const QualifiedName &qualified_name,
    CreateTableDefinition create_table_definition, const optional<bool> &commit_action) {
	auto result = make_uniq<CreateStatement>();
	if (qualified_name.Name().empty()) {
		throw ParserException("Empty table name not supported");
	}
	auto info = make_uniq<CreateTableInfo>(qualified_name);

	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	info->query = std::move(create_table_definition.select_statement);
	info->columns = std::move(create_table_definition.columns);
	info->constraints = std::move(create_table_definition.constraints);
	info->partition_keys = std::move(create_table_definition.partition_keys);
	info->sort_keys = std::move(create_table_definition.sort_keys);
	info->options = std::move(create_table_definition.options);

	result->info = std::move(info);
	return result;
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
			info->label_map[label] = vertex_table;
		}
	}
	for (auto &edge_table : info->edge_tables) {
		info->label_map[edge_table->main_label] = edge_table;
		for (auto &label : edge_table->sub_labels) {
			info->label_map[label] = edge_table;
		}
	}
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

CreateTableDefinition
PEGTransformerFactory::TransformCreateTableAs(PEGTransformer &transformer, optional<ColumnList> identifier_list,
                                              optional<PartitionSortedOptions> partition_sorted_options,
                                              optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> with_list,
                                              unique_ptr<SQLStatement> statement, const optional<bool> &with_data) {
	CreateTableDefinition result;
	if (identifier_list) {
		result.columns = std::move(*identifier_list);
	}
	if (partition_sorted_options) {
		result.partition_keys = std::move(partition_sorted_options->partition_keys);
		result.sort_keys = std::move(partition_sorted_options->sort_keys);
	}
	if (with_list) {
		result.options = std::move(*with_list);
	}
	if (statement->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("CREATE TABLE AS requires a SELECT clause");
	}
	result.select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(statement));
	if (with_data && *with_data) {
		auto limit_modifier = make_uniq<LimitModifier>();
		limit_modifier->limit = make_uniq<ConstantExpression>(0);
		result.select_statement->node->modifiers.push_back(std::move(limit_modifier));
	}
	return result;
}

ColumnList PEGTransformerFactory::TransformIdentifierList(PEGTransformer &transformer,
                                                          const vector<Identifier> &identifier) {
	ColumnList result;
	for (auto &name : identifier) {
		result.AddColumn(ColumnDefinition(name, LogicalType::UNKNOWN));
	}
	return result;
}

CreateTableDefinition PEGTransformerFactory::TransformCreateColumnList(
    PEGTransformer &transformer, optional<ColumnElements> create_table_column_list,
    optional<PartitionSortedOptions> partition_sorted_options,
    optional<case_insensitive_map_t<unique_ptr<ParsedExpression>>> with_list) {
	if (!create_table_column_list || create_table_column_list->columns.empty()) {
		throw ParserException("Table must have at least one column!");
	}
	CreateTableDefinition result;
	result.columns = std::move(create_table_column_list->columns);
	result.constraints = std::move(create_table_column_list->constraints);
	if (partition_sorted_options) {
		result.partition_keys = std::move(partition_sorted_options->partition_keys);
		result.sort_keys = std::move(partition_sorted_options->sort_keys);
	}
	if (with_list) {
		result.options = std::move(*with_list);
	}
	return result;
}

bool PEGTransformerFactory::TransformOrReplace(PEGTransformer &transformer) {
	return true;
}

bool PEGTransformerFactory::TransformIfNotExists(PEGTransformer &transformer) {
	return true;
}

ColumnElements
PEGTransformerFactory::TransformCreateTableColumnList(PEGTransformer &transformer,
                                                      vector<CreateTableColumnElement> create_table_column_element) {
	ColumnElements result;
	for (idx_t col_idx = 0; col_idx < create_table_column_element.size(); ++col_idx) {
		auto &column_element = create_table_column_element[col_idx];
		if (column_element.column_definition) {
			auto &column_result = *column_element.column_definition;
			for (auto &constraint : column_result.constraints) {
				result.constraints.push_back(std::move(constraint));
			}
			for (auto constraint_type : column_result.constraint_types) {
				if (constraint_type.second == ConstraintType::NOT_NULL) {
					result.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(col_idx)));
				} else if (constraint_type.second == ConstraintType::UNIQUE) {
					result.constraints.push_back(make_uniq<UniqueConstraint>(
					    LogicalIndex(col_idx), column_result.column_definition.GetName(), constraint_type.first));
				}
			}
			result.columns.AddColumn(std::move(column_result.column_definition));
		} else {
			result.constraints.push_back(std::move(column_element.constraint));
		}
	}
	return result;
}

CreateTableColumnElement
PEGTransformerFactory::TransformCreateTableColumnDefinition(PEGTransformer &transformer,
                                                            ConstraintColumnDefinition column_definition) {
	CreateTableColumnElement result;
	result.column_definition = make_uniq<ConstraintColumnDefinition>(std::move(column_definition));
	return result;
}

CreateTableColumnElement
PEGTransformerFactory::TransformCreateTableConstraint(PEGTransformer &transformer,
                                                      unique_ptr<Constraint> top_level_constraint) {
	CreateTableColumnElement result;
	result.constraint = std::move(top_level_constraint);
	return result;
}

QualifiedName PEGTransformerFactory::TransformIdentifierOrStringLiteral(PEGTransformer &transformer,
                                                                        const string &child) {
	return QualifiedName(Identifier(child));
}

string PEGTransformerFactory::TransformColLabelOrString(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.GetResult().type == ParseResultType::STRING) {
		return choice_pr.GetResult().Cast<StringLiteralParseResult>().result;
	}
	return transformer.Transform<string>(choice_pr.GetResult());
}

Identifier PEGTransformerFactory::TransformColIdOrString(PEGTransformer &transformer, ParseResult &choice_result) {
	if (choice_result.type == ParseResultType::STRING) {
		return Identifier(choice_result.Cast<StringLiteralParseResult>().result);
	}
	return transformer.Transform<Identifier>(choice_result);
}

string PEGTransformerFactory::TransformIdentifier(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier.GetIdentifierName();
}

vector<string> PEGTransformerFactory::TransformDottedIdentifier(PEGTransformer &transformer,
                                                                const Identifier &identifier,
                                                                const optional<vector<string>> &dot_col_label) {
	vector<string> parts {identifier.GetIdentifierName()};
	if (dot_col_label) {
		parts.insert(parts.end(), dot_col_label->begin(), dot_col_label->end());
	}
	return parts;
}

ConstraintColumnDefinition PEGTransformerFactory::TransformColumnDefinition(
    PEGTransformer &transformer, const vector<string> &dotted_identifier, const optional<LogicalType> &type,
    optional<GeneratedColumnDefinition> generated_column, const bool &has_result,
    optional<vector<ColumnConstraintEntry>> column_constraint) {
	auto qualified_name = StringToQualifiedName(dotted_identifier);
	bool has_type = type.has_value();
	bool has_generated = generated_column && generated_column->expr != nullptr;
	if (!has_type && !has_generated) {
		throw ParserException("Column %s must have a type or be defined as a GENERATED column.",
		                      qualified_name.ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA));
	}
	auto column_type = has_type ? *type : LogicalType::ANY;
	CompressionType compression_type = CompressionType::COMPRESSION_AUTO;
	ColumnConstraint accumulated_constraints;
	if (column_constraint) {
		for (auto &cc_entry : *column_constraint) {
			if (cc_entry.constraint_name == "DefaultValue") {
				if (accumulated_constraints.default_value) {
					throw ParserException("Cannot define a default value twice");
				}
				accumulated_constraints.default_value = std::move(cc_entry.expression);
			} else if (cc_entry.constraint_name == "NotNullConstraint" ||
			           cc_entry.constraint_name == "UniqueConstraint" ||
			           cc_entry.constraint_name == "PrimaryKeyConstraint") {
				accumulated_constraints.constraint_types.push_back(cc_entry.constraint_type_info);
			} else if (cc_entry.constraint_name == "ColumnCompression") {
				compression_type = cc_entry.compression_type;
				if (compression_type == CompressionType::COMPRESSION_AUTO) {
					throw ParserException("Unrecognized option for column compression, expected none, uncompressed, "
					                      "rle, dictionary, pfor, bitpacking, fsst, chimp, patas, zstd, alp, alprd or "
					                      "roaring");
				}
			} else if (cc_entry.constraint_name == "ForeignKeyConstraint") {
				auto &fk_constraint = cc_entry.constraint->Cast<ForeignKeyConstraint>();
				fk_constraint.fk_columns.push_back(qualified_name.Name());
				accumulated_constraints.constraints.push_back(std::move(cc_entry.constraint));
			} else if (cc_entry.constraint_name == "ColumnCollation") {
				if (has_generated) {
					throw ParserException("Collations are not supported on generated columns");
				}
				if (column_type.id() == LogicalTypeId::ANY) {
					throw ParserException("Specify the VARCHAR type for column \"%s\" with collation.",
					                      qualified_name.ToString(QualifiedNameToStringMode::HIDE_DEFAULT_SCHEMA));
				} else if (column_type.IsUnbound()) {
					auto &expr = UnboundType::GetTypeExpression(column_type);
					if (expr->GetExpressionClass() != ExpressionClass::TYPE) {
						throw InternalException("Expected a type expression");
					}
					auto &type_expr = expr->Cast<TypeExpression>();
					if (DefaultTypeGenerator::GetDefaultType(type_expr.GetTypeName()) != LogicalTypeId::VARCHAR) {
						throw ParserException("Only VARCHAR columns can have collations!");
					}
				} else {
					throw InternalException("Expected only unbound types here");
				}
				vector<unique_ptr<ParsedExpression>> type_children;
				type_children.push_back(std::move(cc_entry.expression));
				column_type =
				    LogicalType::UNBOUND(make_uniq<TypeExpression>(Identifier("VARCHAR"), std::move(type_children)));
			} else {
				accumulated_constraints.constraints.push_back(std::move(cc_entry.constraint));
			}
		}
	}
	if (has_generated) {
		auto generated = std::move(*generated_column);
		if (generated.expr->HasSubquery()) {
			throw ParserException("Expression of generated column \"%s\" contains a subquery, which isn't allowed",
			                      qualified_name.Name());
		}
		if (column_type != LogicalType::ANY) {
			generated.expr = make_uniq<CastExpression>(column_type, std::move(generated.expr));
		}
		if (generated.expr->HasSubquery()) {
			throw ParserException("Expression of generated column \"%s\" contains a subquery, which isn't allowed",
			                      qualified_name.Name());
		}

		ColumnDefinition col(qualified_name.Name(), column_type, std::move(generated.expr), TableColumnType::GENERATED);
		col.SetCompressionType(compression_type);
		if (accumulated_constraints.default_value) {
			throw ParserException("Not allowed to set default on a generated column");
		}
		ConstraintColumnDefinition result = {std::move(col), accumulated_constraints.constraint_types,
		                                     std::move(accumulated_constraints.constraints)};
		return result;
	}

	ColumnDefinition col(qualified_name.Name(), column_type);

	if (accumulated_constraints.default_value) {
		col.SetDefaultValue(std::move(accumulated_constraints.default_value));
	}
	col.SetCompressionType(compression_type);
	ConstraintColumnDefinition result = {std::move(col), accumulated_constraints.constraint_types,
	                                     std::move(accumulated_constraints.constraints)};
	return result;
}

GeneratedColumnDefinition PEGTransformerFactory::TransformGeneratedColumn(PEGTransformer &transformer,
                                                                          const bool &has_result,
                                                                          unique_ptr<ParsedExpression> expression,
                                                                          const optional<bool> &generated_column_type) {
	GeneratedColumnDefinition generated;
	generated.expr = std::move(expression);
	VerifyColumnRefs(*generated.expr);
	return generated;
}

ColumnConstraintEntry PEGTransformerFactory::TransformDefaultValue(PEGTransformer &transformer,
                                                                   unique_ptr<ParsedExpression> column_default_expr) {
	ColumnConstraintEntry entry;
	entry.constraint_name = "DefaultValue";
	entry.expression = std::move(column_default_expr);
	return entry;
}

unique_ptr<Constraint>
PEGTransformerFactory::TransformTopLevelConstraint(PEGTransformer &transformer, const bool &has_result,
                                                   unique_ptr<Constraint> top_level_constraint_list) {
	return top_level_constraint_list;
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopLevelConstraintList(PEGTransformer &transformer,
                                                                              ParseResult &choice_result) {
	if (choice_result.name == "CheckConstraint") {
		auto cc_entry = transformer.Transform<ColumnConstraintEntry>(choice_result);
		return std::move(cc_entry.constraint);
	}
	return transformer.Transform<unique_ptr<Constraint>>(choice_result);
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopPrimaryKeyConstraint(PEGTransformer &transformer,
                                                                               const vector<string> &column_id_list) {
	auto result = make_uniq<UniqueConstraint>(StringsToIdentifiers(column_id_list), true);
	return std::move(result);
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopUniqueConstraint(PEGTransformer &transformer,
                                                                           const vector<string> &column_id_list) {
	return make_uniq<UniqueConstraint>(StringsToIdentifiers(column_id_list), false);
}

ColumnConstraintEntry PEGTransformerFactory::TransformCheckConstraint(PEGTransformer &transformer,
                                                                      unique_ptr<ParsedExpression> expression) {
	if (expression->HasSubquery()) {
		throw ParserException("subqueries prohibited in CHECK constraints");
	}
	ColumnConstraintEntry entry;
	entry.constraint_name = "CheckConstraint";
	entry.constraint = make_uniq<CheckConstraint>(std::move(expression));
	return entry;
}

unique_ptr<Constraint> PEGTransformerFactory::TransformTopForeignKeyConstraint(
    PEGTransformer &transformer, const vector<string> &column_id_list, ColumnConstraintEntry foreign_key_constraint) {
	auto &fk_constraint = foreign_key_constraint.constraint->Cast<ForeignKeyConstraint>();
	fk_constraint.fk_columns = StringsToIdentifiers(column_id_list);
	if (!fk_constraint.pk_columns.empty() && fk_constraint.fk_columns.size() != fk_constraint.pk_columns.size()) {
		throw ParserException("The number of referencing and referenced columns for foreign keys must be the same");
	}
	return std::move(foreign_key_constraint.constraint);
}

vector<string> PEGTransformerFactory::TransformColumnIdList(PEGTransformer &transformer,
                                                            const vector<Identifier> &col_id) {
	return IdentifiersToStrings(col_id);
}

ColumnConstraintEntry PEGTransformerFactory::TransformColumnCompression(PEGTransformer &transformer,
                                                                        const Identifier &col_id_or_string) {
	ColumnConstraintEntry entry;
	entry.constraint_name = "ColumnCompression";
	entry.compression_type =
	    EnumUtil::FromString<CompressionType>(StringUtil::Lower(col_id_or_string.GetIdentifierName()));
	return entry;
}

ColumnConstraintEntry PEGTransformerFactory::TransformForeignKeyConstraint(PEGTransformer &transformer,
                                                                           unique_ptr<BaseTableRef> base_table_name,
                                                                           const optional<vector<string>> &column_list,
                                                                           const KeyActions &key_actions) {
	ForeignKeyInfo fk_info;
	fk_info.schema = base_table_name->GetQualifiedName().Schema();
	fk_info.table = base_table_name->Table();
	fk_info.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;

	ColumnConstraintEntry entry;
	entry.constraint_name = "ForeignKeyConstraint";
	vector<Identifier> columns;
	if (column_list) {
		columns = StringsToIdentifiers(*column_list);
	}
	entry.constraint = make_uniq<ForeignKeyConstraint>(columns, vector<Identifier>(), fk_info);
	return entry;
}

KeyActions PEGTransformerFactory::TransformKeyActions(PEGTransformer &transformer,
                                                      const optional<string> &update_action,
                                                      const optional<string> &delete_action) {
	KeyActions results;
	if (update_action) {
		results.update_action = *update_action;
	}
	if (delete_action) {
		results.delete_action = *delete_action;
	}
	return results;
}

string PEGTransformerFactory::TransformUpdateAction(PEGTransformer &transformer, const string &key_action) {
	return key_action;
}

string PEGTransformerFactory::TransformDeleteAction(PEGTransformer &transformer, const string &key_action) {
	return key_action;
}

string PEGTransformerFactory::TransformNoKeyAction(PEGTransformer &transformer) {
	return "NoKeyAction";
}

string PEGTransformerFactory::TransformRestrictKeyAction(PEGTransformer &transformer) {
	return "Restrict";
}

string PEGTransformerFactory::TransformCascadeKeyAction(PEGTransformer &transformer) {
	throw ParserException("FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT");
}

string PEGTransformerFactory::TransformSetNullKeyAction(PEGTransformer &transformer) {
	throw ParserException("FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT");
}

string PEGTransformerFactory::TransformSetDefaultKeyAction(PEGTransformer &transformer) {
	throw ParserException("FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT");
}

ColumnConstraintEntry PEGTransformerFactory::TransformPrimaryKeyConstraint(PEGTransformer &transformer) {
	ColumnConstraintEntry entry;
	entry.constraint_name = "PrimaryKeyConstraint";
	entry.constraint_type_info = make_pair(true, ConstraintType::UNIQUE);
	return entry;
}

ColumnConstraintEntry PEGTransformerFactory::TransformUniqueConstraint(PEGTransformer &transformer) {
	ColumnConstraintEntry entry;
	entry.constraint_name = "UniqueConstraint";
	entry.constraint_type_info = make_pair(false, ConstraintType::UNIQUE);
	return entry;
}

bool PEGTransformerFactory::TransformNullConstraint(PEGTransformer &transformer) {
	return false;
}

bool PEGTransformerFactory::TransformNotNullColumnConstraint(PEGTransformer &transformer) {
	return true;
}

ColumnConstraintEntry PEGTransformerFactory::TransformNotNullConstraint(PEGTransformer &transformer,
                                                                        const bool &child) {
	ColumnConstraintEntry entry;
	entry.constraint_name = "NotNullConstraint";
	entry.constraint_type_info = make_pair(false, child ? ConstraintType::NOT_NULL : ConstraintType::INVALID);
	return entry;
}

ColumnConstraintEntry PEGTransformerFactory::TransformColumnCollation(PEGTransformer &transformer,
                                                                      const vector<string> &dotted_identifier) {
	string collation = StringUtil::Join(dotted_identifier, ".");
	auto expr = make_uniq<ConstantExpression>(Value(collation));
	expr->SetAlias("collation");
	ColumnConstraintEntry entry;
	entry.constraint_name = "ColumnCollation";
	entry.expression = std::move(expr);
	return entry;
}

bool PEGTransformerFactory::TransformWithDataOnly(PEGTransformer &transformer) {
	return false;
}

bool PEGTransformerFactory::TransformWithNoData(PEGTransformer &transformer) {
	return true;
}

bool PEGTransformerFactory::TransformCommitAction(PEGTransformer &transformer, const bool &preserve_or_delete) {
	return preserve_or_delete;
}

bool PEGTransformerFactory::TransformPreserveRows(PEGTransformer &transformer) {
	return true;
}

bool PEGTransformerFactory::TransformDeleteRows(PEGTransformer &transformer) {
	throw NotImplementedException("Only ON COMMIT PRESERVE ROWS is supported");
}

bool PEGTransformerFactory::TransformVirtualGeneratedColumn(PEGTransformer &transformer) {
	return true;
}

bool PEGTransformerFactory::TransformStoredGeneratedColumn(PEGTransformer &transformer) {
	throw InvalidInputException("Can not create a STORED generated column!");
}

void PEGTransformerFactory::VerifyColumnRefs(const ParsedExpression &expr) {
	ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(expr, [&](const ColumnRefExpression &column_ref) {
		if (column_ref.IsQualified()) {
			throw ParserException(
			    "Qualified (tbl.name) column references are not allowed inside of generated column expressions");
		}
	});
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformPartitionOptions(PEGTransformer &transformer,
                                                 vector<unique_ptr<ParsedExpression>> expression) {
	return expression;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSortedOptions(PEGTransformer &transformer,
                                              vector<unique_ptr<ParsedExpression>> expression) {
	return expression;
}

PartitionSortedOptions PEGTransformerFactory::TransformPartitionOptSortedOptions(
    PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> partition_options,
    optional<vector<unique_ptr<ParsedExpression>>> sorted_options) {
	PartitionSortedOptions result;
	result.partition_keys = std::move(partition_options);
	if (sorted_options) {
		result.sort_keys = std::move(*sorted_options);
	}
	return result;
}

PartitionSortedOptions PEGTransformerFactory::TransformSortedOptPartitionOptions(
    PEGTransformer &transformer, vector<unique_ptr<ParsedExpression>> sorted_options,
    optional<vector<unique_ptr<ParsedExpression>>> partition_options) {
	PartitionSortedOptions result;
	result.sort_keys = std::move(sorted_options);
	if (partition_options) {
		result.partition_keys = std::move(*partition_options);
	}
	return result;
}

} // namespace duckpgq_peg
} // namespace duckdb
