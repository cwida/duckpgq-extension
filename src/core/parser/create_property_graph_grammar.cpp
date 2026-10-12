#include "duckpgq/core/parser/duckpgq_parser.hpp"

#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckpgq/parser/parsed_data/create_property_graph_info.hpp"
#include "duckpgq_create_property_graph_grammar.hpp"

namespace duckdb {

DUCKDB_REGISTER_TRANSFORM_RESULT_TYPE("duckpgq.transform_result.PropertyGraphTable", shared_ptr<PropertyGraphTable>);

namespace {

static ParseResult &GetChoice(ParseResult &parse_result) {
	return parse_result.Cast<ListParseResult>().Child<ChoiceParseResult>(0).GetResult();
}

template <class T>
static vector<T> TransformParensList(PEGTransformer &transformer, ParseResult &parse_result) {
	vector<T> result;
	auto &list = PEGTransformerFactory::ExtractResultFromParens(parse_result);
	for (auto &item : PEGTransformerFactory::ExtractParseResultsFromList(list)) {
		result.push_back(transformer.Transform<T>(item.get()));
	}
	return result;
}

static void ApplyProperties(PEGTransformer &transformer, OptionalParseResult &properties, PropertyGraphTable &table) {
	if (!properties.HasResult()) {
		table.all_columns = true;
		return;
	}
	auto &choice = GetChoice(properties.GetResult());
	auto &list = choice.Cast<ListParseResult>();
	if (choice.Name() == "DuckPGQNoProperties") {
		table.no_columns = true;
	} else if (choice.Name() == "DuckPGQPropertyList") {
		auto &items = PEGTransformerFactory::ExtractResultFromParens(list.GetChild(1));
		for (auto &item : PEGTransformerFactory::ExtractParseResultsFromList(items)) {
			auto &property = item.get().Cast<ListParseResult>();
			auto column = transformer.Transform<Identifier>(property.GetChild(0));
			auto &alias = property.Child<OptionalParseResult>(1);
			table.column_names.push_back(column);
			table.column_aliases.push_back(
			    alias.HasResult()
			        ? transformer.Transform<Identifier>(alias.GetResult().Cast<ListParseResult>().GetChild(1))
			        : column);
		}
	} else {
		table.all_columns = true;
		if (choice.Name() == "DuckPGQAllColumnsExcept") {
			table.except_columns = TransformParensList<Identifier>(transformer, list.GetChild(5));
		}
	}
}

static void ApplySubLabels(PEGTransformer &transformer, ParseResult &parse_result, PropertyGraphTable &table) {
	auto &list = parse_result.Cast<ListParseResult>();
	table.discriminator = transformer.Transform<Identifier>(list.GetChild(1));
	table.sub_labels = TransformParensList<Identifier>(transformer, list.GetChild(2));
}

static void ApplyLabel(PEGTransformer &transformer, OptionalParseResult &label, PropertyGraphTable &table) {
	table.main_label = table.table_name_alias.empty() ? table.table_name : table.table_name_alias;
	if (!label.HasResult()) {
		return;
	}
	auto &choice = GetChoice(label.GetResult());
	auto &list = choice.Cast<ListParseResult>();
	if (choice.Name() == "DuckPGQExplicitLabel") {
		table.main_label = transformer.Transform<Identifier>(list.GetChild(1));
		auto &sub_labels = list.Child<OptionalParseResult>(2);
		if (sub_labels.HasResult()) {
			ApplySubLabels(transformer, sub_labels.GetResult(), table);
		}
	} else {
		ApplySubLabels(transformer, list.GetChild(0), table);
	}
}

static void ApplyReference(PEGTransformer &transformer, ParseResult &parse_result, PropertyGraphTable &edge,
                           bool source) {
	auto &reference = GetChoice(parse_result.Cast<ListParseResult>().GetChild(1));
	unique_ptr<BaseTableRef> base_table;
	auto &foreign_keys = source ? edge.source_fk : edge.destination_fk;
	auto &primary_keys = source ? edge.source_pk : edge.destination_pk;
	if (reference.Name() == "DuckPGQKeyReference") {
		auto &list = reference.Cast<ListParseResult>();
		foreign_keys = TransformParensList<Identifier>(transformer, list.GetChild(1));
		base_table = transformer.Transform<unique_ptr<BaseTableRef>>(list.GetChild(3));
		primary_keys = TransformParensList<Identifier>(transformer, list.GetChild(4));
	} else {
		base_table = transformer.Transform<unique_ptr<BaseTableRef>>(reference);
	}
	auto &name = base_table->GetQualifiedName();
	(source ? edge.source_catalog : edge.destination_catalog) = name.Catalog();
	(source ? edge.source_schema : edge.destination_schema) = name.Schema();
	(source ? edge.source_reference : edge.destination_reference) = name.Name();
}

static arena_ptr<TransformResultValue> TransformGraphTable(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto table = make_shared_ptr<PropertyGraphTable>();
	table->is_vertex_table = parse_result.Name() == "DuckPGQVertexTable";
	auto base_table = transformer.Transform<unique_ptr<BaseTableRef>>(list.GetChild(0));
	auto &name = base_table->GetQualifiedName();
	table->catalog_name = name.Catalog();
	table->schema_name = name.Schema();
	table->table_name = name.Name();
	auto &alias = list.Child<OptionalParseResult>(1);
	if (alias.HasResult()) {
		table->table_name_alias = transformer.Transform<TableAlias>(alias.GetResult()).name;
	}
	idx_t properties_index = 2;
	if (!table->is_vertex_table) {
		ApplyReference(transformer, list.GetChild(2), *table, true);
		ApplyReference(transformer, list.GetChild(3), *table, false);
		properties_index = 4;
	}
	ApplyProperties(transformer, list.Child<OptionalParseResult>(properties_index), *table);
	ApplyLabel(transformer, list.Child<OptionalParseResult>(properties_index + 1), *table);
	return transformer.MakeResult<shared_ptr<PropertyGraphTable>>(std::move(table));
}

static shared_ptr<PropertyGraphTable> FindVertexTable(const CreatePropertyGraphInfo &info, const Identifier &catalog,
                                                      const Identifier &schema, const Identifier &name) {
	for (auto &table : info.vertex_tables) {
		if ((!catalog.empty() && table->catalog_name != catalog) || (!schema.empty() && table->schema_name != schema)) {
			continue;
		}
		if (table->table_name == name || (!table->table_name_alias.empty() && table->table_name_alias == name)) {
			return table;
		}
	}
	return nullptr;
}

static void RegisterLabels(CreatePropertyGraphInfo &info, const shared_ptr<PropertyGraphTable> &table) {
	auto register_label = [&](const Identifier &label) {
		auto name = label.GetIdentifierName();
		if (!info.label_map.emplace(name, table).second) {
			throw ConstraintException("Label %s is not unique, make sure all labels are unique",
			                          StringUtil::Lower(name));
		}
	};
	register_label(table->main_label);
	for (auto &label : table->sub_labels) {
		register_label(label);
	}
}

static arena_ptr<TransformResultValue> TransformCreateGraph(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto or_replace = list.Child<OptionalParseResult>(1).HasResult();
	auto if_not_exists = list.Child<OptionalParseResult>(4).HasResult();
	if (or_replace && if_not_exists) {
		throw ParserException("Cannot specify both OR REPLACE and IF NOT EXISTS within single create statement");
	}
	auto name = transformer.Transform<QualifiedName>(list.GetChild(5));
	if (name.Name().empty()) {
		throw ParserException("Empty property graph name not supported");
	}
	auto info = make_uniq<CreatePropertyGraphInfo>(name.Name().GetIdentifierName());
	info->SetQualifiedName(name);
	info->on_conflict = or_replace      ? OnCreateConflict::REPLACE_ON_CONFLICT
	                    : if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT
	                                    : OnCreateConflict::ERROR_ON_CONFLICT;
	info->vertex_tables = TransformParensList<shared_ptr<PropertyGraphTable>>(
	    transformer, list.GetChild(6).Cast<ListParseResult>().GetChild(2));
	auto &edges = list.Child<OptionalParseResult>(7);
	if (edges.HasResult()) {
		info->edge_tables = TransformParensList<shared_ptr<PropertyGraphTable>>(
		    transformer, edges.GetResult().Cast<ListParseResult>().GetChild(2));
	}
	for (auto &table : info->vertex_tables) {
		RegisterLabels(*info, table);
	}
	for (auto &table : info->edge_tables) {
		table->source_pg_table =
		    FindVertexTable(*info, table->source_catalog, table->source_schema, table->source_reference);
		table->destination_pg_table =
		    FindVertexTable(*info, table->destination_catalog, table->destination_schema, table->destination_reference);
		RegisterLabels(*info, table);
	}
	auto statement = make_uniq<CreateStatement>();
	statement->info = std::move(info);
	auto parse_data = make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(std::move(statement));
	unique_ptr<SQLStatement> result = make_uniq<ExtensionStatement>(DuckPGQParserExtension(), std::move(parse_data));
	return transformer.MakeResult<unique_ptr<SQLStatement>>(std::move(result));
}

static arena_ptr<TransformProcess> StartCreateGraphTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, TransformCreateGraph);
}

static arena_ptr<TransformProcess> StartGraphTableTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, TransformGraphTable);
}

class DuckPGQGrammarExtension final : public GrammarExtension {
public:
	DuckPGQGrammarExtension() : GrammarExtension("duckpgq", "SQL/PGQ CREATE PROPERTY GRAPH syntax") {
	}

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		// New words remain usable as ordinary SQL identifiers outside their PGQ context.
		for (auto keyword : {"ARE", "DESTINATION", "EDGE", "GRAPH", "PROPERTIES", "PROPERTY", "VERTEX"}) {
			changes.push_back(GrammarChange::AddChoice("UnreservedKeyword", "'" + string(keyword) + "'"));
		}
		for (auto rule : DUCKPGQ_CREATE_PROPERTY_GRAPH_RULES) {
			changes.push_back(GrammarChange::AddRule(rule));
		}
		changes.push_back(GrammarChange::SetTransformProcess("DuckPGQCreatePropertyGraph", StartCreateGraphTransform));
		changes.push_back(GrammarChange::SetTransformProcess("DuckPGQVertexTable", StartGraphTableTransform));
		changes.push_back(GrammarChange::SetTransformProcess("DuckPGQEdgeTable", StartGraphTableTransform));
		// Return an ExtensionStatement directly, without intercepting ordinary CREATE statements.
		changes.push_back(GrammarChange::PrependChoice("Statement", "DuckPGQCreatePropertyGraph"));
		return changes;
	}
};

} // namespace

void CorePGQParser::RegisterPGQGrammarExtension(ExtensionLoader &loader) {
	GrammarExtension::Register(loader.GetDatabaseInstance(), make_shared_ptr<DuckPGQGrammarExtension>());
}

} // namespace duckdb
