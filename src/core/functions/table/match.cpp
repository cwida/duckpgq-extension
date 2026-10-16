#include <duckpgq_extension.hpp>
#include "duckpgq/core/functions/table/match.hpp"
#include "duckpgq/core/parser/duckpgq_grammar.hpp"

#include "duckpgq/core/utils/duckpgq_sql.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckpgq/parser/tableref/matchref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"

#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"

#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

#include "duckpgq/parser/property_graph_table.hpp"
#include "duckpgq/parser/subpath_element.hpp"
#include <duckdb/common/enums/set_operation_type.hpp>
#include <duckpgq/core/functions/table.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckdb {

static Identifier PGQIdentifier(const string &value) {
	return Identifier(value);
}

static Identifier PGQIdentifier(const Identifier &value) {
	return value;
}

static void SetExpressionAlias(ParsedExpression &expr, const string &alias) {
	expr.SetAlias(PGQIdentifier(alias));
}

static unique_ptr<ColumnRefExpression> PGQColumnRef(const string &column_name, const string &table_name) {
	return make_uniq<ColumnRefExpression>(PGQIdentifier(column_name), PGQIdentifier(table_name));
}

static unique_ptr<ColumnRefExpression> PGQColumnRef(const Identifier &column_name, const string &table_name) {
	return make_uniq<ColumnRefExpression>(column_name, PGQIdentifier(table_name));
}

static unique_ptr<ColumnRefExpression> PGQColumnRef(const string &table_name, const string &column_name,
                                                    bool qualified) {
	(void)qualified;
	vector<Identifier> column_names;
	column_names.push_back(PGQIdentifier(table_name));
	column_names.push_back(PGQIdentifier(column_name));
	return make_uniq<ColumnRefExpression>(std::move(column_names));
}

static unique_ptr<ColumnRefExpression> PGQColumnRef(const string &table_name, const Identifier &column_name,
                                                    bool qualified) {
	vector<Identifier> column_names;
	column_names.push_back(PGQIdentifier(table_name));
	column_names.push_back(column_name);
	return make_uniq<ColumnRefExpression>(std::move(column_names));
}

static string DuckPGQSQLCountTable(const PropertyGraphTable &table, const string &table_alias,
                                   const Identifier &primary_key) {
	std::ostringstream query;
	query << "SELECT count(" << DuckPGQSQL::Column(primary_key, table_alias) << ") FROM "
	      << DuckPGQSQL::TableRef(table, table_alias);
	return query.str();
}

static void PGQAppendCrossJoin(unique_ptr<TableRef> &from_clause, unique_ptr<TableRef> new_ref) {
	if (from_clause) {
		auto join = make_uniq<JoinRef>(JoinRefType::CROSS);
		join->left = std::move(from_clause);
		join->right = std::move(new_ref);
		from_clause = std::move(join);
	} else {
		from_clause = std::move(new_ref);
	}
}

static bool PGQNormalizeStructExtract(unique_ptr<ParsedExpression> &expression,
                                      const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map);

static void PGQCheckPathModeSupport(const PathReference &path_reference, bool shortest);

static void PGQCheckPathModeSupport(const PathPattern &path_pattern) {
	if (path_pattern.all && path_pattern.shortest) {
		throw NotImplementedException("ALL SHORTEST has not been implemented yet.");
	}
	if (path_pattern.topk) {
		throw NotImplementedException("TopK has not been implemented yet.");
	}
	for (auto &path_reference : path_pattern.path_elements) {
		PGQCheckPathModeSupport(*path_reference, path_pattern.shortest);
	}
}

static void PGQCheckPathModeSupport(const PathReference &path_reference, bool shortest) {
	if (path_reference.path_reference_type != PGQPathReferenceType::SUBPATH) {
		return;
	}
	auto &subpath = reinterpret_cast<const SubPath &>(path_reference);
	if (subpath.path_mode != PGQPathMode::NONE && subpath.path_mode != PGQPathMode::WALK) {
		throw NotImplementedException("Path modes other than WALK have not been implemented yet.");
	}
	if (!shortest && subpath.upper == NumericLimits<int64_t>::Maximum() &&
	    (subpath.path_mode == PGQPathMode::NONE || subpath.path_mode == PGQPathMode::WALK)) {
		throw ConstraintException("ALL unbounded with path mode WALK is not possible as this could lead to infinite "
		                          "results. Consider specifying an upper bound or path mode other than WALK");
	}
	for (auto &child : subpath.path_list) {
		PGQCheckPathModeSupport(*child, shortest);
	}
}

static void PGQNormalizeGraphElementRefs(unique_ptr<ParsedExpression> &expression,
                                         const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map) {
	if (!expression) {
		return;
	}
	if (PGQNormalizeStructExtract(expression, alias_map)) {
		return;
	}
	switch (expression->GetExpressionClass()) {
	case ExpressionClass::OPERATOR: {
		auto &op = expression->Cast<OperatorExpression>();
		for (auto &child : op.GetChildrenMutable()) {
			PGQNormalizeGraphElementRefs(child, alias_map);
		}
		break;
	}
	case ExpressionClass::COMPARISON: {
		auto &comparison = expression->Cast<ComparisonExpression>();
		PGQNormalizeGraphElementRefs(comparison.LeftMutable(), alias_map);
		PGQNormalizeGraphElementRefs(comparison.RightMutable(), alias_map);
		break;
	}
	case ExpressionClass::CONJUNCTION: {
		auto &conjunction = expression->Cast<ConjunctionExpression>();
		for (auto &child : conjunction.GetChildrenMutable()) {
			PGQNormalizeGraphElementRefs(child, alias_map);
		}
		break;
	}
	case ExpressionClass::FUNCTION: {
		auto &function = expression->Cast<FunctionExpression>();
		for (auto &argument : function.GetArgumentsMutable()) {
			PGQNormalizeGraphElementRefs(argument.GetExpressionMutable(), alias_map);
		}
		PGQNormalizeGraphElementRefs(function.FilterMutable(), alias_map);
		break;
	}
	case ExpressionClass::BETWEEN: {
		auto &between = expression->Cast<BetweenExpression>();
		PGQNormalizeGraphElementRefs(between.InputMutable(), alias_map);
		PGQNormalizeGraphElementRefs(between.LowerBoundMutable(), alias_map);
		PGQNormalizeGraphElementRefs(between.UpperBoundMutable(), alias_map);
		break;
	}
	default:
		break;
	}
}

static bool PGQNormalizeStructExtract(unique_ptr<ParsedExpression> &expression,
                                      const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map) {
	if (expression->GetExpressionClass() != ExpressionClass::OPERATOR) {
		return false;
	}
	auto &op = expression->Cast<OperatorExpression>();
	if (op.GetExpressionType() != ExpressionType::STRUCT_EXTRACT) {
		return false;
	}
	auto &children = op.GetChildrenMutable();
	if (children.size() != 2 || children[0]->GetExpressionClass() != ExpressionClass::COLUMN_REF ||
	    children[1]->GetExpressionClass() != ExpressionClass::CONSTANT) {
		return false;
	}
	auto &alias_ref = children[0]->Cast<ColumnRefExpression>();
	if (alias_ref.ColumnNames().size() != 1) {
		return false;
	}
	auto alias = alias_ref.GetColumnName().GetIdentifierName();
	if (alias_map.find(alias) == alias_map.end()) {
		return false;
	}
	auto field = children[1]->Cast<ConstantExpression>().GetLiteral().ToValue();
	expression = PGQColumnRef(alias, field.GetValue<string>(), true);
	return true;
}

namespace {

// Get all fully-qualified column names from the given property graph [pg] for
// the given relation [alias], only vertex table is selected.
//
// Return column reference expressions which represent columns to select.
vector<unique_ptr<ColumnRefExpression>>
GetColRefExprFromPg(const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map, const std::string &alias) {
	vector<unique_ptr<ColumnRefExpression>> registered_col_names;
	auto iter = alias_map.find(alias);
	D_ASSERT(iter != alias_map.end());
	const auto &tbl = iter->second;
	registered_col_names.reserve(tbl->column_names.size());
	for (idx_t i = 0; i < tbl->column_names.size(); i++) {
		registered_col_names.emplace_back(PGQColumnRef(alias, tbl->column_names[i], true));
		// PROPERTIES (column AS property): expose the column under its property name.
		if (i < tbl->column_aliases.size() && tbl->column_aliases[i] != tbl->column_names[i]) {
			registered_col_names.back()->SetAlias(tbl->column_aliases[i]);
		}
	}
	return registered_col_names;
}

// Get all fully-qualified column names from the given property graph [pg] for
// all vertex relations.
//
// Return column reference expressions which represent columns to select.
vector<unique_ptr<ColumnRefExpression>>
GetColRefExprFromPg(const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map) {
	vector<unique_ptr<ColumnRefExpression>> registered_col_names;
	for (const auto &alias_and_table : alias_map) {
		const auto &alias = alias_and_table.first;
		const auto &tbl = alias_and_table.second;
		// Skip edge table.
		registered_col_names.reserve(registered_col_names.size() + tbl->column_names.size());
		for (idx_t i = 0; i < tbl->column_names.size(); i++) {
			registered_col_names.emplace_back(PGQColumnRef(alias, tbl->column_names[i], true));
			if (i < tbl->column_aliases.size() && tbl->column_aliases[i] != tbl->column_names[i]) {
				registered_col_names.back()->SetAlias(tbl->column_aliases[i]);
			}
		}
	}
	return registered_col_names;
}

bool IsSameGraphVertexReference(const PropertyGraphTable &edge_table) {
	if (edge_table.source_pg_table || edge_table.destination_pg_table) {
		return edge_table.source_pg_table && edge_table.destination_pg_table &&
		       edge_table.source_pg_table->SameTableIdentity(*edge_table.destination_pg_table);
	}
	return edge_table.source_catalog == edge_table.destination_catalog &&
	       edge_table.source_schema == edge_table.destination_schema &&
	       edge_table.source_reference == edge_table.destination_reference;
}

string CreateReverseSelfLoopFilter(const PropertyGraphTable &edge_table, const string &edge_binding) {
	if (!IsSameGraphVertexReference(edge_table)) {
		return "";
	}

	vector<string> equality_conditions;
	for (idx_t source_idx = 0; source_idx < edge_table.source_pk.size(); source_idx++) {
		for (idx_t destination_idx = 0; destination_idx < edge_table.destination_pk.size(); destination_idx++) {
			if (edge_table.source_pk[source_idx] != edge_table.destination_pk[destination_idx]) {
				continue;
			}
			equality_conditions.push_back(DuckPGQSQL::Column(edge_table.source_fk[source_idx], edge_binding) +
			                              " IS NOT DISTINCT FROM " +
			                              DuckPGQSQL::Column(edge_table.destination_fk[destination_idx], edge_binding));
			break;
		}
	}

	if (equality_conditions.size() != edge_table.source_pk.size()) {
		return "";
	}
	return " WHERE NOT (" + StringUtil::Join(equality_conditions, " AND ") + ")";
}

} // namespace

static vector<string> PGQMainLabels(const vector<shared_ptr<PropertyGraphTable>> &tables) {
	vector<string> labels;
	for (auto &table : tables) {
		labels.push_back(table->main_label.GetIdentifierName());
	}
	return labels;
}

struct PGQFlatElement {
	PathElement *element;
	//! True when the element sits inside a quantified subpath and is evaluated by path-finding.
	bool quantified;
};

static void PGQFlattenPathElements(vector<unique_ptr<PathReference>> &path_list, vector<PGQFlatElement> &elements,
                                   bool quantified = false) {
	for (auto &path_reference : path_list) {
		if (path_reference->path_reference_type == PGQPathReferenceType::PATH_ELEMENT) {
			elements.push_back({reinterpret_cast<PathElement *>(path_reference.get()), quantified});
		} else {
			auto &subpath = *reinterpret_cast<SubPath *>(path_reference.get());
			PGQFlattenPathElements(subpath.path_list, elements,
			                       quantified || subpath.lower != 1 || subpath.upper != 1);
		}
	}
}

// The outcome of evaluating a label expression against one table: a constant, or a condition on the rows of the table.
struct PGQLabelCondition {
	static PGQLabelCondition Constant(bool value) {
		PGQLabelCondition result;
		result.constant = true;
		result.value = value;
		return result;
	}
	static PGQLabelCondition Condition(string sql) {
		PGQLabelCondition result;
		result.constant = false;
		result.sql = std::move(sql);
		return result;
	}
	bool IsFalse() const {
		return constant && !value;
	}
	bool IsTrue() const {
		return constant && value;
	}

	bool constant = true;
	bool value = false;
	string sql;
};

// A table carries its main label on every row and each sublabel on the rows whose discriminator has the sublabel's bit
// set (CREATE PROPERTY GRAPH ... LABEL main IN discriminator (sub, ...)).
static PGQLabelCondition PGQEvaluateLabelExpression(const PGQLabelExpression &expression,
                                                    const PropertyGraphTable &table, const string &alias) {
	switch (expression.type) {
	case PGQLabelExpressionType::WILDCARD:
		return PGQLabelCondition::Constant(true);
	case PGQLabelExpressionType::LABEL: {
		if (StringUtil::CIEquals(table.main_label.GetIdentifierName(), expression.label)) {
			return PGQLabelCondition::Constant(true);
		}
		if (table.discriminator.empty()) {
			return PGQLabelCondition::Constant(false);
		}
		for (idx_t i = 0; i < table.sub_labels.size(); i++) {
			if (StringUtil::CIEquals(table.sub_labels[i].GetIdentifierName(), expression.label)) {
				auto bit = std::to_string(static_cast<int64_t>(std::pow(2, i)));
				return PGQLabelCondition::Condition("(" + DuckPGQSQL::Column(table.discriminator, alias) + " & " + bit +
				                                    ") = " + bit);
			}
		}
		return PGQLabelCondition::Constant(false);
	}
	case PGQLabelExpressionType::NEGATION: {
		auto child = PGQEvaluateLabelExpression(*expression.children[0], table, alias);
		if (child.constant) {
			return PGQLabelCondition::Constant(!child.value);
		}
		return PGQLabelCondition::Condition("NOT (" + child.sql + ")");
	}
	case PGQLabelExpressionType::CONJUNCTION:
	case PGQLabelExpressionType::DISJUNCTION: {
		bool conjunction = expression.type == PGQLabelExpressionType::CONJUNCTION;
		vector<string> parts;
		for (auto &child_expression : expression.children) {
			auto child = PGQEvaluateLabelExpression(*child_expression, table, alias);
			if (child.constant) {
				// FALSE decides a conjunction and TRUE a disjunction; the other constant is neutral.
				if (child.value != conjunction) {
					return child;
				}
				continue;
			}
			parts.push_back("(" + child.sql + ")");
		}
		if (parts.empty()) {
			return PGQLabelCondition::Constant(conjunction);
		}
		return PGQLabelCondition::Condition(StringUtil::Join(parts, conjunction ? " AND " : " OR "));
	}
	}
	throw InternalException("Unknown label expression type");
}

// Bind an element with a compound label expression (10.8). Because an element binds to exactly one table, the
// expression must be satisfiable by the rows of exactly one vertex or edge table; a condition on that table's
// discriminator column is added when the expression only holds for some of its rows.
static void PGQBindLabelExpression(PGQFlatElement &flat, CreatePropertyGraphInfo &pg_table,
                                   vector<unique_ptr<ParsedExpression>> &conditions) {
	auto &element = *flat.element;
	auto &expression = *element.label_expression;
	vector<string> labels;
	expression.CollectLabels(labels);
	for (auto &label : labels) {
		if (pg_table.label_map.find(label) == pg_table.label_map.end()) {
			throw BinderException("The label %s is not registered in property graph %s", label,
			                      pg_table.property_graph_name);
		}
	}
	bool vertex = element.match_type == PGQMatchType::MATCH_VERTEX;
	auto &tables = vertex ? pg_table.vertex_tables : pg_table.edge_tables;
	vector<shared_ptr<PropertyGraphTable>> candidates;
	PGQLabelCondition condition;
	for (auto &table : tables) {
		auto evaluated = PGQEvaluateLabelExpression(expression, *table, element.variable_binding);
		if (evaluated.IsFalse()) {
			continue;
		}
		candidates.push_back(table);
		condition = evaluated;
	}
	if (candidates.empty()) {
		throw BinderException("Label expression %s matches no %s table of property graph %s", expression.ToString(),
		                      vertex ? "vertex" : "edge", pg_table.property_graph_name);
	}
	if (candidates.size() > 1) {
		throw NotImplementedException("Label expression %s matches several tables (%s); an element pattern binds to "
		                              "one table in DuckPGQ",
		                              expression.ToString(), StringUtil::Join(PGQMainLabels(candidates), ", "));
	}
	if (!condition.IsTrue() && flat.quantified) {
		throw NotImplementedException("Label expression %s selects part of table %s; sublabel filters on a "
		                              "quantified edge are not supported",
		                              expression.ToString(), candidates[0]->main_label.GetIdentifierName());
	}
	element.label = candidates[0]->main_label.GetIdentifierName();
	if (!condition.IsTrue()) {
		conditions.push_back(DuckPGQSQL::ParseExpression(condition.sql));
	}
}

static bool PGQSameVertexTable(const shared_ptr<PropertyGraphTable> &reference,
                               const shared_ptr<PropertyGraphTable> &table) {
	return reference && reference->SameTableIdentity(*table);
}

// Resolve element patterns without a label (10.6: the label is optional in the <element pattern filler>).
// A named element whose variable is a registered label keeps the historical DuckPGQ reading (a) == (a:a).
// Otherwise a vertex binds to the only vertex table, and an edge to the only edge table that connects its endpoint
// tables in the pattern's direction; anything else is ambiguous, because every element binds to exactly one table.
static void PGQResolveElementLabels(PathPattern &path_pattern, CreatePropertyGraphInfo &pg_table,
                                    vector<unique_ptr<ParsedExpression>> &conditions) {
	vector<PGQFlatElement> flat_elements;
	PGQFlattenPathElements(path_pattern.path_elements, flat_elements);
	vector<PathElement *> elements;
	for (auto &flat : flat_elements) {
		if (flat.element->label_expression) {
			PGQBindLabelExpression(flat, pg_table, conditions);
		}
		elements.push_back(flat.element);
	}
	for (auto element : elements) {
		if (!element->label.empty() || element->match_type != PGQMatchType::MATCH_VERTEX) {
			continue;
		}
		if (!element->anonymous && pg_table.label_map.find(element->variable_binding) != pg_table.label_map.end()) {
			element->label = element->variable_binding;
			continue;
		}
		if (pg_table.vertex_tables.size() != 1) {
			throw BinderException("Vertex pattern %s has no label and property graph %s has %s vertex tables (%s); "
			                      "specify a label",
			                      element->ToString(), pg_table.property_graph_name,
			                      std::to_string(pg_table.vertex_tables.size()),
			                      StringUtil::Join(PGQMainLabels(pg_table.vertex_tables), ", "));
		}
		element->label = pg_table.vertex_tables[0]->main_label.GetIdentifierName();
	}
	for (idx_t i = 0; i < elements.size(); i++) {
		auto element = elements[i];
		if (!element->label.empty() || element->match_type == PGQMatchType::MATCH_VERTEX) {
			continue;
		}
		if (!element->anonymous && pg_table.label_map.find(element->variable_binding) != pg_table.label_map.end()) {
			element->label = element->variable_binding;
			continue;
		}
		if (i == 0 || i + 1 >= elements.size() || elements[i - 1]->match_type != PGQMatchType::MATCH_VERTEX ||
		    elements[i + 1]->match_type != PGQMatchType::MATCH_VERTEX) {
			throw BinderException("Vertex and edge patterns must be alternated.");
		}
		auto prev_table = PGQMatchFunction::FindGraphTable(elements[i - 1]->label, pg_table);
		auto next_table = PGQMatchFunction::FindGraphTable(elements[i + 1]->label, pg_table);
		vector<shared_ptr<PropertyGraphTable>> candidates;
		for (auto &edge_table : pg_table.edge_tables) {
			bool forward = PGQSameVertexTable(edge_table->source_pg_table, prev_table) &&
			               PGQSameVertexTable(edge_table->destination_pg_table, next_table);
			bool backward = PGQSameVertexTable(edge_table->source_pg_table, next_table) &&
			                PGQSameVertexTable(edge_table->destination_pg_table, prev_table);
			bool connects = element->match_type == PGQMatchType::MATCH_EDGE_RIGHT  ? forward
			                : element->match_type == PGQMatchType::MATCH_EDGE_LEFT ? backward
			                                                                        : forward || backward;
			if (connects) {
				candidates.push_back(edge_table);
			}
		}
		if (candidates.empty()) {
			throw BinderException("Edge pattern %s has no label and no edge table of property graph %s connects %s "
			                      "and %s in that direction",
			                      element->ToString(), pg_table.property_graph_name, elements[i - 1]->label,
			                      elements[i + 1]->label);
		}
		if (candidates.size() > 1) {
			throw BinderException("Edge pattern %s has no label and is ambiguous: edge tables %s all connect %s and %s; "
			                      "specify a label",
			                      element->ToString(), StringUtil::Join(PGQMainLabels(candidates), ", "),
			                      elements[i - 1]->label, elements[i + 1]->label);
		}
		element->label = candidates[0]->main_label.GetIdentifierName();
	}
}

shared_ptr<PropertyGraphTable> PGQMatchFunction::FindGraphTable(const string &label,
                                                                CreatePropertyGraphInfo &pg_table) {
	const auto graph_table_entry = pg_table.label_map.find(label);
	if (graph_table_entry == pg_table.label_map.end()) {
		throw Exception(ExceptionType::BINDER,
		                "The label " + label + " is not registered in property graph " + pg_table.property_graph_name);
	}

	return graph_table_entry->second;
}

void PGQMatchFunction::CheckInheritance(const shared_ptr<PropertyGraphTable> &tableref, PathElement *element,
                                        vector<unique_ptr<ParsedExpression>> &conditions) {
	if (tableref->main_label == element->label) {
		return;
	}
	if (tableref->discriminator.empty()) {
		throw BinderException("Label %s is not a sublabel of %s", element->label,
		                      tableref->main_label.GetIdentifierName());
	}
	const auto itr = std::find(tableref->sub_labels.begin(), tableref->sub_labels.end(), element->label);
	if (itr == tableref->sub_labels.end()) {
		throw BinderException("Label %s is not a sublabel of %s", element->label,
		                      tableref->main_label.GetIdentifierName());
	}

	const auto idx_of_label = std::distance(tableref->sub_labels.begin(), itr);
	std::ostringstream condition;
	condition << "(" << DuckPGQSQL::Column(tableref->discriminator, element->variable_binding) << " & CAST(power(2, "
	          << idx_of_label << ") AS INTEGER)) = " << static_cast<int32_t>(std::pow(2, idx_of_label));
	conditions.push_back(DuckPGQSQL::ParseExpression(condition.str()));
}

void PGQMatchFunction::CheckEdgeTableConstraints(const Identifier &src_reference, const Identifier &dst_reference,
                                                 const shared_ptr<PropertyGraphTable> &edge_table) {
	if (src_reference != edge_table->source_reference) {
		throw BinderException("Label %s is not registered as a source reference "
		                      "for edge pattern of table %s",
		                      src_reference.GetIdentifierName(), edge_table->table_name.GetIdentifierName());
	}
	if (dst_reference != edge_table->destination_reference) {
		throw BinderException("Label %s is not registered as a destination "
		                      "reference for edge pattern of table %s",
		                      dst_reference.GetIdentifierName(), edge_table->table_name.GetIdentifierName());
	}
}

unique_ptr<ParsedExpression> PGQMatchFunction::CreateMatchJoinExpression(vector<Identifier> vertex_keys,
                                                                         vector<Identifier> edge_keys,
                                                                         const string &vertex_alias,
                                                                         const string &edge_alias) {
	vector<unique_ptr<ParsedExpression>> conditions;

	if (vertex_keys.size() != edge_keys.size()) {
		throw BinderException("Vertex columns and edge columns size mismatch");
	}
	for (idx_t i = 0; i < vertex_keys.size(); i++) {
		auto vertex_colref = PGQColumnRef(vertex_keys[i], vertex_alias);
		auto edge_colref = PGQColumnRef(edge_keys[i], edge_alias);
		conditions.push_back(make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(vertex_colref),
		                                                     std::move(edge_colref)));
	}
	unique_ptr<ParsedExpression> where_clause;

	for (auto &condition : conditions) {
		if (where_clause) {
			where_clause = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(where_clause),
			                                                std::move(condition));
		} else {
			where_clause = std::move(condition);
		}
	}

	return where_clause;
}

// A quantified edge is evaluated by path-finding unless it is exactly one edge: {0,1} and ? admit the zero-length path.
static bool PGQNeedsPathFinding(const SubPath &subpath) {
	return subpath.upper > 1 || subpath.lower == 0;
}

PathElement *PGQMatchFunction::GetPathElement(const unique_ptr<PathReference> &path_reference) {
	if (path_reference->path_reference_type == PGQPathReferenceType::PATH_ELEMENT) {
		return reinterpret_cast<PathElement *>(path_reference.get());
	}
	if (path_reference->path_reference_type == PGQPathReferenceType::SUBPATH) {
		return nullptr;
	}
	throw InternalException("Unknown path reference type detected");
}

SubPath *PGQMatchFunction::GetSubPath(const unique_ptr<PathReference> &path_reference) {
	if (path_reference->path_reference_type == PGQPathReferenceType::PATH_ELEMENT) {
		return nullptr;
	}
	if (path_reference->path_reference_type == PGQPathReferenceType::SUBPATH) {
		return reinterpret_cast<SubPath *>(path_reference.get());
	}
	throw InternalException("Unknown path reference type detected");
}

unique_ptr<SubqueryRef> PGQMatchFunction::CreateCountCTESubquery() {
	return DuckPGQSQL::ParseSubqueryRef("SELECT multiply(0, count(cte1.temp)) AS temp FROM cte1", "__x");
}

void PGQMatchFunction::EdgeTypeAny(const shared_ptr<PropertyGraphTable> &edge_table, const string &edge_binding,
                                   const string &prev_binding, const string &next_binding,
                                   vector<unique_ptr<ParsedExpression>> &conditions,
                                   unique_ptr<TableRef> &from_clause) {
	std::ostringstream query;
	query << "SELECT " << DuckPGQSQL::Column(edge_table->source_fk[0], edge_binding) << " AS "
	      << DuckPGQSQL::Identifier(edge_table->source_fk[0]) << ", "
	      << DuckPGQSQL::Column(edge_table->destination_fk[0], edge_binding) << " AS "
	      << DuckPGQSQL::Identifier(edge_table->destination_fk[0]) << ", * FROM "
	      << DuckPGQSQL::TableRef(*edge_table, edge_binding) << " UNION ALL SELECT "
	      << DuckPGQSQL::Column(edge_table->destination_fk[0], edge_binding) << " AS "
	      << DuckPGQSQL::Identifier(edge_table->source_fk[0]) << ", "
	      << DuckPGQSQL::Column(edge_table->source_fk[0], edge_binding) << " AS "
	      << DuckPGQSQL::Identifier(edge_table->destination_fk[0]) << ", * FROM "
	      << DuckPGQSQL::TableRef(*edge_table, edge_binding) << CreateReverseSelfLoopFilter(*edge_table, edge_binding);
	PGQAppendCrossJoin(from_clause, DuckPGQSQL::ParseSubqueryRef(query.str(), edge_binding));
	// (a) src.key = edge.src
	auto src_left_expr =
	    CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk, prev_binding, edge_binding);
	// (b) dst.key = edge.dst
	auto dst_left_expr =
	    CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk, next_binding, edge_binding);
	// (a) AND (b)
	auto combined_left_expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
	                                                           std::move(src_left_expr), std::move(dst_left_expr));

	conditions.push_back(std::move(combined_left_expr));
}

void PGQMatchFunction::EdgeTypeLeft(const shared_ptr<PropertyGraphTable> &edge_table, const Identifier &next_table_name,
                                    const Identifier &prev_table_name, const string &edge_binding,
                                    const string &prev_binding, const string &next_binding,
                                    vector<unique_ptr<ParsedExpression>> &conditions) {
	CheckEdgeTableConstraints(next_table_name, prev_table_name, edge_table);
	conditions.push_back(
	    CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk, next_binding, edge_binding));
	conditions.push_back(
	    CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk, prev_binding, edge_binding));
}

void PGQMatchFunction::EdgeTypeRight(const shared_ptr<PropertyGraphTable> &edge_table,
                                     const Identifier &next_table_name, const Identifier &prev_table_name,
                                     const string &edge_binding, const string &prev_binding, const string &next_binding,
                                     vector<unique_ptr<ParsedExpression>> &conditions) {
	CheckEdgeTableConstraints(prev_table_name, next_table_name, edge_table);
	conditions.push_back(
	    CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk, prev_binding, edge_binding));
	conditions.push_back(
	    CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk, next_binding, edge_binding));
}

void PGQMatchFunction::EdgeTypeLeftRight(const shared_ptr<PropertyGraphTable> &edge_table, const string &edge_binding,
                                         const string &prev_binding, const string &next_binding,
                                         vector<unique_ptr<ParsedExpression>> &conditions,
                                         case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
                                         int32_t &extra_alias_counter) {
	auto src_left_expr =
	    CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk, next_binding, edge_binding);
	auto dst_left_expr =
	    CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk, prev_binding, edge_binding);

	auto combined_left_expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
	                                                           std::move(src_left_expr), std::move(dst_left_expr));

	const auto additional_edge_alias = edge_binding + std::to_string(extra_alias_counter);
	extra_alias_counter++;

	alias_map[additional_edge_alias] = edge_table;

	auto src_right_expr =
	    CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk, prev_binding, additional_edge_alias);
	auto dst_right_expr = CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
	                                                next_binding, additional_edge_alias);
	auto combined_right_expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
	                                                            std::move(src_right_expr), std::move(dst_right_expr));

	auto combined_expr = make_uniq<ConjunctionExpression>(
	    ExpressionType::CONJUNCTION_AND, std::move(combined_left_expr), std::move(combined_right_expr));
	conditions.push_back(std::move(combined_expr));
}

PathElement *PGQMatchFunction::HandleNestedSubPath(unique_ptr<PathReference> &path_reference,
                                                   vector<unique_ptr<ParsedExpression>> &conditions,
                                                   idx_t element_idx) {
	auto subpath = reinterpret_cast<SubPath *>(path_reference.get());
	return GetPathElement(subpath->path_list[element_idx]);
}

unique_ptr<ParsedExpression> PGQMatchFunction::CreateWhereClause(vector<unique_ptr<ParsedExpression>> &conditions) {
	unique_ptr<ParsedExpression> where_clause;
	for (auto &condition : conditions) {
		if (where_clause) {
			where_clause = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(where_clause),
			                                                std::move(condition));
		} else {
			where_clause = std::move(condition);
		}
	}
	return where_clause;
}

// Path-finding runs along the edge direction. For a left-pointing edge pattern (a)<-[e]-(b) the search therefore starts
// at the right endpoint and ends at the left one; the resulting path is reversed so it still reads from a to b.
static void PGQPathFindingEndpoints(PGQMatchType edge_type, const string &prev_binding, const string &next_binding,
                                    string &from_binding, string &to_binding) {
	if (edge_type == PGQMatchType::MATCH_EDGE_LEFT) {
		from_binding = next_binding;
		to_binding = prev_binding;
	} else {
		from_binding = prev_binding;
		to_binding = next_binding;
	}
}

static unique_ptr<CommonTableExpressionInfo> PGQCreateCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                                                            PGQMatchType edge_type, const string &prev_binding,
                                                            const string &edge_binding, const string &next_binding,
                                                            unique_ptr<SelectNode> &select_node) {
	switch (edge_type) {
	case PGQMatchType::MATCH_EDGE_RIGHT:
	case PGQMatchType::MATCH_EDGE_LEFT: {
		string from_binding, to_binding;
		PGQPathFindingEndpoints(edge_type, prev_binding, next_binding, from_binding, to_binding);
		return CreateDirectedCSRCTE(edge_table, from_binding, edge_binding, to_binding);
	}
	case PGQMatchType::MATCH_EDGE_ANY:
		return CreateUndirectedCSRCTE(edge_table, select_node);
	default:
		throw NotImplementedException("Path-finding over a <-[e]-> edge pattern is not supported");
	}
}

unique_ptr<CommonTableExpressionInfo>
PGQMatchFunction::GenerateShortestPathCTE(CreatePropertyGraphInfo &pg_table, SubPath *edge_subpath,
                                          PathElement *previous_vertex_element, PathElement *next_vertex_element,
                                          vector<unique_ptr<ParsedExpression>> &path_finding_conditions) {
	auto edge_element = GetPathElement(edge_subpath->path_list[0]);
	auto edge_table = FindGraphTable(edge_element->label, pg_table);
	string from_binding, to_binding;
	PGQPathFindingEndpoints(edge_element->match_type, previous_vertex_element->variable_binding,
	                        next_vertex_element->variable_binding, from_binding, to_binding);
	bool reverse_path = edge_element->match_type == PGQMatchType::MATCH_EDGE_LEFT;

	path_finding_conditions.push_back(AddPathQuantifierCondition(from_binding, to_binding, edge_table, edge_subpath));

	std::ostringstream query;
	query << "SELECT " << (reverse_path ? "list_reverse(" : "") << "shortestpath(0, ("
	      << DuckPGQSQLCountTable(*edge_table->source_pg_table, previous_vertex_element->variable_binding,
	                              edge_table->source_pk[0])
	      << "), " << DuckPGQSQL::Column(string("rowid"), from_binding) << ", "
	      << DuckPGQSQL::Column(string("rowid"), to_binding) << ")" << (reverse_path ? ")" : "") << " AS path, "
	      << DuckPGQSQL::Column(string("rowid"), previous_vertex_element->variable_binding) << " AS src_rowid, "
	      << DuckPGQSQL::Column(string("rowid"), next_vertex_element->variable_binding) << " AS dst_rowid FROM "
	      << DuckPGQSQL::TableRef(*edge_table->source_pg_table, previous_vertex_element->variable_binding)
	      << " CROSS JOIN "
	      << DuckPGQSQL::TableRef(*edge_table->destination_pg_table, next_vertex_element->variable_binding)
	      << " CROSS JOIN (SELECT multiply(0, count(cte1.temp)) AS temp FROM cte1) AS __x";

	auto select_statement = DuckPGQSQL::ParseSelect(query.str());
	auto &select_node = select_statement->node->Cast<SelectNode>();
	select_node.where_clause = CreateWhereClause(path_finding_conditions);
	auto cte_info = make_uniq<CommonTableExpressionInfo>();
	cte_info->query_node = std::move(select_statement->node);
	return cte_info;
}

unique_ptr<ParsedExpression> PGQMatchFunction::CreatePathFindingFunction(
    vector<unique_ptr<PathReference>> &path_list, CreatePropertyGraphInfo &pg_table, const string &path_variable,
    unique_ptr<SelectNode> &final_select_node, vector<unique_ptr<ParsedExpression>> &conditions) {
	// This method will return a SubqueryRef of a list of rowids
	// For every vertex and edge element, we add the rowid to the list using
	// list_append, or list_prepend The difficulty is that there may be a
	// (un)bounded path pattern at some point in the query This is computed using
	// the shortestpath() UDF and returns a list. This list will be part of the
	// full list of element rowids, using list_concat. For now we will only
	// support returning rowids

	unique_ptr<ParsedExpression> final_list;
	vector<unique_ptr<ParsedExpression>> path_finding_conditions;
	auto previous_vertex_element = GetPathElement(path_list[0]);
	SubPath *previous_vertex_subpath = nullptr; // NOLINT
	if (!previous_vertex_element) {
		// We hit a vertex element with a WHERE, but we only care about the rowid
		// here
		// In the future this might be a recursive path pattern
		previous_vertex_subpath = reinterpret_cast<SubPath *>(path_list[0].get());
		previous_vertex_element = GetPathElement(previous_vertex_subpath->path_list[0]);
	}

	for (idx_t idx_i = 1; idx_i < path_list.size(); idx_i = idx_i + 2) {
		auto next_vertex_element = GetPathElement(path_list[idx_i + 1]);
		SubPath *next_vertex_subpath = nullptr; // NOLINT
		if (!next_vertex_element) {
			next_vertex_subpath = reinterpret_cast<SubPath *>(path_list[idx_i + 1].get());
			next_vertex_element = GetPathElement(next_vertex_subpath->path_list[0]);
		}

		auto edge_element = GetPathElement(path_list[idx_i]);
		if (!edge_element) {
			auto edge_subpath = reinterpret_cast<SubPath *>(path_list[idx_i].get());
			if (PGQNeedsPathFinding(*edge_subpath)) {
				// (un)bounded shortest path
				// Add the shortest path UDF as a CTE
				if (previous_vertex_subpath) {
					path_finding_conditions.push_back(std::move(previous_vertex_subpath->where_clause));
				}
				if (next_vertex_subpath) {
					path_finding_conditions.push_back(std::move(next_vertex_subpath->where_clause));
				}
				if (final_select_node->cte_map.map.find(Identifier("cte1")) == final_select_node->cte_map.map.end()) {
					edge_element = reinterpret_cast<PathElement *>(edge_subpath->path_list[0].get());
					final_select_node->cte_map.map[Identifier("cte1")] = PGQCreateCSRCTE(
					    FindGraphTable(edge_element->label, pg_table), edge_element->match_type,
					    previous_vertex_element->variable_binding, edge_element->variable_binding,
					    next_vertex_element->variable_binding, final_select_node);
				}
				string shortest_path_cte_name = "shortest_path_cte";
				if (final_select_node->cte_map.map.find(PGQIdentifier(shortest_path_cte_name)) ==
				    final_select_node->cte_map.map.end()) {
					final_select_node->cte_map.map[PGQIdentifier(shortest_path_cte_name)] = GenerateShortestPathCTE(
					    pg_table, edge_subpath, previous_vertex_element, next_vertex_element, path_finding_conditions);
					PGQAppendCrossJoin(final_select_node->from_table,
					                   DuckPGQSQL::ParseFromTableRef(shortest_path_cte_name));

					conditions.push_back(make_uniq<ComparisonExpression>(
					    ExpressionType::COMPARE_EQUAL, PGQColumnRef(string("src_rowid"), shortest_path_cte_name),
					    PGQColumnRef(string("rowid"), previous_vertex_element->variable_binding)));
					conditions.push_back(make_uniq<ComparisonExpression>(
					    ExpressionType::COMPARE_EQUAL, PGQColumnRef(string("dst_rowid"), shortest_path_cte_name),
					    PGQColumnRef(string("rowid"), next_vertex_element->variable_binding)));
				}
				auto shortest_path_ref = PGQColumnRef(string("path"), shortest_path_cte_name);
				if (!final_list) {
					final_list = std::move(shortest_path_ref);
				} else {
					auto pop_front_shortest_path_children = vector<unique_ptr<ParsedExpression>>();
					pop_front_shortest_path_children.push_back(std::move(shortest_path_ref));
					auto pop_front =
					    make_uniq<FunctionExpression>("array_pop_front", std::move(pop_front_shortest_path_children));

					auto final_list_children = vector<unique_ptr<ParsedExpression>>();
					final_list_children.push_back(std::move(final_list));
					final_list_children.push_back(std::move(pop_front));
					final_list = make_uniq<FunctionExpression>("list_concat", std::move(final_list_children));
				}
				// Set next vertex to be previous
				previous_vertex_element = next_vertex_element;
				continue;
			}
			if (previous_vertex_subpath) {
				conditions.push_back(std::move(previous_vertex_subpath->where_clause));
			}
			if (next_vertex_subpath) {
				conditions.push_back(std::move(next_vertex_subpath->where_clause));
			}
			edge_element = GetPathElement(edge_subpath->path_list[0]);
		}
		auto previous_rowid = PGQColumnRef(string("rowid"), previous_vertex_element->variable_binding);
		auto edge_rowid = PGQColumnRef(string("rowid"), edge_element->variable_binding);
		auto next_rowid = PGQColumnRef(string("rowid"), next_vertex_element->variable_binding);
		auto starting_list_children = vector<unique_ptr<ParsedExpression>>();

		if (!final_list) {
			starting_list_children.push_back(std::move(previous_rowid));
			starting_list_children.push_back(std::move(edge_rowid));
			starting_list_children.push_back(std::move(next_rowid));
			final_list = make_uniq<FunctionExpression>("list_value", std::move(starting_list_children));
		} else {
			starting_list_children.push_back(std::move(edge_rowid));
			starting_list_children.push_back(std::move(next_rowid));
			auto next_elements_list = make_uniq<FunctionExpression>("list_value", std::move(starting_list_children));
			auto final_list_children = vector<unique_ptr<ParsedExpression>>();
			final_list_children.push_back(std::move(final_list));
			final_list_children.push_back(std::move(next_elements_list));
			final_list = make_uniq<FunctionExpression>("list_concat", std::move(final_list_children));
		}
		previous_vertex_element = next_vertex_element;
		previous_vertex_subpath = next_vertex_subpath;
	}

	return final_list;
}

void PGQMatchFunction::AddEdgeJoins(const shared_ptr<PropertyGraphTable> &edge_table,
                                    const shared_ptr<PropertyGraphTable> &previous_vertex_table,
                                    const shared_ptr<PropertyGraphTable> &next_vertex_table, PGQMatchType edge_type,
                                    const string &edge_binding, const string &prev_binding, const string &next_binding,
                                    vector<unique_ptr<ParsedExpression>> &conditions,
                                    case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
                                    int32_t &extra_alias_counter, unique_ptr<TableRef> &from_clause) {
	if (edge_type != PGQMatchType::MATCH_EDGE_ANY) {
		alias_map[edge_binding] = edge_table;
	}
	switch (edge_type) {
	case PGQMatchType::MATCH_EDGE_ANY: {
		EdgeTypeAny(edge_table, edge_binding, prev_binding, next_binding, conditions, from_clause);
		break;
	}
	case PGQMatchType::MATCH_EDGE_LEFT:
		EdgeTypeLeft(edge_table, next_vertex_table->table_name, previous_vertex_table->table_name, edge_binding,
		             prev_binding, next_binding, conditions);
		break;
	case PGQMatchType::MATCH_EDGE_RIGHT:
		EdgeTypeRight(edge_table, next_vertex_table->table_name, previous_vertex_table->table_name, edge_binding,
		              prev_binding, next_binding, conditions);
		break;
	case PGQMatchType::MATCH_EDGE_LEFT_RIGHT: {
		EdgeTypeLeftRight(edge_table, edge_binding, prev_binding, next_binding, conditions, alias_map,
		                  extra_alias_counter);
		break;
	}
	default:
		throw InternalException("Unknown match type found");
	}
}

unique_ptr<ParsedExpression>
PGQMatchFunction::AddPathQuantifierCondition(const string &prev_binding, const string &next_binding,
                                             const shared_ptr<PropertyGraphTable> &edge_table, const SubPath *subpath) {
	std::ostringstream expression;
	expression << "add(" << DuckPGQSQL::Column(string("temp"), string("__x")) << ", iterativelength(0, ("
	           << DuckPGQSQLCountTable(*edge_table->source_pg_table, prev_binding, edge_table->source_pk[0]) << "), "
	           << DuckPGQSQL::Column(string("rowid"), prev_binding) << ", "
	           << DuckPGQSQL::Column(string("rowid"), next_binding) << "))";
	if (subpath->upper == NumericLimits<int64_t>::Maximum()) {
		expression << " >= " << subpath->lower;
		return DuckPGQSQL::ParseExpression(expression.str());
	}
	expression << " BETWEEN " << subpath->lower << " AND " << subpath->upper;
	return DuckPGQSQL::ParseExpression(expression.str());
}

void PGQMatchFunction::AddPathFinding(unique_ptr<SelectNode> &select_node,
                                      vector<unique_ptr<ParsedExpression>> &conditions, const string &prev_binding,
                                      const string &edge_binding, const string &next_binding,
                                      const shared_ptr<PropertyGraphTable> &edge_table,
                                      CreatePropertyGraphInfo &pg_table, SubPath *subpath, PGQMatchType edge_type) {
	//! START
	//! FROM (SELECT count(cte1.temp) * 0 as temp from cte1) __x
	if (select_node->cte_map.map.find("cte1") == select_node->cte_map.map.end()) {
		select_node->cte_map.map["cte1"] =
		    PGQCreateCSRCTE(edge_table, edge_type, prev_binding, edge_binding, next_binding, select_node);
	}
	if (select_node->cte_map.map.find("shortest_path_cte") != select_node->cte_map.map.end()) {
		return;
	}
	auto temp_cte_select_subquery = CreateCountCTESubquery();
	PGQAppendCrossJoin(select_node->from_table, std::move(temp_cte_select_subquery));
	//! END
	//! FROM (SELECT count(cte1.temp) * 0 as temp from cte1) __x

	//! START
	//! WHERE __x.temp + iterativelength(<csr_id>, (SELECT count(c.id)
	//!       from dst c, a.rowid, b.rowid) between lower and upper
	string from_binding, to_binding;
	PGQPathFindingEndpoints(edge_type, prev_binding, next_binding, from_binding, to_binding);
	conditions.push_back(AddPathQuantifierCondition(from_binding, to_binding, edge_table, subpath));
	//! END
	//! WHERE __x.temp + iterativelength(<csr_id>, (SELECT count(s.id)
	//! from src s, a.rowid, b.rowid) between lower and upper
}

void PGQMatchFunction::CheckNamedSubpath(SubPath &subpath, MatchExpression &original_ref,
                                         CreatePropertyGraphInfo &pg_table, unique_ptr<SelectNode> &final_select_node,
                                         vector<unique_ptr<ParsedExpression>> &conditions) {
	for (idx_t idx_i = 0; idx_i < original_ref.column_list.size(); idx_i++) {
		auto parsed_ref = dynamic_cast<FunctionExpression *>(original_ref.column_list[idx_i].get());
		if (parsed_ref == nullptr) {
			continue;
		}
		if (parsed_ref->GetArgumentsMutable().empty()) {
			continue;
		}
		auto column_ref =
		    dynamic_cast<ColumnRefExpression *>(parsed_ref->GetArgumentsMutable()[0].GetExpressionMutable().get());
		if (column_ref == nullptr) {
			continue;
		}

		if (column_ref->ColumnNames()[0] != subpath.path_variable) {
			continue;
		}
		// Trying to check parsed_ref->alias directly leads to a segfault
		string column_alias = parsed_ref->GetAlias().GetIdentifierName();
		if (parsed_ref->FunctionName() == "element_id") {
			// Check subpath name matches the column referenced in the function -->
			// element_id(named_subpath)
			auto shortest_path_function = CreatePathFindingFunction(subpath.path_list, pg_table, subpath.path_variable,
			                                                        final_select_node, conditions);

			if (column_alias.empty()) {
				SetExpressionAlias(*shortest_path_function, "element_id(" + subpath.path_variable + ")");
			} else {
				SetExpressionAlias(*shortest_path_function, column_alias);
			}
			original_ref.column_list.erase(original_ref.column_list.begin() + static_cast<int64_t>(idx_i));
			original_ref.column_list.insert(original_ref.column_list.begin() + static_cast<int64_t>(idx_i),
			                                std::move(shortest_path_function));
		} else if (parsed_ref->FunctionName() == "path_length") {
			auto shortest_path_function = CreatePathFindingFunction(subpath.path_list, pg_table, subpath.path_variable,
			                                                        final_select_node, conditions);
			auto path_len_children = vector<unique_ptr<ParsedExpression>>();
			path_len_children.push_back(std::move(shortest_path_function));
			auto path_len = make_uniq<FunctionExpression>("len", std::move(path_len_children));
			auto constant_two = ConstantExpression::Integer(2);
			vector<unique_ptr<ParsedExpression>> div_children;
			div_children.push_back(std::move(path_len));
			div_children.push_back(std::move(constant_two));
			auto path_length_function = make_uniq<FunctionExpression>("//", std::move(div_children));
			SetExpressionAlias(*path_length_function,
			                   column_alias.empty() ? "path_length(" + subpath.path_variable + ")" : column_alias);
			original_ref.column_list.erase(original_ref.column_list.begin() + static_cast<int64_t>(idx_i));
			original_ref.column_list.insert(original_ref.column_list.begin() + static_cast<int64_t>(idx_i),
			                                std::move(path_length_function));
		} else if (parsed_ref->FunctionName() == "vertices" || parsed_ref->FunctionName() == "edges") {
			auto list_slice_children = vector<unique_ptr<ParsedExpression>>();
			auto shortest_path_function = CreatePathFindingFunction(subpath.path_list, pg_table, subpath.path_variable,
			                                                        final_select_node, conditions);
			list_slice_children.push_back(std::move(shortest_path_function));

			if (parsed_ref->FunctionName() == "vertices") {
				list_slice_children.push_back(ConstantExpression::Integer(1));
			} else {
				list_slice_children.push_back(ConstantExpression::Integer(2));
			}
			auto slice_end = ConstantExpression::Integer(-1);
			auto slice_step = ConstantExpression::Integer(2);

			list_slice_children.push_back(std::move(slice_end));
			list_slice_children.push_back(std::move(slice_step));
			auto list_slice = make_uniq<FunctionExpression>("list_slice", std::move(list_slice_children));
			if (parsed_ref->FunctionName() == "vertices") {
				SetExpressionAlias(*list_slice,
				                   column_alias.empty() ? "vertices(" + subpath.path_variable + ")" : column_alias);
			} else {
				SetExpressionAlias(*list_slice,
				                   column_alias.empty() ? "edges(" + subpath.path_variable + ")" : column_alias);
			}
			original_ref.column_list.erase(original_ref.column_list.begin() + static_cast<int64_t>(idx_i));
			original_ref.column_list.insert(original_ref.column_list.begin() + static_cast<int64_t>(idx_i),
			                                std::move(list_slice));
		}
	}
}

void PGQMatchFunction::ProcessPathList(vector<unique_ptr<PathReference>> &path_list,
                                       vector<unique_ptr<ParsedExpression>> &conditions,
                                       unique_ptr<SelectNode> &final_select_node,
                                       case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
                                       CreatePropertyGraphInfo &pg_table, int32_t &extra_alias_counter,
                                       MatchExpression &original_ref) {
	PathElement *previous_vertex_element = GetPathElement(path_list[0]);
	if (!previous_vertex_element) {
		const auto previous_vertex_subpath = reinterpret_cast<SubPath *>(path_list[0].get());
		if (previous_vertex_subpath->where_clause) {
			conditions.push_back(std::move(previous_vertex_subpath->where_clause));
		}
		if (!previous_vertex_subpath->path_variable.empty() && previous_vertex_subpath->path_list.size() > 1) {
			CheckNamedSubpath(*previous_vertex_subpath, original_ref, pg_table, final_select_node, conditions);
		}
		if (previous_vertex_subpath->path_list.size() == 1) {
			previous_vertex_element = GetPathElement(previous_vertex_subpath->path_list[0]);
		} else {
			// Add the shortest path if the name is found in the column_list
			ProcessPathList(previous_vertex_subpath->path_list, conditions, final_select_node, alias_map, pg_table,
			                extra_alias_counter, original_ref);
			return;
		}
	}
	auto previous_vertex_table = FindGraphTable(previous_vertex_element->label, pg_table);
	CheckInheritance(previous_vertex_table, previous_vertex_element, conditions);
	alias_map[previous_vertex_element->variable_binding] = previous_vertex_table;

	for (idx_t idx_j = 1; idx_j < path_list.size(); idx_j = idx_j + 2) {
		PathElement *next_vertex_element = GetPathElement(path_list[idx_j + 1]);
		if (!next_vertex_element) {
			auto next_vertex_subpath = reinterpret_cast<SubPath *>(path_list[idx_j + 1].get());
			if (next_vertex_subpath->path_list.size() > 1) {
				throw NotImplementedException("Recursive patterns are not yet supported.");
			}
			if (next_vertex_subpath->where_clause) {
				conditions.push_back(std::move(next_vertex_subpath->where_clause));
			}
			next_vertex_element = GetPathElement(next_vertex_subpath->path_list[0]);
		}
		if (next_vertex_element->match_type != PGQMatchType::MATCH_VERTEX ||
		    previous_vertex_element->match_type != PGQMatchType::MATCH_VERTEX) {
			throw BinderException("Vertex and edge patterns must be alternated.");
		}
		auto next_vertex_table = FindGraphTable(next_vertex_element->label, pg_table);
		CheckInheritance(next_vertex_table, next_vertex_element, conditions);
		alias_map[next_vertex_element->variable_binding] = next_vertex_table;

		PathElement *edge_element = GetPathElement(path_list[idx_j]);
		if (!edge_element) {
			// We are dealing with a subpath
			auto edge_subpath = reinterpret_cast<SubPath *>(path_list[idx_j].get());
			if (edge_subpath->where_clause) {
				conditions.push_back(std::move(edge_subpath->where_clause));
			}
			if (edge_subpath->path_list.size() > 1) {
				throw NotImplementedException("Subpath on an edge is not yet supported.");
			}
			edge_element = GetPathElement(edge_subpath->path_list[0]);
			auto edge_table = FindGraphTable(edge_element->label, pg_table);

			if (PGQNeedsPathFinding(*edge_subpath)) {
				// Add the path-finding
				AddPathFinding(final_select_node, conditions, previous_vertex_element->variable_binding,
				               edge_element->variable_binding, next_vertex_element->variable_binding, edge_table,
				               pg_table, edge_subpath, edge_element->match_type);
			} else {
				AddEdgeJoins(edge_table, previous_vertex_table, next_vertex_table, edge_element->match_type,
				             edge_element->variable_binding, previous_vertex_element->variable_binding,
				             next_vertex_element->variable_binding, conditions, alias_map, extra_alias_counter,
				             final_select_node->from_table);
			}
		} else {
			// The edge element is a path element without WHERE or path-finding.
			auto edge_table = FindGraphTable(edge_element->label, pg_table);
			CheckInheritance(edge_table, edge_element, conditions);
			// check aliases
			AddEdgeJoins(edge_table, previous_vertex_table, next_vertex_table, edge_element->match_type,
			             edge_element->variable_binding, previous_vertex_element->variable_binding,
			             next_vertex_element->variable_binding, conditions, alias_map, extra_alias_counter,
			             final_select_node->from_table);
			// Check the edge type
			// If (a)-[b]->(c) 	-> 	b.src = a.id AND b.dst = c.id
			// If (a)<-[b]-(c) 	-> 	b.dst = a.id AND b.src = c.id
			// If (a)-[b]-(c)  	-> 	(b.src = a.id AND b.dst = c.id)
			//              FROM (src, dst, * from b UNION ALL dst, src, * from b)
			// If (a)<-[b]->(c)	->  (b.src = a.id AND b.dst = c.id) AND
			//						(b.dst = a.id AND b.src
			//= c.id)
		}
		previous_vertex_element = next_vertex_element;
		previous_vertex_table = next_vertex_table;
	}
}

void PGQMatchFunction::PopulateGraphTableAliasMap(
    const CreatePropertyGraphInfo &pg_table, const unique_ptr<PathReference> &path_reference,
    case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_to_vertex_and_edge_tables) {
	PathElement *path_elem = GetPathElement(path_reference);

	// Populate binding from PathElement.
	if (path_elem != nullptr) {
		auto iter = pg_table.label_map.find(path_elem->label);
		if (iter == pg_table.label_map.end()) {
			throw BinderException("The label %s is not registered in property graph %s", path_elem->label,
			                      pg_table.property_graph_name);
		}
		alias_to_vertex_and_edge_tables[path_elem->variable_binding] = iter->second;
		return;
	}

	// Recursively populate binding from SubPath.
	SubPath *sub_path = GetSubPath(path_reference);
	D_ASSERT(sub_path != nullptr);
	const auto &path_list = sub_path->path_list;
	for (const auto &cur_path : path_list) {
		PopulateGraphTableAliasMap(pg_table, cur_path, alias_to_vertex_and_edge_tables);
	}
}

case_insensitive_map_t<shared_ptr<PropertyGraphTable>>
PGQMatchFunction::PopulateGraphTableAliasMap(const CreatePropertyGraphInfo &pg_table,
                                             const MatchExpression &match_expr) {
	case_insensitive_map_t<shared_ptr<PropertyGraphTable>> alias_to_vertex_and_edge_tables;
	for (idx_t idx_i = 0; idx_i < match_expr.path_patterns.size(); idx_i++) {
		const auto &path_list = match_expr.path_patterns[idx_i]->path_elements;
		for (const auto &cur_path : path_list) {
			PopulateGraphTableAliasMap(pg_table, cur_path, alias_to_vertex_and_edge_tables);
		}
	}
	return alias_to_vertex_and_edge_tables;
}

// Find the property [name] among the properties exported by [table]; the index refers to column_names/column_aliases.
static bool PGQFindProperty(const PropertyGraphTable &table, const string &name, idx_t &index) {
	// Tables created without an explicit PROPERTIES clause export every column under its own name.
	auto &exported = table.column_aliases.size() == table.column_names.size() ? table.column_aliases : table.column_names;
	for (idx_t i = 0; i < exported.size(); i++) {
		if (StringUtil::CIEquals(exported[i].GetIdentifierName(), name)) {
			index = i;
			return true;
		}
	}
	return false;
}

static string PGQExportedProperties(const PropertyGraphTable &table) {
	auto &exported = table.column_aliases.size() == table.column_names.size() ? table.column_aliases : table.column_names;
	if (exported.empty()) {
		return "no properties";
	}
	return "properties " + StringUtil::Join(IdentifiersToStrings(exported), ", ");
}

// 6.5 <property reference> (G090): element.property must name a property the element's table exports. A property
// exported under an alias (PROPERTIES (column AS property)) is rewritten to its column. Returns true when rewritten.
static bool PGQCheckPropertyReference(ColumnRefExpression &column_ref,
                                      const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map) {
	auto &names = column_ref.ColumnNamesMutable();
	if (names.back().GetIdentifierName() == "rowid") {
		// DuckDB binds rowid on every table; it is the element identity.
		return false;
	}
	auto reference = StringUtil::Join(IdentifiersToStrings(names), ".");
	if (names.size() == 1) {
		auto &name = names[0].GetIdentifierName();
		if (alias_map.find(name) != alias_map.end()) {
			return false;
		}
		idx_t index;
		for (auto &entry : alias_map) {
			if (PGQFindProperty(*entry.second, name, index)) {
				return false;
			}
		}
		throw BinderException("Property %s is never registered! No element of the pattern exports a property %s",
		                      reference, name);
	}
	auto &alias = names[0].GetIdentifierName();
	if (alias == "shortest_path_cte") {
		// Internal reference produced by the path-finding rewrite.
		return false;
	}
	auto entry = alias_map.find(alias);
	if (entry == alias_map.end()) {
		throw BinderException("Property %s is never registered! %s is not an element variable of the pattern; "
		                      "property references take the form element.property",
		                      reference, alias);
	}
	auto &table = *entry->second;
	auto &property = names[1].GetIdentifierName();
	idx_t index;
	if (!PGQFindProperty(table, property, index)) {
		throw BinderException("Property %s is never registered! Element %s (label %s) exports %s", reference, alias,
		                      table.main_label.GetIdentifierName(), PGQExportedProperties(table));
	}
	if (index < table.column_aliases.size() && table.column_aliases[index] != table.column_names[index]) {
		names[1] = table.column_names[index];
		return true;
	}
	return false;
}

static void PGQCheckPropertyReferences(ParsedExpression &expression,
                                       const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map) {
	if (expression.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		PGQCheckPropertyReference(expression.Cast<ColumnRefExpression>(), alias_map);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expression, [&](ParsedExpression &child) { PGQCheckPropertyReferences(child, alias_map); });
}

// Element-local WHERE clauses live on subpaths and are consumed by ProcessPathList, so they are checked beforehand.
static void PGQCheckSubpathPropertyReferences(vector<unique_ptr<PathReference>> &path_list,
                                              const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map) {
	for (auto &path_reference : path_list) {
		if (path_reference->path_reference_type != PGQPathReferenceType::SUBPATH) {
			continue;
		}
		auto &subpath = *reinterpret_cast<SubPath *>(path_reference.get());
		if (subpath.where_clause) {
			PGQCheckPropertyReferences(*subpath.where_clause, alias_map);
		}
		PGQCheckSubpathPropertyReferences(subpath.path_list, alias_map);
	}
}

void PGQMatchFunction::CheckColumnBinding(
    MatchExpression &ref, const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_to_vertex_and_edge_tables) {
	for (auto &expression : ref.column_list) {
		if (expression->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &column_ref = expression->Cast<ColumnRefExpression>();
			auto property = column_ref.ColumnNames().back();
			if (PGQCheckPropertyReference(column_ref, alias_to_vertex_and_edge_tables) &&
			    expression->GetAlias().GetIdentifierName().empty()) {
				// Keep the property name as the output column name after rewriting to the underlying column.
				expression->SetAlias(property);
			}
			continue;
		}
		PGQCheckPropertyReferences(*expression, alias_to_vertex_and_edge_tables);
	}
	if (ref.where_clause) {
		PGQCheckPropertyReferences(*ref.where_clause, alias_to_vertex_and_edge_tables);
	}
}

unique_ptr<TableRef> PGQMatchFunction::MatchBindReplace(ClientContext &context, TableFunctionBindInput &bind_input) {
	auto duckpgq_state = GetDuckPGQState(context);

	auto match_info = dynamic_cast<DuckPGQMatchInfo *>(bind_input.ref.bind_info.get());
	if (!match_info) {
		throw BinderException("duckpgq_match can only be used through GRAPH_TABLE");
	}
	// Rewrite a copy: the expansion consumes the expression, and a prepared statement can be bound again.
	auto match_expression = unique_ptr_cast<ParsedExpression, MatchExpression>(match_info->match->Copy());
	auto *ref = match_expression.get();
	QualifiedName graph_name(Identifier(ref->pg_catalog), Identifier(ref->pg_schema), Identifier(ref->pg_name));
	auto *pg_table = duckpgq_state->GetPropertyGraph(context, graph_name);

	vector<unique_ptr<ParsedExpression>> conditions;

	auto final_select_node = make_uniq<SelectNode>();
	case_insensitive_map_t<shared_ptr<PropertyGraphTable>> alias_map;

	for (auto &path_pattern : ref->path_patterns) {
		PGQResolveElementLabels(*path_pattern, *pg_table, conditions);
	}
	// Maps from graph element alias to table, including vertex and edge tables.
	auto alias_to_vertex_and_edge_tables = PopulateGraphTableAliasMap(*pg_table, *ref);
	for (auto &path_pattern : ref->path_patterns) {
		PGQCheckSubpathPropertyReferences(path_pattern->path_elements, alias_to_vertex_and_edge_tables);
	}

	int32_t extra_alias_counter = 0;
	for (idx_t idx_i = 0; idx_i < ref->path_patterns.size(); idx_i++) {
		auto &path_pattern = ref->path_patterns[idx_i];
		PGQCheckPathModeSupport(*path_pattern);
		// Check if the element is PathElement or a Subpath with potentially many
		// items
		ProcessPathList(path_pattern->path_elements, conditions, final_select_node, alias_map, *pg_table,
		                extra_alias_counter, *ref);
	}

	// Go through all aliases encountered
	for (auto &table_alias_entry : alias_map) {
		auto table_ref =
		    DuckPGQSQL::ParseFromTableRef(DuckPGQSQL::TableRef(*table_alias_entry.second, table_alias_entry.first));
		PGQAppendCrossJoin(final_select_node->from_table, std::move(table_ref));
	}

	for (auto &column : ref->column_list) {
		PGQNormalizeGraphElementRefs(column, alias_to_vertex_and_edge_tables);
	}
	if (ref->where_clause) {
		PGQNormalizeGraphElementRefs(ref->where_clause, alias_to_vertex_and_edge_tables);
	}
	CheckColumnBinding(*ref, alias_to_vertex_and_edge_tables);
	if (ref->where_clause) {
		conditions.push_back(std::move(ref->where_clause));
	}

	std::vector<unique_ptr<ParsedExpression>> final_column_list;

	for (auto &expression : ref->column_list) {
		unordered_set<string> named_subpaths;

		// Handle ColumnRefExpression.
		auto *column_ref = dynamic_cast<ColumnRefExpression *>(expression.get());
		if (column_ref != nullptr) {
			auto &column_names = column_ref->ColumnNames();
			if (named_subpaths.count(column_names[0].GetIdentifierName()) && column_names.size() == 1) {
				final_column_list.emplace_back(make_uniq<ColumnRefExpression>("path", column_names[0]));
			} else {
				final_column_list.push_back(std::move(expression));
			}
			continue;
		}

		// Handle FunctionExpression.
		auto *function_ref = dynamic_cast<FunctionExpression *>(expression.get());
		if (function_ref != nullptr) {
			if (function_ref->FunctionName() == "path_length") {
				if (function_ref->GetArgumentsMutable().empty()) {
					continue;
				}
				column_ref = dynamic_cast<ColumnRefExpression *>(
				    function_ref->GetArgumentsMutable()[0].GetExpressionMutable().get());
				if (column_ref == nullptr) {
					continue;
				}
				auto &column_names = column_ref->ColumnNames();
				if (named_subpaths.count(column_names[0].GetIdentifierName()) && column_names.size() == 1) {
					auto path_name = column_names[0].GetIdentifierName();
					final_column_list.emplace_back(DuckPGQSQL::ParseExpression(
					    "len(" + DuckPGQSQL::Column(string("path"), path_name) + ") // 2", "path_length_" + path_name,
					    "DuckPGQ MATCH path_length projection"));
				}
			} else {
				final_column_list.push_back(std::move(expression));
			}

			continue;
		}

		// Handle StarExpression.
		auto *star_expression = dynamic_cast<StarExpression *>(expression.get());
		if (star_expression != nullptr) {
			auto &relation_name = star_expression->RelationName();
			if (!relation_name.empty()) {
				auto tbl_iter = alias_to_vertex_and_edge_tables.find(relation_name.GetIdentifierName());
				if (tbl_iter == alias_to_vertex_and_edge_tables.end()) {
					continue;
				}
			}

			auto selected_col_exprs = relation_name.empty() ? GetColRefExprFromPg(alias_to_vertex_and_edge_tables)
			                                                : GetColRefExprFromPg(alias_to_vertex_and_edge_tables,
			                                                                      relation_name.GetIdentifierName());

			// Fallback to star expression if cannot figure out the columns to query.
			if (selected_col_exprs.empty()) {
				final_column_list.emplace_back(std::move(expression));
				continue;
			}

			final_column_list.reserve(final_column_list.size() + selected_col_exprs.size());
			for (auto &expr : selected_col_exprs) {
				final_column_list.emplace_back(std::move(expr));
			}
			continue;
		}

		// By default, directly handle expression without further processing.
		final_column_list.emplace_back(std::move(expression));
	}

	final_select_node->where_clause = CreateWhereClause(conditions);
	final_select_node->select_list = std::move(final_column_list);

	auto subquery = make_uniq<SelectStatement>();
	subquery->node = std::move(final_select_node);
	auto result = make_uniq<SubqueryRef>(std::move(subquery), PGQIdentifier(ref->alias));
	return std::move(result);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterMatchTableFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(PGQMatchFunction());
}

} // namespace duckdb
