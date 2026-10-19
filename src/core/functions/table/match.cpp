#include <duckpgq_extension.hpp>
#include "duckpgq/core/functions/table/match.hpp"
#include "duckpgq/core/parser/duckpgq_grammar.hpp"

#include "duckpgq/core/utils/duckpgq_sql.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
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
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

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

// Get fully-qualified column names for the property graph [tbl], and insert
// into set [col_names].
void PopulateFullyQualifiedColName(const vector<shared_ptr<PropertyGraphTable>> &tbls,
                                   const case_insensitive_map_t<vector<string>> &tbl_name_to_aliases,
                                   case_insensitive_set_t &col_names) {
	for (const auto &cur_tbl : tbls) {
		for (const auto &cur_col : cur_tbl->column_names) {
			// It's legal to query by `<col>` instead of `<table>.<col>`.
			col_names.insert(cur_col.GetIdentifierName());

			const auto &tbl_name = cur_tbl->table_name.GetIdentifierName();
			auto iter = tbl_name_to_aliases.find(tbl_name);
			// Prefer to use table alias specified in the statement, otherwise use
			// table name.
			if (iter == tbl_name_to_aliases.end()) {
				col_names.insert(StringUtil::Format("%s.%s", tbl_name, cur_col.GetIdentifierName()));
			} else {
				const auto &all_aliases = iter->second;
				for (const auto &cur_alias : all_aliases) {
					col_names.insert(StringUtil::Format("%s.%s", cur_alias, cur_col.GetIdentifierName()));
				}
			}
		}
	}
}

// Get fully-qualified column names from property graph.
case_insensitive_set_t
GetFullyQualifiedColFromPg(const CreatePropertyGraphInfo &pg,
                           const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map) {
	case_insensitive_map_t<vector<string>> relation_name_to_aliases;
	for (const auto &entry : alias_map) {
		relation_name_to_aliases[entry.second->table_name.GetIdentifierName()].emplace_back(entry.first);
	}

	case_insensitive_set_t col_names;
	PopulateFullyQualifiedColName(pg.vertex_tables, relation_name_to_aliases, col_names);
	PopulateFullyQualifiedColName(pg.edge_tables, relation_name_to_aliases, col_names);
	return col_names;
}

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
	for (const auto &cur_col : tbl->column_names) {
		registered_col_names.emplace_back(PGQColumnRef(alias, cur_col, true));
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
		for (const auto &cur_col : tbl->column_names) {
			registered_col_names.emplace_back(PGQColumnRef(alias, cur_col, true));
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

void PGQMatchFunction::RecordVertexJoin(VertexJoinMap &vertex_joins, const string &vertex_alias,
                                        const vector<Identifier> &vertex_keys, const string &edge_alias,
                                        const vector<Identifier> &edge_keys,
                                        const shared_ptr<PropertyGraphTable> &edge_table,
                                        const shared_ptr<PropertyGraphTable> &referenced_vertex_table,
                                        vector<vector<Identifier>> backing_keys) {
	if (vertex_keys.size() != edge_keys.size()) {
		throw BinderException("Vertex columns and edge columns size mismatch");
	}
	VertexEdgeKeyReference reference;
	reference.edge_alias = edge_alias;
	reference.edge_table = edge_table;
	reference.referenced_vertex_table = referenced_vertex_table;
	reference.edge_keys = edge_keys;
	reference.vertex_keys = vertex_keys;
	if (backing_keys.empty()) {
		backing_keys.push_back(edge_keys);
	}
	reference.backing_keys = std::move(backing_keys);
	vertex_joins.AddReference(vertex_alias, std::move(reference));
}

void PGQMatchFunction::EdgeTypeAny(const shared_ptr<PropertyGraphTable> &edge_table, const string &edge_binding,
                                   const string &prev_binding, const string &next_binding, VertexJoinMap &vertex_joins,
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
	// The subquery exposes the source key under the source column name and the destination key under the destination
	// column name, each fed by both underlying columns.
	vector<vector<Identifier>> backing_keys {edge_table->source_fk, edge_table->destination_fk};
	// (a) src.key = edge.src
	RecordVertexJoin(vertex_joins, prev_binding, edge_table->source_pk, edge_binding, edge_table->source_fk, edge_table,
	                 edge_table->source_pg_table, backing_keys);
	// (b) dst.key = edge.dst
	RecordVertexJoin(vertex_joins, next_binding, edge_table->destination_pk, edge_binding, edge_table->destination_fk,
	                 edge_table, edge_table->destination_pg_table, backing_keys);
}

void PGQMatchFunction::EdgeTypeLeft(const shared_ptr<PropertyGraphTable> &edge_table, const Identifier &next_table_name,
                                    const Identifier &prev_table_name, const string &edge_binding,
                                    const string &prev_binding, const string &next_binding,
                                    VertexJoinMap &vertex_joins) {
	CheckEdgeTableConstraints(next_table_name, prev_table_name, edge_table);
	RecordVertexJoin(vertex_joins, next_binding, edge_table->source_pk, edge_binding, edge_table->source_fk, edge_table,
	                 edge_table->source_pg_table);
	RecordVertexJoin(vertex_joins, prev_binding, edge_table->destination_pk, edge_binding, edge_table->destination_fk,
	                 edge_table, edge_table->destination_pg_table);
}

void PGQMatchFunction::EdgeTypeRight(const shared_ptr<PropertyGraphTable> &edge_table,
                                     const Identifier &next_table_name, const Identifier &prev_table_name,
                                     const string &edge_binding, const string &prev_binding, const string &next_binding,
                                     VertexJoinMap &vertex_joins) {
	CheckEdgeTableConstraints(prev_table_name, next_table_name, edge_table);
	RecordVertexJoin(vertex_joins, prev_binding, edge_table->source_pk, edge_binding, edge_table->source_fk, edge_table,
	                 edge_table->source_pg_table);
	RecordVertexJoin(vertex_joins, next_binding, edge_table->destination_pk, edge_binding, edge_table->destination_fk,
	                 edge_table, edge_table->destination_pg_table);
}

void PGQMatchFunction::EdgeTypeLeftRight(const shared_ptr<PropertyGraphTable> &edge_table, const string &edge_binding,
                                         const string &prev_binding, const string &next_binding,
                                         VertexJoinMap &vertex_joins,
                                         case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
                                         int32_t &extra_alias_counter) {
	// One copy of the edge table per direction: edge_binding points from next to prev ...
	RecordVertexJoin(vertex_joins, next_binding, edge_table->source_pk, edge_binding, edge_table->source_fk, edge_table,
	                 edge_table->source_pg_table);
	RecordVertexJoin(vertex_joins, prev_binding, edge_table->destination_pk, edge_binding, edge_table->destination_fk,
	                 edge_table, edge_table->destination_pg_table);

	const auto additional_edge_alias = edge_binding + std::to_string(extra_alias_counter);
	extra_alias_counter++;

	alias_map[additional_edge_alias] = edge_table;

	// ... and the additional alias from prev to next.
	RecordVertexJoin(vertex_joins, prev_binding, edge_table->source_pk, additional_edge_alias, edge_table->source_fk,
	                 edge_table, edge_table->source_pg_table);
	RecordVertexJoin(vertex_joins, next_binding, edge_table->destination_pk, additional_edge_alias,
	                 edge_table->destination_fk, edge_table, edge_table->destination_pg_table);
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

unique_ptr<CommonTableExpressionInfo>
PGQMatchFunction::GenerateShortestPathCTE(CreatePropertyGraphInfo &pg_table, SubPath *edge_subpath,
                                          PathElement *previous_vertex_element, PathElement *next_vertex_element,
                                          vector<unique_ptr<ParsedExpression>> &path_finding_conditions) {
	auto edge_element = GetPathElement(edge_subpath->path_list[0]);
	auto edge_table = FindGraphTable(edge_element->label, pg_table);

	path_finding_conditions.push_back(AddPathQuantifierCondition(
	    previous_vertex_element->variable_binding, next_vertex_element->variable_binding, edge_table, edge_subpath));

	std::ostringstream query;
	query << "SELECT shortestpath(0, ("
	      << DuckPGQSQLCountTable(*edge_table->source_pg_table, previous_vertex_element->variable_binding,
	                              edge_table->source_pk[0])
	      << "), " << DuckPGQSQL::Column(string("rowid"), previous_vertex_element->variable_binding) << ", "
	      << DuckPGQSQL::Column(string("rowid"), next_vertex_element->variable_binding) << ") AS path, "
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
			if (edge_subpath->upper > 1) {
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
					if (edge_element->match_type == PGQMatchType::MATCH_EDGE_RIGHT) {
						final_select_node->cte_map.map[Identifier("cte1")] = CreateDirectedCSRCTE(
						    FindGraphTable(edge_element->label, pg_table), previous_vertex_element->variable_binding,
						    edge_element->variable_binding, next_vertex_element->variable_binding);
					} else if (edge_element->match_type == PGQMatchType::MATCH_EDGE_ANY) {
						final_select_node->cte_map.map[Identifier("cte1")] =
						    CreateUndirectedCSRCTE(FindGraphTable(edge_element->label, pg_table), final_select_node);
					} else {
						throw NotImplementedException("Cannot do shortest path for edge type %s",
						                              edge_element->match_type == PGQMatchType::MATCH_EDGE_LEFT
						                                  ? "MATCH_EDGE_LEFT"
						                                  : "MATCH_EDGE_LEFT_RIGHT");
					}
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
                                    VertexJoinMap &vertex_joins,
                                    case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
                                    int32_t &extra_alias_counter, unique_ptr<TableRef> &from_clause) {
	if (edge_type != PGQMatchType::MATCH_EDGE_ANY) {
		alias_map[edge_binding] = edge_table;
	}
	switch (edge_type) {
	case PGQMatchType::MATCH_EDGE_ANY: {
		EdgeTypeAny(edge_table, edge_binding, prev_binding, next_binding, vertex_joins, from_clause);
		break;
	}
	case PGQMatchType::MATCH_EDGE_LEFT:
		EdgeTypeLeft(edge_table, next_vertex_table->table_name, previous_vertex_table->table_name, edge_binding,
		             prev_binding, next_binding, vertex_joins);
		break;
	case PGQMatchType::MATCH_EDGE_RIGHT:
		EdgeTypeRight(edge_table, next_vertex_table->table_name, previous_vertex_table->table_name, edge_binding,
		              prev_binding, next_binding, vertex_joins);
		break;
	case PGQMatchType::MATCH_EDGE_LEFT_RIGHT: {
		EdgeTypeLeftRight(edge_table, edge_binding, prev_binding, next_binding, vertex_joins, alias_map,
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
                                      CreatePropertyGraphInfo &pg_table, SubPath *subpath, PGQMatchType edge_type,
                                      VertexJoinMap &vertex_joins) {
	// Path-finding needs the endpoint tables for their row ids and the CSR; never eliminate them.
	vertex_joins.MarkRequired(prev_binding);
	vertex_joins.MarkRequired(next_binding);
	//! START
	//! FROM (SELECT count(cte1.temp) * 0 as temp from cte1) __x
	if (select_node->cte_map.map.find("cte1") == select_node->cte_map.map.end()) {
		if (edge_type == PGQMatchType::MATCH_EDGE_RIGHT) {
			select_node->cte_map.map["cte1"] =
			    CreateDirectedCSRCTE(edge_table, prev_binding, edge_binding, next_binding);
		} else if (edge_type == PGQMatchType::MATCH_EDGE_ANY) {
			select_node->cte_map.map["cte1"] = CreateUndirectedCSRCTE(edge_table, select_node);
		} else {
			throw NotImplementedException("Cannot do shortest path for edge type %s",
			                              edge_type == PGQMatchType::MATCH_EDGE_LEFT ? "MATCH_EDGE_LEFT"
			                                                                         : "MATCH_EDGE_LEFT_RIGHT");
		}
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
	conditions.push_back(AddPathQuantifierCondition(prev_binding, next_binding, edge_table, subpath));
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
                                       MatchExpression &original_ref, VertexJoinMap &vertex_joins) {
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
			                extra_alias_counter, original_ref, vertex_joins);
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

			if (edge_subpath->upper > 1) {
				// Add the path-finding
				AddPathFinding(final_select_node, conditions, previous_vertex_element->variable_binding,
				               edge_element->variable_binding, next_vertex_element->variable_binding, edge_table,
				               pg_table, edge_subpath, edge_element->match_type, vertex_joins);
			} else {
				AddEdgeJoins(edge_table, previous_vertex_table, next_vertex_table, edge_element->match_type,
				             edge_element->variable_binding, previous_vertex_element->variable_binding,
				             next_vertex_element->variable_binding, vertex_joins, alias_map, extra_alias_counter,
				             final_select_node->from_table);
			}
		} else {
			// The edge element is a path element without WHERE or path-finding.
			auto edge_table = FindGraphTable(edge_element->label, pg_table);
			CheckInheritance(edge_table, edge_element, conditions);
			// check aliases
			AddEdgeJoins(edge_table, previous_vertex_table, next_vertex_table, edge_element->match_type,
			             edge_element->variable_binding, previous_vertex_element->variable_binding,
			             next_vertex_element->variable_binding, vertex_joins, alias_map, extra_alias_counter,
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

void PGQMatchFunction::CheckColumnBinding(
    const CreatePropertyGraphInfo &pg_table, const MatchExpression &ref,
    const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_to_vertex_and_edge_tables) {
	// All fully-qualified column names for vertex tables and edge tables.
	const auto all_fq_col_names = GetFullyQualifiedColFromPg(pg_table, alias_to_vertex_and_edge_tables);

	for (auto &expression : ref.column_list) {
		// TODO(hjiang): `ColumnRefExpression` alone is not enough, we could have
		// more complicated expression.
		//
		// See issue for reference:
		// https://github.com/cwida/duckpgq-extension/issues/198
		auto *column_ref = dynamic_cast<ColumnRefExpression *>(expression.get());
		if (column_ref == nullptr) {
			continue;
		}
		auto column_names = IdentifiersToStrings(column_ref->ColumnNames());
		// 'shortest_path_cte' is a special table populated by pgq.
		if (column_names[0] == "shortest_path_cte") {
			continue;
		}
		// 'rowid' is a column duckdb binds automatically.
		if (column_names.back() == "rowid") {
			continue;
		}
		if (column_names.size() == 1) {
			bool single_alias = false;
			for (const auto &alias : alias_to_vertex_and_edge_tables) {
				if (alias.first == column_names[0]) {
					single_alias = true;
					break;
				}
			}
			if (single_alias) {
				continue;
			}
		}
		const auto cur_fq_col_name = StringUtil::Join(column_names, /*separator=*/".");
		if (all_fq_col_names.find(cur_fq_col_name) == all_fq_col_names.end()) {
			throw BinderException("Property %s is never registered!", cur_fq_col_name);
		}
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
	VertexJoinMap vertex_joins;

	int32_t extra_alias_counter = 0;
	for (idx_t idx_i = 0; idx_i < ref->path_patterns.size(); idx_i++) {
		auto &path_pattern = ref->path_patterns[idx_i];
		PGQCheckPathModeSupport(*path_pattern);
		// Check if the element is PathElement or a Subpath with potentially many
		// items
		ProcessPathList(path_pattern->path_elements, conditions, final_select_node, alias_map, *pg_table,
		                extra_alias_counter, *ref, vertex_joins);
	}

	// Maps from graph element alias to table, including vertex and edge tables.
	auto alias_to_vertex_and_edge_tables = PopulateGraphTableAliasMap(*pg_table, *ref);

	for (auto &column : ref->column_list) {
		PGQNormalizeGraphElementRefs(column, alias_to_vertex_and_edge_tables);
	}
	if (ref->where_clause) {
		PGQNormalizeGraphElementRefs(ref->where_clause, alias_to_vertex_and_edge_tables);
		conditions.push_back(std::move(ref->where_clause));
	}

	CheckColumnBinding(*pg_table, *ref, alias_to_vertex_and_edge_tables);

	// Vertex-join elimination: at this point every expression of the translated query is either in the column list
	// or in conditions, so the planner can see all references to each vertex variable.
	VertexEliminationPlanner vertex_elimination(context, vertex_joins, alias_map, pg_table->verified);
	vertex_elimination.Plan(ref->column_list, conditions);
	vertex_elimination.RewriteReferences(ref->column_list, conditions);
	vertex_elimination.EmitJoinConditions(conditions);

	// Go through all aliases encountered
	for (auto &table_alias_entry : alias_map) {
		if (vertex_elimination.IsEliminated(table_alias_entry.first)) {
			continue;
		}
		auto table_ref =
		    DuckPGQSQL::ParseFromTableRef(DuckPGQSQL::TableRef(*table_alias_entry.second, table_alias_entry.first));
		PGQAppendCrossJoin(final_select_node->from_table, std::move(table_ref));
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
// Vertex-join elimination
//------------------------------------------------------------------------------
VertexJoinInfo &VertexJoinMap::GetOrCreate(const string &vertex_alias) {
	auto entry = entries.find(vertex_alias);
	if (entry == entries.end()) {
		order.push_back(vertex_alias);
		entry = entries.emplace(vertex_alias, VertexJoinInfo()).first;
	}
	return entry->second;
}

void VertexJoinMap::AddReference(const string &vertex_alias, VertexEdgeKeyReference reference) {
	GetOrCreate(vertex_alias).edge_references.push_back(std::move(reference));
}

void VertexJoinMap::MarkRequired(const string &vertex_alias) {
	GetOrCreate(vertex_alias).required = true;
}

VertexEliminationPlanner::VertexEliminationPlanner(
    ClientContext &context_p, VertexJoinMap &vertex_joins_p,
    const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map_p, bool graph_verified_p)
    : context(context_p), vertex_joins(vertex_joins_p), alias_map(alias_map_p), graph_verified(graph_verified_p) {
	Value value;
	if (context.TryGetCurrentSetting(Identifier(SETTING_NAME), value)) {
		enabled = BooleanValue::Get(value);
	}
}

bool VertexEliminationPlanner::IsEliminated(const string &vertex_alias) const {
	return eliminated.find(vertex_alias) != eliminated.end();
}

void VertexEliminationPlanner::Plan(const vector<unique_ptr<ParsedExpression>> &column_list,
                                    const vector<unique_ptr<ParsedExpression>> &conditions) {
	if (!enabled || !graph_verified || vertex_joins.entries.empty()) {
		return;
	}
	for (auto &expression : column_list) {
		CollectReferences(*expression);
	}
	for (auto &expression : conditions) {
		CollectReferences(*expression);
	}
	for (auto &vertex_alias : vertex_joins.order) {
		EliminatedVertex plan;
		if (TryPlanVertex(vertex_alias, vertex_joins.entries[vertex_alias], plan)) {
			eliminated[vertex_alias] = std::move(plan);
		}
	}
}

void VertexEliminationPlanner::Disqualify(const string &vertex_alias) {
	disqualified.insert(vertex_alias);
}

void VertexEliminationPlanner::DisqualifyAll() {
	for (auto &vertex_alias : vertex_joins.order) {
		disqualified.insert(vertex_alias);
	}
}

bool VertexEliminationPlanner::IsVertexKey(const VertexJoinInfo &info, const Identifier &column) const {
	for (auto &reference : info.edge_references) {
		if (KeyPosition(reference.vertex_keys, column) != DConstants::INVALID_INDEX) {
			return true;
		}
	}
	return false;
}

bool VertexEliminationPlanner::TableHasColumn(const string &vertex_alias, const Identifier &column) {
	auto table_entry = alias_map.find(vertex_alias);
	if (table_entry == alias_map.end()) {
		return true;
	}
	auto table = LookupTable(*table_entry->second);
	// Without catalog information assume the column may belong to the table.
	return !table || table->ColumnExists(column);
}

void VertexEliminationPlanner::CollectReferences(const ParsedExpression &expression) {
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		auto &column_ref = expression.Cast<ColumnRefExpression>();
		auto &names = column_ref.ColumnNames();
		if (names.size() == 1) {
			auto &name = names[0].GetIdentifierName();
			if (vertex_joins.entries.find(name) != vertex_joins.entries.end()) {
				// The vertex itself, e.g. element_id(v) or v as a struct.
				Disqualify(name);
				return;
			}
			// An unqualified column may resolve to any vertex table that has a column of that name.
			for (auto &vertex_alias : vertex_joins.order) {
				if (TableHasColumn(vertex_alias, names[0])) {
					Disqualify(vertex_alias);
				}
			}
			return;
		}
		auto &vertex_alias = names[0].GetIdentifierName();
		auto entry = vertex_joins.entries.find(vertex_alias);
		if (entry == vertex_joins.entries.end()) {
			return;
		}
		if (names.size() != 2 || !IsVertexKey(entry->second, names[1])) {
			// A property, rowid, a struct field of a property, ...
			Disqualify(vertex_alias);
		}
		return;
	}
	case ExpressionClass::STAR: {
		auto &star = expression.Cast<StarExpression>();
		if (star.RelationName().empty()) {
			DisqualifyAll();
		} else {
			Disqualify(star.RelationName().GetIdentifierName());
		}
		// REPLACE / COLUMNS(...) expressions may reference other variables
		ParsedExpressionIterator::EnumerateChildren(expression,
		                                            [&](const ParsedExpression &child) { CollectReferences(child); });
		return;
	}
	case ExpressionClass::SUBQUERY:
	case ExpressionClass::LAMBDA:
		// A correlated subquery could reference anything, and a subquery alias or lambda parameter could shadow a
		// vertex variable, so references inside them cannot be classified.
		DisqualifyAll();
		return;
	default:
		ParsedExpressionIterator::EnumerateChildren(expression,
		                                            [&](const ParsedExpression &child) { CollectReferences(child); });
	}
}

optional_ptr<TableCatalogEntry> VertexEliminationPlanner::LookupTable(const PropertyGraphTable &table) {
	auto cache_key = DuckPGQSQL::QualifiedTableName(table);
	auto cached = table_cache.find(cache_key);
	if (cached != table_cache.end()) {
		return cached->second;
	}
	optional_ptr<TableCatalogEntry> result;
	try {
		result = GetPropertyGraphTableEntry(context, table);
	} catch (CatalogException &) {
		// Views and tables outside the catalog cannot be checked for constraints: keep the join.
		result = nullptr;
	} catch (BinderException &) {
		result = nullptr;
	}
	table_cache[cache_key] = result;
	return result;
}

bool VertexEliminationPlanner::SameKeySet(const vector<Identifier> &left, const vector<Identifier> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (KeyPosition(right, left[i]) == DConstants::INVALID_INDEX) {
			return false;
		}
		for (idx_t j = i + 1; j < left.size(); j++) {
			if (left[i] == left[j]) {
				return false;
			}
		}
	}
	return true;
}

idx_t VertexEliminationPlanner::KeyPosition(const vector<Identifier> &keys, const Identifier &key) {
	for (idx_t i = 0; i < keys.size(); i++) {
		if (keys[i] == key) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

bool VertexEliminationPlanner::HasUniqueConstraint(const TableCatalogEntry &table, const vector<Identifier> &keys) {
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		vector<Identifier> columns;
		if (unique.HasIndex()) {
			columns.push_back(table.GetColumn(unique.GetIndex()).Name());
		} else {
			columns = unique.GetColumnNames();
		}
		if (SameKeySet(columns, keys)) {
			return true;
		}
	}
	return false;
}

bool VertexEliminationPlanner::IsNotNull(const TableCatalogEntry &table, const Identifier &column) {
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::NOT_NULL) {
			continue;
		}
		auto &not_null = constraint->Cast<NotNullConstraint>();
		if (table.GetColumn(not_null.index).Name() == column) {
			return true;
		}
	}
	return false;
}

bool VertexEliminationPlanner::TryPlanVertex(const string &vertex_alias, const VertexJoinInfo &info,
                                             EliminatedVertex &result) {
	if (info.required || info.edge_references.empty() || disqualified.find(vertex_alias) != disqualified.end()) {
		return false;
	}
	auto table_entry = alias_map.find(vertex_alias);
	if (table_entry == alias_map.end() || !table_entry->second->is_vertex_table) {
		return false;
	}
	auto vertex_table = LookupTable(*table_entry->second);
	if (!vertex_table) {
		return false;
	}
	auto &representative = info.edge_references[0];
	auto &keys = representative.vertex_keys;
	// Every incident edge must reference this vertex table (undirected and bidirectional edges are not checked
	// against the labels when the pattern is translated; the join did that), join on the same vertex key, and the
	// key must be unique.
	for (auto &reference : info.edge_references) {
		if (!reference.referenced_vertex_table ||
		    !reference.referenced_vertex_table->SameTableIdentity(*table_entry->second)) {
			return false;
		}
		if (!SameKeySet(reference.vertex_keys, keys)) {
			return false;
		}
		for (auto &backing : reference.backing_keys) {
			if (backing.size() != reference.edge_keys.size()) {
				return false;
			}
		}
	}
	for (auto &key : keys) {
		if (!vertex_table->ColumnExists(key)) {
			return false;
		}
	}
	if (!HasUniqueConstraint(*vertex_table, keys)) {
		return false;
	}
	// Every edge column must exist in the catalog, for the type and NOT NULL checks below.
	for (auto &reference : info.edge_references) {
		auto edge_table = LookupTable(*reference.edge_table);
		if (!edge_table) {
			return false;
		}
		for (auto &backing : reference.backing_keys) {
			for (auto &column : backing) {
				if (!edge_table->ColumnExists(column)) {
					return false;
				}
			}
		}
	}
	// The first edge provides the replacement for the vertex key.
	result.representative_alias = representative.edge_alias;
	auto representative_table = LookupTable(*representative.edge_table);
	for (auto &key : keys) {
		auto position = KeyPosition(representative.vertex_keys, key);
		result.representative_column[key.GetIdentifierName()] = representative.edge_keys[position];
		auto &vertex_column = vertex_table->GetColumn(key);
		result.output_name[key.GetIdentifierName()] = vertex_column.Name();
		auto &vertex_type = vertex_column.Type();
		for (auto &backing : representative.backing_keys) {
			auto &column = representative_table->GetColumn(backing[position]);
			if (column.Type() != vertex_type) {
				result.cast_type[key.GetIdentifierName()] = vertex_type;
			}
			if (!IsNotNull(*representative_table, column.Name())) {
				result.nullable_keys.insert(key.GetIdentifierName());
			}
		}
	}
	return true;
}

void VertexEliminationPlanner::RewriteReferences(vector<unique_ptr<ParsedExpression>> &column_list,
                                                 vector<unique_ptr<ParsedExpression>> &conditions) {
	if (eliminated.empty()) {
		return;
	}
	for (auto &expression : column_list) {
		RewriteExpression(expression, true);
	}
	for (auto &expression : conditions) {
		RewriteExpression(expression, false);
	}
}

void VertexEliminationPlanner::RewriteExpression(unique_ptr<ParsedExpression> &expression, bool preserve_name) {
	if (expression->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
		ParsedExpressionIterator::EnumerateChildren(
		    *expression, [&](unique_ptr<ParsedExpression> &child) { RewriteExpression(child, false); });
		return;
	}
	auto &column_ref = expression->Cast<ColumnRefExpression>();
	auto &names = column_ref.ColumnNames();
	if (names.size() != 2) {
		return;
	}
	auto plan = eliminated.find(names[0].GetIdentifierName());
	if (plan == eliminated.end()) {
		return;
	}
	auto replacement_column = plan->second.representative_column.find(names[1].GetIdentifierName());
	if (replacement_column == plan->second.representative_column.end()) {
		throw InternalException("Vertex-join elimination: %s.%s is not a key of the eliminated vertex %s",
		                        names[0].GetIdentifierName(), names[1].GetIdentifierName(),
		                        names[0].GetIdentifierName());
	}
	auto &key_name = names[1].GetIdentifierName();
	unique_ptr<ParsedExpression> replacement =
	    PGQColumnRef(replacement_column->second, plan->second.representative_alias);
	auto cast_type = plan->second.cast_type.find(key_name);
	if (cast_type != plan->second.cast_type.end()) {
		replacement = make_uniq<CastExpression>(cast_type->second, std::move(replacement));
	}
	// Reproduce the result column name the vertex column would have had. DuckDB names a column reference after the
	// catalog column when it has no alias or an alias that only differs in case, so COLUMNS (v.ID) and
	// COLUMNS (v.id AS ID) both yield the catalog spelling. (An alias that differs only in case from the *edge*
	// column's name is still renamed by that rule; the join-based translation did not have that collision.)
	auto &catalog_name = plan->second.output_name[key_name];
	if (expression->HasAlias()) {
		replacement->SetAlias(expression->GetAlias() == catalog_name ? catalog_name : expression->GetAlias());
	} else if (preserve_name) {
		replacement->SetAlias(catalog_name);
	}
	expression = std::move(replacement);
}

void VertexEliminationPlanner::EmitJoinConditions(vector<unique_ptr<ParsedExpression>> &conditions) {
	for (auto &vertex_alias : vertex_joins.order) {
		auto &info = vertex_joins.entries[vertex_alias];
		auto plan = eliminated.find(vertex_alias);
		if (plan == eliminated.end()) {
			for (auto &reference : info.edge_references) {
				conditions.push_back(PGQMatchFunction::CreateMatchJoinExpression(
				    reference.vertex_keys, reference.edge_keys, vertex_alias, reference.edge_alias));
			}
			continue;
		}
		auto &representative = info.edge_references[0];
		for (auto &key : representative.vertex_keys) {
			auto &key_name = key.GetIdentifierName();
			auto representative_column = plan->second.representative_column[key_name];
			// A NULL edge key never joined with the vertex; keep dropping it.
			if (plan->second.nullable_keys.find(key_name) != plan->second.nullable_keys.end()) {
				conditions.push_back(
				    make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL,
				                                  PGQColumnRef(representative_column, representative.edge_alias)));
			}
			// The other edges meet the representative at the vertex key.
			for (idx_t i = 1; i < info.edge_references.size(); i++) {
				auto &reference = info.edge_references[i];
				auto position = KeyPosition(reference.vertex_keys, key);
				conditions.push_back(make_uniq<ComparisonExpression>(
				    ExpressionType::COMPARE_EQUAL, PGQColumnRef(reference.edge_keys[position], reference.edge_alias),
				    PGQColumnRef(representative_column, representative.edge_alias)));
			}
		}
	}
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterMatchTableFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(PGQMatchFunction());
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption(Identifier(VertexEliminationPlanner::SETTING_NAME),
	                          "Leave vertex tables out of the MATCH translation when the query only uses their key, "
	                          "joining the incident edge tables to each other instead",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));
}

} // namespace duckdb
