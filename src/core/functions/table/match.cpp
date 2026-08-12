#include <duckpgq_extension.hpp>
#include "duckpgq/core/functions/table/match.hpp"

#include "duckpgq/core/utils/duckpgq_sql.hpp"
#include "duckpgq/core/option/duckpgq_option.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckpgq/parser/tableref/matchref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
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
#include "duckdb/storage/data_table.hpp"
#include <duckdb/common/enums/set_operation_type.hpp>
#include <duckpgq/core/functions/table.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckdb {

static constexpr const char *PATH_FINDING_EDGE_SRC = "pathfinding_edge_src";
static constexpr const char *PATH_FINDING_EDGE_DST = "pathfinding_edge_dst";
static constexpr const char *PATH_FINDING_RESULT_PREFIX = "__duckpgq_path_length_";
static constexpr const char *PATH_FINDING_PAIRS_ALIAS = "__duckpgq_path_pairs";

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

static optional_ptr<DuckTableEntry> GetDuckTableEntry(ClientContext &context, const PropertyGraphTable &table) {
	auto entry = Catalog::GetEntry<TableCatalogEntry>(context,
	                                                  QualifiedName(PGQIdentifier(table.catalog_name),
	                                                                PGQIdentifier(table.schema_name),
	                                                                PGQIdentifier(table.table_name)),
	                                                  OnEntryNotFound::RETURN_NULL);
	if (!entry || !entry->IsDuckTable()) {
		return nullptr;
	}
	return &entry->Cast<DuckTableEntry>();
}

static bool GetPathFindingStorageCounts(ClientContext &context, const PropertyGraphTable &edge_table,
                                        PGQMatchType edge_type, idx_t &vertex_count, idx_t &edge_count) {
	if (!edge_table.source_pg_table || !edge_table.destination_pg_table ||
	    !edge_table.source_pg_table->SameTableIdentity(*edge_table.destination_pg_table)) {
		return false;
	}
	auto vertex_entry = GetDuckTableEntry(context, *edge_table.source_pg_table);
	auto edge_entry = GetDuckTableEntry(context, edge_table);
	if (!vertex_entry || !edge_entry) {
		return false;
	}
	// Row IDs can contain gaps after deletes. The next row ID is the required CSR range.
	vertex_count = vertex_entry->GetStorage().GetNextRowId();
	edge_count = edge_entry->GetStorage().GetTotalRows();
	if (edge_type == PGQMatchType::MATCH_EDGE_ANY) {
		if (edge_count > NumericLimits<idx_t>::Maximum() / 2) {
			throw OutOfRangeException("Undirected path-finding edge count is too large");
		}
		edge_count *= 2;
	}
	return true;
}

static string PathFindingEndpointJoin(const string &edge_alias, const PropertyGraphTable &vertex_table,
                                      const string &vertex_alias, const vector<Identifier> &foreign_keys,
                                      const vector<Identifier> &primary_keys) {
	if (foreign_keys.size() != primary_keys.size()) {
		throw BinderException("Vertex columns and edge columns size mismatch");
	}
	std::ostringstream result;
	result << DuckPGQSQL::TableRef(vertex_table, vertex_alias) << " ON ";
	for (idx_t key_idx = 0; key_idx < foreign_keys.size(); key_idx++) {
		if (key_idx > 0) {
			result << " AND ";
		}
		result << DuckPGQSQL::Column(foreign_keys[key_idx], edge_alias) << " = "
		       << DuckPGQSQL::Column(primary_keys[key_idx], vertex_alias);
	}
	return result.str();
}

static string DirectedPathFindingEndpointsSQL(const PropertyGraphTable &edge_table) {
	const string edge_alias = "__duckpgq_edge";
	const string source_alias = "__duckpgq_source";
	const string destination_alias = "__duckpgq_destination";
	std::ostringstream query;
	query << "SELECT CAST(" << DuckPGQSQL::Column(string("rowid"), source_alias) << " AS BIGINT) AS "
	      << DuckPGQSQL::Identifier(string(PATH_FINDING_EDGE_SRC)) << ", CAST("
	      << DuckPGQSQL::Column(string("rowid"), destination_alias) << " AS BIGINT) AS "
	      << DuckPGQSQL::Identifier(string(PATH_FINDING_EDGE_DST)) << " FROM "
	      << DuckPGQSQL::TableRef(edge_table, edge_alias) << " INNER JOIN "
	      << PathFindingEndpointJoin(edge_alias, *edge_table.source_pg_table, source_alias, edge_table.source_fk,
	                                 edge_table.source_pk)
	      << " INNER JOIN "
	      << PathFindingEndpointJoin(edge_alias, *edge_table.destination_pg_table, destination_alias,
	                                 edge_table.destination_fk, edge_table.destination_pk);
	return query.str();
}

static unique_ptr<SubqueryRef> CreatePathFindingEndpointSubquery(const PropertyGraphTable &edge_table,
                                                                 PGQMatchType edge_type, const string &alias) {
	auto directed = DirectedPathFindingEndpointsSQL(edge_table);
	if (edge_type == PGQMatchType::MATCH_EDGE_RIGHT) {
		return DuckPGQSQL::ParseSubqueryRef(directed, alias, "DuckPGQ path-finding endpoint input");
	}
	if (edge_type != PGQMatchType::MATCH_EDGE_ANY) {
		return nullptr;
	}
	std::ostringstream query;
	query << "WITH __duckpgq_directed_endpoints AS MATERIALIZED (" << directed << ") SELECT "
	      << DuckPGQSQL::Identifier(string(PATH_FINDING_EDGE_SRC)) << ", "
	      << DuckPGQSQL::Identifier(string(PATH_FINDING_EDGE_DST))
	      << " FROM __duckpgq_directed_endpoints UNION ALL SELECT "
	      << DuckPGQSQL::Identifier(string(PATH_FINDING_EDGE_DST)) << " AS "
	      << DuckPGQSQL::Identifier(string(PATH_FINDING_EDGE_SRC)) << ", "
	      << DuckPGQSQL::Identifier(string(PATH_FINDING_EDGE_SRC)) << " AS "
	      << DuckPGQSQL::Identifier(string(PATH_FINDING_EDGE_DST)) << " FROM __duckpgq_directed_endpoints";
	return DuckPGQSQL::ParseSubqueryRef(query.str(), alias, "DuckPGQ undirected path-finding endpoint input");
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
	auto &field = children[1]->Cast<ConstantExpression>().GetValue();
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
                                      CreatePropertyGraphInfo &pg_table, SubPath *subpath, PGQMatchType edge_type,
                                      ClientContext &context, vector<PathFindingOperatorResult> &operator_results) {
	if (select_node->cte_map.map.find("shortest_path_cte") != select_node->cte_map.map.end()) {
		return;
	}

	if (GetPathFindingOption(context) && operator_results.empty() &&
	    (edge_type == PGQMatchType::MATCH_EDGE_RIGHT || edge_type == PGQMatchType::MATCH_EDGE_ANY)) {
		idx_t vertex_count;
		idx_t edge_count;
		if (GetPathFindingStorageCounts(context, *edge_table, edge_type, vertex_count, edge_count)) {
			auto result_index = operator_results.size();
			auto endpoint_alias = "__duckpgq_path_edges_" + std::to_string(result_index);
			auto result_alias = string(PATH_FINDING_RESULT_PREFIX) + std::to_string(result_index);
			auto endpoints = CreatePathFindingEndpointSubquery(*edge_table, edge_type, endpoint_alias);
			auto cache_key = pg_table.property_graph_name + "|" + edge_table->FullTableName() + "|" +
			                 (edge_type == PGQMatchType::MATCH_EDGE_ANY ? "undirected" : "directed");
			auto source_expression = DuckPGQSQL::ParseExpression(
			    "CAST(" + DuckPGQSQL::Column(string("rowid"), prev_binding) + " AS BIGINT)",
			    "__duckpgq_pair_src_" + std::to_string(result_index), "DuckPGQ path-finding source row ID");
			auto destination_expression = DuckPGQSQL::ParseExpression(
			    "CAST(" + DuckPGQSQL::Column(string("rowid"), next_binding) + " AS BIGINT)",
			    "__duckpgq_pair_dst_" + std::to_string(result_index), "DuckPGQ path-finding destination row ID");
			operator_results.push_back({std::move(source_expression), std::move(destination_expression),
			                            std::move(endpoints), std::move(endpoint_alias), std::move(result_alias),
			                            std::move(cache_key), vertex_count, edge_count, subpath->lower,
			                            subpath->upper});
			return;
		}
	}

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
			auto constant_two = make_uniq<ConstantExpression>(Value::INTEGER(2));
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
				list_slice_children.push_back(make_uniq<ConstantExpression>(Value::INTEGER(1)));
			} else {
				list_slice_children.push_back(make_uniq<ConstantExpression>(Value::INTEGER(2)));
			}
			auto slice_end = make_uniq<ConstantExpression>(Value::INTEGER(-1));
			auto slice_step = make_uniq<ConstantExpression>(Value::INTEGER(2));

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
                                       MatchExpression &original_ref, ClientContext &context,
                                       vector<PathFindingOperatorResult> &operator_results) {
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
			                extra_alias_counter, original_ref, context, operator_results);
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
				               pg_table, edge_subpath, edge_element->match_type, context, operator_results);
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

	auto match_index = bind_input.inputs[0].GetValue<int32_t>();
	auto *ref = dynamic_cast<MatchExpression *>(duckpgq_state->transform_expression[match_index].get());
	auto *pg_table = duckpgq_state->GetPropertyGraph(ref->pg_name);

	vector<unique_ptr<ParsedExpression>> conditions;
	vector<PathFindingOperatorResult> operator_results;

	auto final_select_node = make_uniq<SelectNode>();
	case_insensitive_map_t<shared_ptr<PropertyGraphTable>> alias_map;

	int32_t extra_alias_counter = 0;
	for (idx_t idx_i = 0; idx_i < ref->path_patterns.size(); idx_i++) {
		auto &path_pattern = ref->path_patterns[idx_i];
		PGQCheckPathModeSupport(*path_pattern);
		// Check if the element is PathElement or a Subpath with potentially many
		// items
		ProcessPathList(path_pattern->path_elements, conditions, final_select_node, alias_map, *pg_table,
		                extra_alias_counter, *ref, context, operator_results);
	}

	// Go through all aliases encountered
	for (auto &table_alias_entry : alias_map) {
		auto table_ref =
		    DuckPGQSQL::ParseFromTableRef(DuckPGQSQL::TableRef(*table_alias_entry.second, table_alias_entry.first));
		PGQAppendCrossJoin(final_select_node->from_table, std::move(table_ref));
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
	if (operator_results.empty()) {
		final_select_node->select_list = std::move(final_column_list);
		auto inner_query = make_uniq<SelectStatement>();
		inner_query->node = std::move(final_select_node);
		return make_uniq<SubqueryRef>(std::move(inner_query), PGQIdentifier(ref->alias));
	}

	vector<unique_ptr<ParsedExpression>> candidate_columns;
	for (auto &operator_result : operator_results) {
		candidate_columns.push_back(std::move(operator_result.source_expression));
		candidate_columns.push_back(std::move(operator_result.destination_expression));
	}
	for (auto &column : final_column_list) {
		candidate_columns.push_back(std::move(column));
	}
	final_select_node->select_list = std::move(candidate_columns);

	auto candidate_query = make_uniq<SelectStatement>();
	candidate_query->node = std::move(final_select_node);
	const string candidate_alias = "__duckpgq_candidate_pairs";
	auto &operator_result = operator_results[0];
	const string pair_source_alias = "__duckpgq_pair_src_0";
	const string pair_destination_alias = "__duckpgq_pair_dst_0";

	std::ostringstream operator_query_sql;
	operator_query_sql << "SELECT " << DuckPGQSQL::Identifier(candidate_alias) << ".*, iterativelengthoperator("
	                   << DuckPGQSQL::Column(pair_source_alias, candidate_alias) << ", "
	                   << DuckPGQSQL::Column(pair_destination_alias, candidate_alias) << ", struct_pack(src := "
	                   << DuckPGQSQL::Column(string(PATH_FINDING_EDGE_SRC), operator_result.endpoint_alias)
	                   << ", dst := "
	                   << DuckPGQSQL::Column(string(PATH_FINDING_EDGE_DST), operator_result.endpoint_alias) << "), "
	                   << operator_result.vertex_count << "::BIGINT, " << operator_result.edge_count << "::BIGINT, "
	                   << DuckPGQSQL::StringLiteral(operator_result.cache_key) << ") AS "
	                   << DuckPGQSQL::Identifier(operator_result.alias) << " FROM "
	                   << DuckPGQSQL::Identifier(candidate_alias) << ", "
	                   << DuckPGQSQL::Identifier(operator_result.endpoint_alias);
	auto operator_query = DuckPGQSQL::ParseSelect(operator_query_sql.str(), "DuckPGQ path-finding operator projection");
	auto &operator_select_node = operator_query->node->Cast<SelectNode>();
	auto operator_from = make_uniq<JoinRef>(JoinRefType::CROSS);
	operator_from->left = make_uniq<SubqueryRef>(std::move(candidate_query), PGQIdentifier(candidate_alias));
	operator_from->right = std::move(operator_result.endpoint_input);
	operator_select_node.from_table = std::move(operator_from);

	std::ostringstream outer_query_sql;
	outer_query_sql << "SELECT * EXCLUDE (" << DuckPGQSQL::Identifier(pair_source_alias) << ", "
	                << DuckPGQSQL::Identifier(pair_destination_alias) << ", ";
	for (idx_t result_idx = 0; result_idx < operator_results.size(); result_idx++) {
		if (result_idx > 0) {
			outer_query_sql << ", ";
		}
		outer_query_sql << DuckPGQSQL::Identifier(operator_results[result_idx].alias);
	}
	outer_query_sql << ") FROM " << DuckPGQSQL::Identifier(string(PATH_FINDING_PAIRS_ALIAS)) << " WHERE ";
	for (idx_t result_idx = 0; result_idx < operator_results.size(); result_idx++) {
		if (result_idx > 0) {
			outer_query_sql << " AND ";
		}
		auto &operator_result = operator_results[result_idx];
		outer_query_sql << DuckPGQSQL::Column(operator_result.alias, string(PATH_FINDING_PAIRS_ALIAS));
		if (operator_result.upper == NumericLimits<int64_t>::Maximum()) {
			outer_query_sql << " >= " << operator_result.lower;
		} else {
			outer_query_sql << " BETWEEN " << operator_result.lower << " AND " << operator_result.upper;
		}
	}
	auto outer_query = DuckPGQSQL::ParseSelect(outer_query_sql.str(), "DuckPGQ path-finding result filter");
	auto &outer_select_node = outer_query->node->Cast<SelectNode>();
	outer_select_node.from_table =
	    make_uniq<SubqueryRef>(std::move(operator_query), PGQIdentifier(string(PATH_FINDING_PAIRS_ALIAS)));
	auto result = make_uniq<SubqueryRef>(std::move(outer_query), PGQIdentifier(ref->alias));
	return std::move(result);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterMatchTableFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(PGQMatchFunction());
}

} // namespace duckdb
