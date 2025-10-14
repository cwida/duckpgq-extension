#include <duckpgq_extension.hpp>
#include "duckpgq/core/functions/table/match.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/tableref/matchref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"

#include "duckdb/parser/query_node/set_operation_node.hpp"

#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

#include "duckdb/parser/property_graph_table.hpp"
#include "duckdb/parser/subpath_element.hpp"
#include <duckdb/common/enums/set_operation_type.hpp>
#include <duckpgq/core/functions/table.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckdb {

namespace {

// Get fully-qualified column names for the property graph [tbl], and insert
// into set [col_names].
void PopulateFullyQualifiedColName(const vector<shared_ptr<PropertyGraphTable>> &tbls,
                                   const case_insensitive_map_t<vector<string>> &tbl_name_to_aliases,
                                   case_insensitive_set_t &col_names) {
	for (const auto &cur_tbl : tbls) {
		for (const auto &cur_col : cur_tbl->column_names) {
			// It's legal to query by `<col>` instead of `<table>.<col>`.
			col_names.insert(cur_col);

			const string &tbl_name = cur_tbl->table_name;
			auto iter = tbl_name_to_aliases.find(tbl_name);
			// Prefer to use table alias specified in the statement, otherwise use
			// table name.
			if (iter == tbl_name_to_aliases.end()) {
				col_names.insert(StringUtil::Format("%s.%s", tbl_name, cur_col));
			} else {
				const auto &all_aliases = iter->second;
				for (const auto &cur_alias : all_aliases) {
					col_names.insert(StringUtil::Format("%s.%s", cur_alias, cur_col));
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
		relation_name_to_aliases[entry.second->table_name].emplace_back(entry.first);
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
		auto new_col_names = vector<string> {"", ""};
		new_col_names[0] = alias;
		new_col_names[1] = cur_col;
		registered_col_names.emplace_back(make_uniq<ColumnRefExpression>(std::move(new_col_names)));
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
			auto new_col_names = vector<string> {"", ""};
			new_col_names[0] = alias;
			new_col_names[1] = cur_col;
			registered_col_names.emplace_back(make_uniq<ColumnRefExpression>(std::move(new_col_names)));
		}
	}
	return registered_col_names;
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
	auto constant_expression_two = make_uniq<ConstantExpression>(Value::INTEGER(2));
	const auto itr = std::find(tableref->sub_labels.begin(), tableref->sub_labels.end(), element->label);

	const auto idx_of_label = std::distance(tableref->sub_labels.begin(), itr);
	auto constant_expression_idx_label =
	    make_uniq<ConstantExpression>(Value::INTEGER(static_cast<int32_t>(idx_of_label)));

	vector<unique_ptr<ParsedExpression>> power_of_children;
	power_of_children.push_back(std::move(constant_expression_two));
	power_of_children.push_back(std::move(constant_expression_idx_label));
	auto power_of_term = make_uniq<FunctionExpression>("power", std::move(power_of_children));
	auto bigint_cast = make_uniq<CastExpression>(LogicalType::INTEGER, std::move(power_of_term));
	auto subcategory_colref = make_uniq<ColumnRefExpression>(tableref->discriminator, element->variable_binding);
	vector<unique_ptr<ParsedExpression>> and_children;
	and_children.push_back(std::move(subcategory_colref));
	and_children.push_back(std::move(bigint_cast));

	auto and_expression = make_uniq<FunctionExpression>("&", std::move(and_children));

	auto constant_expression_idx_label_comparison =
	    make_uniq<ConstantExpression>(Value::INTEGER(static_cast<int32_t>(std::pow(2, idx_of_label))));

	auto subset_compare = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(and_expression),
	                                                      std::move(constant_expression_idx_label_comparison));
	conditions.push_back(std::move(subset_compare));
}

void PGQMatchFunction::CheckEdgeTableConstraints(const string &src_reference, const string &dst_reference,
                                                 const shared_ptr<PropertyGraphTable> &edge_table) {
	if (src_reference != edge_table->source_reference) {
		throw BinderException("Label %s is not registered as a source reference "
		                      "for edge pattern of table %s",
		                      src_reference, edge_table->table_name);
	}
	if (dst_reference != edge_table->destination_reference) {
		throw BinderException("Label %s is not registered as a destination "
		                      "reference for edge pattern of table %s",
		                      src_reference, edge_table->table_name);
	}
}

unique_ptr<ParsedExpression> PGQMatchFunction::CreateMatchJoinExpression(vector<string> vertex_keys,
                                                                         vector<string> edge_keys,
                                                                         const string &vertex_alias,
                                                                         const string &edge_alias) {
	vector<unique_ptr<ParsedExpression>> conditions;

	if (vertex_keys.size() != edge_keys.size()) {
		throw BinderException("Vertex columns and edge columns size mismatch");
	}
	for (idx_t i = 0; i < vertex_keys.size(); i++) {
		auto vertex_colref = make_uniq<ColumnRefExpression>(vertex_keys[i], vertex_alias);
		auto edge_colref = make_uniq<ColumnRefExpression>(edge_keys[i], edge_alias);
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
	//! BEGIN OF (SELECT count(cte1.temp) as temp * 0 from cte1) __x

	auto temp_cte_select_node = make_uniq<SelectNode>();

	auto cte_table_ref = make_uniq<BaseTableRef>();

	cte_table_ref->table_name = "cte1";
	temp_cte_select_node->from_table = std::move(cte_table_ref);
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ColumnRefExpression>("temp", "cte1"));

	auto count_function = make_uniq<FunctionExpression>("count", std::move(children));

	auto zero = make_uniq<ConstantExpression>(Value::INTEGER(0));

	vector<unique_ptr<ParsedExpression>> multiply_children;

	multiply_children.push_back(std::move(zero));
	multiply_children.push_back(std::move(count_function));
	auto multiply_function = make_uniq<FunctionExpression>("multiply", std::move(multiply_children));
	multiply_function->alias = "temp";
	temp_cte_select_node->select_list.push_back(std::move(multiply_function));
	auto temp_cte_select_statement = make_uniq<SelectStatement>();
	temp_cte_select_statement->node = std::move(temp_cte_select_node);

	auto temp_cte_select_subquery = make_uniq<SubqueryRef>(std::move(temp_cte_select_statement), "__x");
	//! END OF (SELECT count(cte1.temp) * 0 as temp from cte1) __x
	return temp_cte_select_subquery;
}

void PGQMatchFunction::EdgeTypeAny(const shared_ptr<PropertyGraphTable> &edge_table, const string &edge_binding,
                                   const string &prev_binding, const string &next_binding,
                                   vector<unique_ptr<ParsedExpression>> &conditions,
                                   unique_ptr<TableRef> &from_clause) {

	// START SELECT src, dst, * from edge_table
	auto src_dst_select_node = make_uniq<SelectNode>();

	auto edge_left_ref = edge_table->CreateBaseTableRef(edge_binding);
	src_dst_select_node->from_table = std::move(edge_left_ref);
	auto src_dst_children = vector<unique_ptr<ParsedExpression>>();
	src_dst_children.push_back(make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_binding));
	src_dst_children.push_back(make_uniq<ColumnRefExpression>(edge_table->destination_fk[0], edge_binding));
	src_dst_children.push_back(make_uniq<StarExpression>());

	src_dst_select_node->select_list = std::move(src_dst_children);
	// END SELECT src, dst, * from edge_table

	// START SELECT dst, src, * from edge_table
	auto dst_src_select_node = make_uniq<SelectNode>();

	auto edge_right_ref = edge_table->CreateBaseTableRef(edge_binding);
	auto dst_src_children = vector<unique_ptr<ParsedExpression>>();
	dst_src_select_node->from_table = std::move(edge_right_ref);

	dst_src_children.push_back(make_uniq<ColumnRefExpression>(edge_table->destination_fk[0], edge_binding));
	dst_src_children.push_back(make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_binding));
	dst_src_children.push_back(make_uniq<StarExpression>());

	dst_src_select_node->select_list = std::move(dst_src_children);
	// END SELECT dst, src, * from edge_table

	auto union_node = make_uniq<SetOperationNode>();
	union_node->setop_type = SetOperationType::UNION;
	union_node->setop_all = true;
	union_node->children.push_back(std::move(src_dst_select_node));
	union_node->children.push_back(std::move(dst_src_select_node));
	auto union_select = make_uniq<SelectStatement>();
	union_select->node = std::move(union_node);
	// (SELECT src, dst, * from edge_table UNION ALL SELECT dst, src, * from
	// edge_table)
	auto union_subquery = make_uniq<SubqueryRef>(std::move(union_select));
	union_subquery->alias = edge_binding;
	if (from_clause) {
		auto from_join = make_uniq<JoinRef>(JoinRefType::CROSS);
		from_join->left = std::move(from_clause);
		from_join->right = std::move(union_subquery);
		from_clause = std::move(from_join);
	} else {
		from_clause = std::move(union_subquery);
	}
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

void PGQMatchFunction::EdgeTypeLeft(const shared_ptr<PropertyGraphTable> &edge_table, const string &next_table_name,
                                    const string &prev_table_name, const string &edge_binding,
                                    const string &prev_binding, const string &next_binding,
                                    vector<unique_ptr<ParsedExpression>> &conditions) {
	CheckEdgeTableConstraints(next_table_name, prev_table_name, edge_table);
	conditions.push_back(
	    CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk, next_binding, edge_binding));
	conditions.push_back(
	    CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk, prev_binding, edge_binding));
}

void PGQMatchFunction::EdgeTypeRight(const shared_ptr<PropertyGraphTable> &edge_table, const string &next_table_name,
                                     const string &prev_table_name, const string &edge_binding,
                                     const string &prev_binding, const string &next_binding,
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
	auto cte_info = make_uniq<CommonTableExpressionInfo>();
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();

	auto edge_element = GetPathElement(edge_subpath->path_list[0]);
	auto edge_table = FindGraphTable(edge_element->label, pg_table);

	auto src_row_id = make_uniq<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
	auto dst_row_id = make_uniq<ColumnRefExpression>("rowid", next_vertex_element->variable_binding);
	auto csr_id = make_uniq<ConstantExpression>(Value::INTEGER(0));

	vector<unique_ptr<ParsedExpression>> pathfinding_children;
	pathfinding_children.push_back(std::move(csr_id));
	pathfinding_children.push_back(std::move(GetCountTable(
	    edge_table->source_pg_table, previous_vertex_element->variable_binding, edge_table->source_pk[0])));
	pathfinding_children.push_back(std::move(src_row_id));
	pathfinding_children.push_back(std::move(dst_row_id));

	auto shortest_path_function = make_uniq<FunctionExpression>("shortestpath", std::move(pathfinding_children));
	shortest_path_function->alias = "path";
	select_node->select_list.push_back(std::move(shortest_path_function));
	auto src_rowid_outer_select = make_uniq<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
	src_rowid_outer_select->alias = "src_rowid";
	select_node->select_list.push_back(std::move(src_rowid_outer_select));
	auto dst_rowid_outer_select = make_uniq<ColumnRefExpression>("rowid", next_vertex_element->variable_binding);
	dst_rowid_outer_select->alias = "dst_rowid";
	select_node->select_list.push_back(std::move(dst_rowid_outer_select));

	auto src_tableref = edge_table->source_pg_table->CreateBaseTableRef();
	src_tableref->alias = previous_vertex_element->variable_binding;
	auto dst_tableref = edge_table->destination_pg_table->CreateBaseTableRef();
	dst_tableref->alias = next_vertex_element->variable_binding;
	auto first_cross_join_ref = make_uniq<JoinRef>(JoinRefType::CROSS);
	first_cross_join_ref->left = std::move(src_tableref);
	first_cross_join_ref->right = std::move(dst_tableref);
	auto temp_cte_select_subquery = CreateCountCTESubquery();

	auto second_cross_join_ref = make_uniq<JoinRef>(JoinRefType::CROSS);
	second_cross_join_ref->left = std::move(first_cross_join_ref);
	second_cross_join_ref->right = std::move(temp_cte_select_subquery);

	select_node->from_table = std::move(second_cross_join_ref);
	vector<unique_ptr<ParsedExpression>> count_children;
	count_children.push_back(make_uniq<ColumnRefExpression>("temp", "cte1"));
	auto count_function = make_uniq<FunctionExpression>("count", std::move(count_children));

	path_finding_conditions.push_back(AddPathQuantifierCondition(
	    previous_vertex_element->variable_binding, next_vertex_element->variable_binding, edge_table, edge_subpath));

	select_node->where_clause = CreateWhereClause(path_finding_conditions);

	select_statement->node = std::move(select_node);
	cte_info->query = std::move(select_statement);
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
				if (final_select_node->cte_map.map.find("cte1") == final_select_node->cte_map.map.end()) {
					edge_element = reinterpret_cast<PathElement *>(edge_subpath->path_list[0].get());
					if (edge_element->match_type == PGQMatchType::MATCH_EDGE_RIGHT) {
						final_select_node->cte_map.map["cte1"] = CreateDirectedCSRCTE(
						    FindGraphTable(edge_element->label, pg_table), previous_vertex_element->variable_binding,
						    edge_element->variable_binding, next_vertex_element->variable_binding);
					} else if (edge_element->match_type == PGQMatchType::MATCH_EDGE_ANY) {
						final_select_node->cte_map.map["cte1"] =
						    CreateUndirectedCSRCTE(FindGraphTable(edge_element->label, pg_table), final_select_node);
					} else {
						throw NotImplementedException("Cannot do shortest path for edge type %s",
						                              edge_element->match_type == PGQMatchType::MATCH_EDGE_LEFT
						                                  ? "MATCH_EDGE_LEFT"
						                                  : "MATCH_EDGE_LEFT_RIGHT");
					}
				}
				string shortest_path_cte_name = "shortest_path_cte";
				if (final_select_node->cte_map.map.find(shortest_path_cte_name) ==
				    final_select_node->cte_map.map.end()) {
					final_select_node->cte_map.map[shortest_path_cte_name] = GenerateShortestPathCTE(
					    pg_table, edge_subpath, previous_vertex_element, next_vertex_element, path_finding_conditions);
					auto cte_shortest_path_ref = make_uniq<BaseTableRef>();
					cte_shortest_path_ref->table_name = shortest_path_cte_name;
					if (!final_select_node->from_table) {
						final_select_node->from_table = std::move(cte_shortest_path_ref);
					} else {
						auto join_ref = make_uniq<JoinRef>(JoinRefType::CROSS);
						join_ref->left = std::move(final_select_node->from_table);
						join_ref->right = std::move(cte_shortest_path_ref);
						final_select_node->from_table = std::move(join_ref);
					}

					conditions.push_back(make_uniq<ComparisonExpression>(
					    ExpressionType::COMPARE_EQUAL,
					    make_uniq<ColumnRefExpression>("src_rowid", shortest_path_cte_name),
					    make_uniq<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding)));
					conditions.push_back(make_uniq<ComparisonExpression>(
					    ExpressionType::COMPARE_EQUAL,
					    make_uniq<ColumnRefExpression>("dst_rowid", shortest_path_cte_name),
					    make_uniq<ColumnRefExpression>("rowid", next_vertex_element->variable_binding)));
				}
				auto shortest_path_ref = make_uniq<ColumnRefExpression>("path", shortest_path_cte_name);
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
		auto previous_rowid = make_uniq<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
		auto edge_rowid = make_uniq<ColumnRefExpression>("rowid", edge_element->variable_binding);
		auto next_rowid = make_uniq<ColumnRefExpression>("rowid", next_vertex_element->variable_binding);
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

	auto src_row_id = make_uniq<ColumnRefExpression>("rowid", prev_binding);
	auto dst_row_id = make_uniq<ColumnRefExpression>("rowid", next_binding);
	auto csr_id = make_uniq<ConstantExpression>(Value::INTEGER(0));

	vector<unique_ptr<ParsedExpression>> pathfinding_children;
	pathfinding_children.push_back(std::move(csr_id));
	pathfinding_children.push_back(
	    std::move(GetCountTable(edge_table->source_pg_table, prev_binding, edge_table->source_pk[0])));
	pathfinding_children.push_back(std::move(src_row_id));
	pathfinding_children.push_back(std::move(dst_row_id));

	auto reachability_function = make_uniq<FunctionExpression>("iterativelength", std::move(pathfinding_children));

	auto cte_col_ref = make_uniq<ColumnRefExpression>("temp", "__x");

	vector<unique_ptr<ParsedExpression>> addition_children;
	addition_children.push_back(std::move(cte_col_ref));
	addition_children.push_back(std::move(reachability_function));

	auto addition_function = make_uniq<FunctionExpression>("add", std::move(addition_children));
	auto lower_limit = make_uniq<ConstantExpression>(Value::INTEGER(static_cast<int32_t>(subpath->lower)));
	auto upper_limit = make_uniq<ConstantExpression>(Value::INTEGER(static_cast<int32_t>(subpath->upper)));
	auto between_expression =
	    make_uniq<BetweenExpression>(std::move(addition_function), std::move(lower_limit), std::move(upper_limit));
	return std::move(between_expression);
}

void PGQMatchFunction::AddPathFinding(unique_ptr<SelectNode> &select_node,
                                      vector<unique_ptr<ParsedExpression>> &conditions, const string &prev_binding,
                                      const string &edge_binding, const string &next_binding,
                                      const shared_ptr<PropertyGraphTable> &edge_table,
                                      CreatePropertyGraphInfo &pg_table, SubPath *subpath, PGQMatchType edge_type) {
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
	if (select_node->from_table) {
		// create a cross join since there is already something in the
		// from clause
		auto from_join = make_uniq<JoinRef>(JoinRefType::CROSS);
		from_join->left = std::move(select_node->from_table);
		from_join->right = std::move(temp_cte_select_subquery);
		select_node->from_table = std::move(from_join);
	} else {
		select_node->from_table = std::move(temp_cte_select_subquery);
	}
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
		auto column_ref = dynamic_cast<ColumnRefExpression *>(parsed_ref->children[0].get());
		if (column_ref == nullptr) {
			continue;
		}

		if (column_ref->column_names[0] != subpath.path_variable) {
			continue;
		}
		// Trying to check parsed_ref->alias directly leads to a segfault
		string column_alias = parsed_ref->alias;
		if (parsed_ref->function_name == "element_id") {
			// Check subpath name matches the column referenced in the function -->
			// element_id(named_subpath)
			auto shortest_path_function = CreatePathFindingFunction(subpath.path_list, pg_table, subpath.path_variable,
			                                                        final_select_node, conditions);

			if (column_alias.empty()) {
				shortest_path_function->alias = "element_id(" + subpath.path_variable + ")";
			} else {
				shortest_path_function->alias = column_alias;
			}
			original_ref.column_list.erase(original_ref.column_list.begin() + static_cast<int64_t>(idx_i));
			original_ref.column_list.insert(original_ref.column_list.begin() + static_cast<int64_t>(idx_i),
			                                std::move(shortest_path_function));
		} else if (parsed_ref->function_name == "path_length") {
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
			path_length_function->alias =
			    column_alias.empty() ? "path_length(" + subpath.path_variable + ")" : column_alias;
			original_ref.column_list.erase(original_ref.column_list.begin() + static_cast<int64_t>(idx_i));
			original_ref.column_list.insert(original_ref.column_list.begin() + static_cast<int64_t>(idx_i),
			                                std::move(path_length_function));
		} else if (parsed_ref->function_name == "vertices" || parsed_ref->function_name == "edges") {
			auto list_slice_children = vector<unique_ptr<ParsedExpression>>();
			auto shortest_path_function = CreatePathFindingFunction(subpath.path_list, pg_table, subpath.path_variable,
			                                                        final_select_node, conditions);
			list_slice_children.push_back(std::move(shortest_path_function));

			if (parsed_ref->function_name == "vertices") {
				list_slice_children.push_back(make_uniq<ConstantExpression>(Value::INTEGER(1)));
			} else {
				list_slice_children.push_back(make_uniq<ConstantExpression>(Value::INTEGER(2)));
			}
			auto slice_end = make_uniq<ConstantExpression>(Value::INTEGER(-1));
			auto slice_step = make_uniq<ConstantExpression>(Value::INTEGER(2));

			list_slice_children.push_back(std::move(slice_end));
			list_slice_children.push_back(std::move(slice_step));
			auto list_slice = make_uniq<FunctionExpression>("list_slice", std::move(list_slice_children));
			if (parsed_ref->function_name == "vertices") {
				list_slice->alias = column_alias.empty() ? "vertices(" + subpath.path_variable + ")" : column_alias;
			} else {
				list_slice->alias = column_alias.empty() ? "edges(" + subpath.path_variable + ")" : column_alias;
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

			if (edge_subpath->upper > 1) {
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
		// 'shortest_path_cte' is a special table populated by pgq.
		if (column_ref->column_names[0] == "shortest_path_cte") {
			continue;
		}
		// 'rowid' is a column duckdb binds automatically.
		if (column_ref->column_names.back() == "rowid") {
			continue;
		}
		if (column_ref->column_names.size() == 1) {
			bool single_alias = false;
			for (const auto &alias : alias_to_vertex_and_edge_tables) {
				if (alias.first == column_ref->column_names[0]) {
					single_alias = true;
					break;
				}
			}
			if (single_alias) {
				continue;
			}
		}
		const auto cur_fq_col_name = StringUtil::Join(column_ref->column_names, /*separator=*/".");
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

	auto final_select_node = make_uniq<SelectNode>();
	case_insensitive_map_t<shared_ptr<PropertyGraphTable>> alias_map;

	int32_t extra_alias_counter = 0;
	for (idx_t idx_i = 0; idx_i < ref->path_patterns.size(); idx_i++) {
		auto &path_pattern = ref->path_patterns[idx_i];
		// Check if the element is PathElement or a Subpath with potentially many
		// items
		ProcessPathList(path_pattern->path_elements, conditions, final_select_node, alias_map, *pg_table,
		                extra_alias_counter, *ref);
	}

	// Go through all aliases encountered
	for (auto &table_alias_entry : alias_map) {
		auto table_ref = table_alias_entry.second->CreateBaseTableRef();
		table_ref->alias = table_alias_entry.first;
		if (final_select_node->from_table) {
			auto new_root = make_uniq<JoinRef>(JoinRefType::CROSS);
			new_root->left = std::move(final_select_node->from_table);
			new_root->right = std::move(table_ref);
			final_select_node->from_table = std::move(new_root);
		} else {
			final_select_node->from_table = std::move(table_ref);
		}
	}

	if (ref->where_clause) {
		conditions.push_back(std::move(ref->where_clause));
	}

	// Maps from table alias to table, including vertex and edge tables.
	auto alias_to_vertex_and_edge_tables = PopulateGraphTableAliasMap(*pg_table, *ref);
	CheckColumnBinding(*pg_table, *ref, alias_to_vertex_and_edge_tables);

	std::vector<unique_ptr<ParsedExpression>> final_column_list;

	for (auto &expression : ref->column_list) {
		unordered_set<string> named_subpaths;

		// Handle ColumnRefExpression.
		auto *column_ref = dynamic_cast<ColumnRefExpression *>(expression.get());
		if (column_ref != nullptr) {
			if (named_subpaths.count(column_ref->column_names[0]) && column_ref->column_names.size() == 1) {
				final_column_list.emplace_back(make_uniq<ColumnRefExpression>("path", column_ref->column_names[0]));
			} else {
				final_column_list.push_back(std::move(expression));
			}
			continue;
		}

		// Handle FunctionExpression.
		auto *function_ref = dynamic_cast<FunctionExpression *>(expression.get());
		if (function_ref != nullptr) {
			if (function_ref->function_name == "path_length") {
				column_ref = dynamic_cast<ColumnRefExpression *>(function_ref->children[0].get());
				if (column_ref == nullptr) {
					continue;
				}
				if (named_subpaths.count(column_ref->column_names[0]) && column_ref->column_names.size() == 1) {
					auto path_ref = make_uniq<ColumnRefExpression>("path", column_ref->column_names[0]);
					vector<unique_ptr<ParsedExpression>> path_children;
					path_children.push_back(std::move(path_ref));
					auto path_len = make_uniq<FunctionExpression>("len", std::move(path_children));
					auto constant_two = make_uniq<ConstantExpression>(Value::INTEGER(2));
					vector<unique_ptr<ParsedExpression>> div_children;
					div_children.push_back(std::move(path_len));
					div_children.push_back(std::move(constant_two));
					auto div_expression = make_uniq<FunctionExpression>("//", std::move(div_children));
					div_expression->alias = "path_length_" + column_ref->column_names[0];
					final_column_list.emplace_back(std::move(div_expression));
				}
			} else {
				final_column_list.push_back(std::move(expression));
			}

			continue;
		}

		// Handle StarExpression.
		auto *star_expression = dynamic_cast<StarExpression *>(expression.get());
		if (star_expression != nullptr) {
			if (!star_expression->relation_name.empty()) {
				auto tbl_iter = alias_to_vertex_and_edge_tables.find(star_expression->relation_name);
				if (tbl_iter == alias_to_vertex_and_edge_tables.end()) {
					continue;
				}
			}

			auto selected_col_exprs =
			    star_expression->relation_name.empty()
			        ? GetColRefExprFromPg(alias_to_vertex_and_edge_tables)
			        : GetColRefExprFromPg(alias_to_vertex_and_edge_tables, star_expression->relation_name);

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
	auto result = make_uniq<SubqueryRef>(std::move(subquery), ref->alias);
	return std::move(result);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterMatchTableFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(PGQMatchFunction());
}

} // namespace duckdb
