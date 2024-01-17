#include <duckpgq_extension.hpp>
#include "duckpgq/functions/tablefunctions/match.hpp"

#include "duckdb/parser/tableref/matchref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"

#include "duckdb/parser/query_node/select_node.hpp"

#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"

#include "duckdb/parser/property_graph_table.hpp"
#include "duckdb/parser/subpath_element.hpp"

#include <iostream>


namespace duckdb {
	shared_ptr<PropertyGraphTable>
	PGQMatchFunction::FindGraphTable(const string& label,
	                                 CreatePropertyGraphInfo& pg_table) {
		const auto graph_table_entry = pg_table.label_map.find(label);
		if (graph_table_entry == pg_table.label_map.end()) {
			throw BinderException("The label %s is not registered in property graph %s",
			                      label, pg_table.property_graph_name);
		}

		return graph_table_entry->second;
	}

	void PGQMatchFunction::CheckInheritance(
		const shared_ptr<PropertyGraphTable>& tableref, PathElement* element,
		vector<unique_ptr<ParsedExpression>>& conditions) {
		if (tableref->main_label == element->label) {
			return;
		}
		auto constant_expression_two =
				make_uniq<ConstantExpression>(Value::INTEGER((int32_t) 2));
		const auto itr = std::find(tableref->sub_labels.begin(), tableref->sub_labels.end(),
		                           element->label);

		const auto idx_of_element = std::distance(tableref->sub_labels.begin(), itr);
		auto constant_expression_idx_label =
				make_uniq<ConstantExpression>(Value::INTEGER(static_cast<int32_t>(idx_of_element)));

		vector<unique_ptr<ParsedExpression>> power_of_children;
		power_of_children.push_back(std::move(constant_expression_two));
		power_of_children.push_back(std::move(constant_expression_idx_label));
		auto power_of_term =
				make_uniq<FunctionExpression>("power", std::move(power_of_children));
		auto bigint_cast =
				make_uniq<CastExpression>(LogicalType::BIGINT, std::move(power_of_term));
		auto subcategory_colref = make_uniq<ColumnRefExpression>(
			tableref->discriminator, element->variable_binding);
		vector<unique_ptr<ParsedExpression>> and_children;
		and_children.push_back(std::move(subcategory_colref));
		and_children.push_back(std::move(bigint_cast));

		auto and_expression =
				make_uniq<FunctionExpression>("&", std::move(and_children));

		auto constant_expression_idx_label_comparison = make_uniq<ConstantExpression>(
			Value::INTEGER(static_cast<int32_t>(idx_of_element + 1)));

		auto subset_compare = make_uniq<ComparisonExpression>(
			ExpressionType::COMPARE_EQUAL, std::move(and_expression),
			std::move(constant_expression_idx_label_comparison));
		conditions.push_back(std::move(subset_compare));
	}

	void PGQMatchFunction::CheckEdgeTableConstraints(
		const string& src_reference, const string& dst_reference,
		const shared_ptr<PropertyGraphTable>& edge_table) {
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

	unique_ptr<ParsedExpression> PGQMatchFunction::CreateMatchJoinExpression(
		vector<string> vertex_keys, vector<string> edge_keys,
		const string& vertex_alias, const string& edge_alias) {
		vector<unique_ptr<ParsedExpression>> conditions;

		if (vertex_keys.size() != edge_keys.size()) {
			throw BinderException("Vertex columns and edge columns size mismatch");
		}
		for (idx_t i = 0; i < vertex_keys.size(); i++) {
			auto vertex_colref =
					make_uniq<ColumnRefExpression>(vertex_keys[i], vertex_alias);
			auto edge_colref = make_uniq<ColumnRefExpression>(edge_keys[i], edge_alias);
			conditions.push_back(make_uniq<ComparisonExpression>(
				ExpressionType::COMPARE_EQUAL, std::move(vertex_colref),
				std::move(edge_colref)));
		}
		unique_ptr<ParsedExpression> where_clause;

		for (auto& condition: conditions) {
			if (where_clause) {
				where_clause = make_uniq<ConjunctionExpression>(
					ExpressionType::CONJUNCTION_AND, std::move(where_clause),
					std::move(condition));
			} else {
				where_clause = std::move(condition);
			}
		}

		return where_clause;
	}

	PathElement* PGQMatchFunction::GetPathElement(
		const unique_ptr<PathReference>& path_reference) {
		if (path_reference->path_reference_type ==
		    PGQPathReferenceType::PATH_ELEMENT) {
			return reinterpret_cast<PathElement *>(path_reference.get());
		}
		if (path_reference->path_reference_type ==
		    PGQPathReferenceType::SUBPATH) {
			return nullptr;
		}
		throw InternalException("Unknown path reference type detected");
	}

	unique_ptr<SubqueryExpression>
	PGQMatchFunction::GetCountTable(const shared_ptr<PropertyGraphTable>& edge_table,
	                                const string& prev_binding) {
		// SELECT count(s.id) FROM src s
		auto select_count = make_uniq<SelectStatement>();
		auto select_inner = make_uniq<SelectNode>();
		auto ref = make_uniq<BaseTableRef>();

		ref->table_name = edge_table->source_reference;
		ref->alias = prev_binding;
		select_inner->from_table = std::move(ref);
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(
			make_uniq<ColumnRefExpression>(edge_table->source_pk[0], prev_binding));

		auto count_function =
				make_uniq<FunctionExpression>("count", std::move(children));
		select_inner->select_list.push_back(std::move(count_function));
		select_count->node = std::move(select_inner);
		auto result = make_uniq<SubqueryExpression>();
		result->subquery = std::move(select_count);
		result->subquery_type = SubqueryType::SCALAR;
		return result;
	}

	unique_ptr<JoinRef>
	PGQMatchFunction::GetJoinRef(const shared_ptr<PropertyGraphTable>& edge_table,
	                             const string& edge_binding,
	                             const string& prev_binding,
	                             const string& next_binding) {
		auto first_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
		first_join_ref->type = JoinType::INNER;

		auto second_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
		second_join_ref->type = JoinType::INNER;

		auto edge_base_ref = make_uniq<BaseTableRef>();
		edge_base_ref->table_name = edge_table->table_name;
		edge_base_ref->alias = edge_binding;
		auto src_base_ref = make_uniq<BaseTableRef>();
		src_base_ref->table_name = edge_table->source_reference;
		src_base_ref->alias = prev_binding;
		second_join_ref->left = std::move(edge_base_ref);
		second_join_ref->right = std::move(src_base_ref);
		auto t_from_ref =
				make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_binding);
		auto src_cid_ref =
				make_uniq<ColumnRefExpression>(edge_table->source_pk[0], prev_binding);
		second_join_ref->condition = make_uniq<ComparisonExpression>(
			ExpressionType::COMPARE_EQUAL, std::move(t_from_ref),
			std::move(src_cid_ref));
		auto dst_base_ref = make_uniq<BaseTableRef>();
		dst_base_ref->table_name = edge_table->destination_reference;
		dst_base_ref->alias = next_binding;
		first_join_ref->left = std::move(second_join_ref);
		first_join_ref->right = std::move(dst_base_ref);

		auto t_to_ref = make_uniq<ColumnRefExpression>(edge_table->destination_fk[0],
		                                               edge_binding);
		auto dst_cid_ref = make_uniq<ColumnRefExpression>(
			edge_table->destination_pk[0], next_binding);
		first_join_ref->condition = make_uniq<ComparisonExpression>(
			ExpressionType::COMPARE_EQUAL, std::move(t_to_ref),
			std::move(dst_cid_ref));
		return first_join_ref;
	}

	unique_ptr<SubqueryRef> PGQMatchFunction::CreateCountCTESubquery() {
		//! BEGIN OF (SELECT count(cte1.temp) as temp * 0 from cte1) __x

		auto temp_cte_select_node = make_uniq<SelectNode>();

		auto cte_table_ref = make_uniq<BaseTableRef>();

		cte_table_ref->table_name = "cte1";
		temp_cte_select_node->from_table = std::move(cte_table_ref);
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(make_uniq<ColumnRefExpression>("temp", "cte1"));

		auto count_function =
				make_uniq<FunctionExpression>("count", std::move(children));

		auto zero = make_uniq<ConstantExpression>(Value::INTEGER((int32_t) 0));

		vector<unique_ptr<ParsedExpression>> multiply_children;

		multiply_children.push_back(std::move(zero));
		multiply_children.push_back(std::move(count_function));
		auto multiply_function =
				make_uniq<FunctionExpression>("multiply", std::move(multiply_children));
		multiply_function->alias = "temp";
		temp_cte_select_node->select_list.push_back(std::move(multiply_function));
		auto temp_cte_select_statement = make_uniq<SelectStatement>();
		temp_cte_select_statement->node = std::move(temp_cte_select_node);

		auto temp_cte_select_subquery =
				make_uniq<SubqueryRef>(std::move(temp_cte_select_statement), "__x");
		//! END OF (SELECT count(cte1.temp) * 0 as temp from cte1) __x
		return temp_cte_select_subquery;
	}

	unique_ptr<CommonTableExpressionInfo>
	PGQMatchFunction::CreateCSRCTE(const shared_ptr<PropertyGraphTable>& edge_table,
	                               const string& prev_binding,
	                               const string& edge_binding,
	                               const string& next_binding) {
		auto csr_edge_id_constant =
				make_uniq<ConstantExpression>(Value::INTEGER(0));
		auto count_create_edge_select = GetCountTable(edge_table, prev_binding);

		auto cast_subquery_expr = make_uniq<SubqueryExpression>();
		auto cast_select_node = make_uniq<SelectNode>();

		vector<unique_ptr<ParsedExpression>> csr_vertex_children;
		csr_vertex_children.push_back(
			make_uniq<ConstantExpression>(Value::INTEGER(0)));

		auto count_create_vertex_expr = GetCountTable(edge_table, prev_binding);

		csr_vertex_children.push_back(std::move(count_create_vertex_expr));

		csr_vertex_children.push_back(
			make_uniq<ColumnRefExpression>("dense_id", "sub"));
		csr_vertex_children.push_back(make_uniq<ColumnRefExpression>("cnt", "sub"));

		auto create_vertex_function = make_uniq<FunctionExpression>(
			"create_csr_vertex", std::move(csr_vertex_children));
		vector<unique_ptr<ParsedExpression>> sum_children;
		sum_children.push_back(std::move(create_vertex_function));
		auto sum_function =
				make_uniq<FunctionExpression>("sum", std::move(sum_children));

		auto inner_select_statement = make_uniq<SelectStatement>();
		auto inner_select_node = make_uniq<SelectNode>();

		auto source_rowid_colref =
				make_uniq<ColumnRefExpression>("rowid", prev_binding);
		source_rowid_colref->alias = "dense_id";

		auto count_create_inner_expr = make_uniq<SubqueryExpression>();
		count_create_inner_expr->subquery_type = SubqueryType::SCALAR;
		auto edge_src_colref =
				make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_binding);
		vector<unique_ptr<ParsedExpression>> inner_count_children;
		inner_count_children.push_back(std::move(edge_src_colref));
		auto inner_count_function =
				make_uniq<FunctionExpression>("count", std::move(inner_count_children));
		inner_count_function->alias = "cnt";

		inner_select_node->select_list.push_back(std::move(source_rowid_colref));
		inner_select_node->select_list.push_back(std::move(inner_count_function));
		auto source_rowid_colref_1 =
				make_uniq<ColumnRefExpression>("rowid", prev_binding);
		expression_map_t<idx_t> grouping_expression_map;
		inner_select_node->groups.group_expressions.push_back(
			std::move(source_rowid_colref_1));
		GroupingSet grouping_set = {0};
		inner_select_node->groups.grouping_sets.push_back(grouping_set);

		auto inner_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
		inner_join_ref->type = JoinType::LEFT;
		auto left_base_ref = make_uniq<BaseTableRef>();
		left_base_ref->table_name = edge_table->source_reference;
		left_base_ref->alias = prev_binding;
		auto right_base_ref = make_uniq<BaseTableRef>();
		right_base_ref->table_name = edge_table->table_name;
		right_base_ref->alias = edge_binding;
		inner_join_ref->left = std::move(left_base_ref);
		inner_join_ref->right = std::move(right_base_ref);

		auto edge_join_colref =
				make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_binding);
		auto vertex_join_colref =
				make_uniq<ColumnRefExpression>(edge_table->source_pk[0], prev_binding);

		inner_join_ref->condition = make_uniq<ComparisonExpression>(
			ExpressionType::COMPARE_EQUAL, std::move(edge_join_colref),
			std::move(vertex_join_colref));
		inner_select_node->from_table = std::move(inner_join_ref);
		inner_select_statement->node = std::move(inner_select_node);

		auto inner_from_subquery =
				make_uniq<SubqueryRef>(std::move(inner_select_statement), "sub");

		cast_select_node->from_table = std::move(inner_from_subquery);

		cast_select_node->select_list.push_back(std::move(sum_function));
		auto cast_select_stmt = make_uniq<SelectStatement>();
		cast_select_stmt->node = std::move(cast_select_node);
		cast_subquery_expr->subquery = std::move(cast_select_stmt);
		cast_subquery_expr->subquery_type = SubqueryType::SCALAR;

		auto src_rowid_colref = make_uniq<ColumnRefExpression>("rowid", prev_binding);
		auto dst_rowid_colref = make_uniq<ColumnRefExpression>("rowid", next_binding);
		auto edge_rowid_colref =
				make_uniq<ColumnRefExpression>("rowid", edge_binding);

		auto cast_expression = make_uniq<CastExpression>(
			LogicalType::BIGINT, std::move(cast_subquery_expr));

		vector<unique_ptr<ParsedExpression>> csr_edge_children;
		csr_edge_children.push_back(std::move(csr_edge_id_constant));
		csr_edge_children.push_back(std::move(count_create_edge_select));
		csr_edge_children.push_back(std::move(cast_expression));
		csr_edge_children.push_back(std::move(src_rowid_colref));
		csr_edge_children.push_back(std::move(dst_rowid_colref));
		csr_edge_children.push_back(std::move(edge_rowid_colref));

		auto outer_select_node = make_uniq<SelectNode>();

		auto create_csr_edge_function = make_uniq<FunctionExpression>(
			"create_csr_edge", std::move(csr_edge_children));
		create_csr_edge_function->alias = "temp";

		outer_select_node->select_list.push_back(std::move(create_csr_edge_function));
		outer_select_node->from_table =
				GetJoinRef(edge_table, edge_binding, prev_binding, next_binding);
		auto outer_select_statement = make_uniq<SelectStatement>();

		outer_select_statement->node = std::move(outer_select_node);
		auto info = make_uniq<CommonTableExpressionInfo>();
		info->query = std::move(outer_select_statement);
		return info;
	}

	void PGQMatchFunction::EdgeTypeAny(
		const shared_ptr<PropertyGraphTable>& edge_table, const string& edge_binding,
		const string& prev_binding, const string& next_binding,
		vector<unique_ptr<ParsedExpression>>& conditions) {
		// (a) src.key = edge.src
		auto src_left_expr = CreateMatchJoinExpression(
			edge_table->source_pk, edge_table->source_fk,
			prev_binding, edge_binding);
		// (b) dst.key = edge.dst
		auto dst_left_expr = CreateMatchJoinExpression(
			edge_table->destination_pk, edge_table->destination_fk,
			next_binding, edge_binding);
		// (a) AND (b)
		auto combined_left_expr = make_uniq<ConjunctionExpression>(
			ExpressionType::CONJUNCTION_AND, std::move(src_left_expr),
			std::move(dst_left_expr));
		// (c) src.key = edge.dst
		auto src_right_expr = CreateMatchJoinExpression(edge_table->source_pk,
		                                                edge_table->destination_fk,
		                                                prev_binding, edge_binding);
		// (d) dst.key = edge.src
		auto dst_right_expr = CreateMatchJoinExpression(edge_table->destination_pk,
		                                                edge_table->source_fk,
		                                                next_binding, edge_binding);
		// (c) AND (d)
		auto combined_right_expr = make_uniq<ConjunctionExpression>(
			ExpressionType::CONJUNCTION_AND, std::move(src_right_expr),
			std::move(dst_right_expr));
		// ((a) AND (b)) OR ((c) AND (d))
		auto combined_expr = make_uniq<ConjunctionExpression>(
			ExpressionType::CONJUNCTION_OR, std::move(combined_left_expr),
			std::move(combined_right_expr));
		conditions.push_back(std::move(combined_expr));
	}

	void PGQMatchFunction::EdgeTypeLeft(
		const shared_ptr<PropertyGraphTable>& edge_table, const string& next_table_name,
		const string& prev_table_name, const string& edge_binding,
		const string& prev_binding, const string& next_binding,
		vector<unique_ptr<ParsedExpression>>& conditions) {
		CheckEdgeTableConstraints(next_table_name, prev_table_name, edge_table);
		conditions.push_back(CreateMatchJoinExpression(edge_table->source_pk,
		                                               edge_table->source_fk,
		                                               next_binding, edge_binding));
		conditions.push_back(CreateMatchJoinExpression(edge_table->destination_pk,
		                                               edge_table->destination_fk,
		                                               prev_binding, edge_binding));
	}

	void PGQMatchFunction::EdgeTypeRight(
		const shared_ptr<PropertyGraphTable>& edge_table, const string& next_table_name,
		const string& prev_table_name, const string& edge_binding,
		const string& prev_binding, const string& next_binding,
		vector<unique_ptr<ParsedExpression>>& conditions) {
		CheckEdgeTableConstraints(prev_table_name, next_table_name, edge_table);
		conditions.push_back(CreateMatchJoinExpression(edge_table->source_pk,
		                                               edge_table->source_fk,
		                                               prev_binding, edge_binding));
		conditions.push_back(CreateMatchJoinExpression(edge_table->destination_pk,
		                                               edge_table->destination_fk,
		                                               next_binding, edge_binding));
	}

	void PGQMatchFunction::EdgeTypeLeftRight(
		const shared_ptr<PropertyGraphTable>& edge_table, const string& edge_binding,
		const string& prev_binding, const string& next_binding,
		vector<unique_ptr<ParsedExpression>>& conditions,
		unordered_map<string, string>& alias_map, int32_t& extra_alias_counter) {
		auto src_left_expr = CreateMatchJoinExpression(
			edge_table->source_pk, edge_table->source_fk, next_binding, edge_binding);
		auto dst_left_expr = CreateMatchJoinExpression(edge_table->destination_pk,
		                                               edge_table->destination_fk,
		                                               prev_binding, edge_binding);

		auto combined_left_expr = make_uniq<ConjunctionExpression>(
			ExpressionType::CONJUNCTION_AND, std::move(src_left_expr),
			std::move(dst_left_expr));

		const auto additional_edge_alias =
				edge_binding + std::to_string(extra_alias_counter);
		extra_alias_counter++;

		alias_map[additional_edge_alias] = edge_table->table_name;

		auto src_right_expr =
				CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
				                          prev_binding, additional_edge_alias);
		auto dst_right_expr = CreateMatchJoinExpression(
			edge_table->destination_pk, edge_table->destination_fk, next_binding,
			additional_edge_alias);
		auto combined_right_expr = make_uniq<ConjunctionExpression>(
			ExpressionType::CONJUNCTION_AND, std::move(src_right_expr),
			std::move(dst_right_expr));

		auto combined_expr = make_uniq<ConjunctionExpression>(
			ExpressionType::CONJUNCTION_AND, std::move(combined_left_expr),
			std::move(combined_right_expr));
		conditions.push_back(std::move(combined_expr));
	}

	PathElement* PGQMatchFunction::HandleNestedSubPath(
		unique_ptr<PathReference>& path_reference,
		vector<unique_ptr<ParsedExpression>>& conditions, idx_t element_idx) {
		auto subpath = reinterpret_cast<SubPath *>(path_reference.get());
		return GetPathElement(subpath->path_list[element_idx]);
	}

	unique_ptr<ParsedExpression>
	CreateWhereClause(vector<unique_ptr<ParsedExpression>>& conditions) {
		unique_ptr<ParsedExpression> where_clause;
		for (auto& condition: conditions) {
			if (where_clause) {
				where_clause = make_uniq<ConjunctionExpression>(
					ExpressionType::CONJUNCTION_AND, std::move(where_clause),
					std::move(condition));
			} else {
				where_clause = std::move(condition);
			}
		}
		return where_clause;
	}

	unique_ptr<ParsedExpression> PGQMatchFunction::CreatePathFindingFunction(
		vector<unique_ptr<PathReference>>& path_list, CreatePropertyGraphInfo& pg_table) {
		// This method will return a SubqueryRef of a list of rowids
		// For every vertex and edge element, we add the rowid to the list using list_append, or list_prepend
		// The difficulty is that there may be a (un)bounded path pattern at some point in the query
		// This is computed using the shortestpath() UDF and returns a list.
		// This list will be part of the full list of element rowids, using list_concat.
		// For now we will only support returning rowids
		unique_ptr<ParsedExpression> final_list;

		auto previous_vertex_element = GetPathElement(path_list[0]);
		if (!previous_vertex_element) {
			// We hit a vertex element with a WHERE, but we only care about the rowid here
			auto previous_vertex_subpath = reinterpret_cast<SubPath *>(path_list[0].get());
			previous_vertex_element = GetPathElement(previous_vertex_subpath->path_list[0]);
		}

		for (idx_t idx_i = 1; idx_i < path_list.size(); idx_i = idx_i + 2) {
			auto next_vertex_element = GetPathElement(path_list[idx_i + 1]);
			if (!next_vertex_element) {
				auto next_vertex_subpath = reinterpret_cast<SubPath *>(path_list[idx_i + 1].get());
				next_vertex_element = GetPathElement(next_vertex_subpath->path_list[0]);
			}

			auto edge_element = GetPathElement(path_list[idx_i]);
			if (!edge_element) {
				auto edge_subpath = reinterpret_cast<SubPath *>(path_list[idx_i].get());
				if (edge_subpath->upper > 1) {
					// (un)bounded shortest path
					// Add the shortest path UDF
					edge_element = GetPathElement(edge_subpath->path_list[0]);
					auto edge_table = FindGraphTable(edge_element->label, pg_table);
					auto src_row_id = make_uniq<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
					auto dst_row_id = make_uniq<ColumnRefExpression>("rowid", next_vertex_element->variable_binding);
					auto csr_id = make_uniq<ConstantExpression>(Value::INTEGER(0));

					vector<unique_ptr<ParsedExpression>> pathfinding_children;
					pathfinding_children.push_back(std::move(csr_id));
					pathfinding_children.push_back(
						std::move(GetCountTable(edge_table, previous_vertex_element->variable_binding)));
					pathfinding_children.push_back(std::move(src_row_id));
					pathfinding_children.push_back(std::move(dst_row_id));

					auto shortest_path_function = make_uniq<FunctionExpression>("shortestpath",
					                                                            std::move(pathfinding_children));

					if (!final_list) {
						final_list = std::move(shortest_path_function);
					} else {
						auto pop_front_shortest_path_children = vector<unique_ptr<ParsedExpression>>();
						pop_front_shortest_path_children.push_back(std::move(shortest_path_function));
						auto pop_front = make_uniq<FunctionExpression>("array_pop_front",
						                                               std::move(pop_front_shortest_path_children));

						auto final_list_children = vector<unique_ptr<ParsedExpression>>();
						final_list_children.push_back(std::move(final_list));
						final_list_children.push_back(std::move(pop_front));
						final_list = make_uniq<FunctionExpression>("list_concat", std::move(final_list_children));
					}
					// Set next vertex to be previous
					previous_vertex_element = next_vertex_element;
					continue;
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
		}

		return final_list;
	}

	void PGQMatchFunction::AddEdgeJoins(const unique_ptr<SelectNode>& select_node,
	                                    const shared_ptr<PropertyGraphTable>& edge_table,
	                                    const shared_ptr<PropertyGraphTable>& previous_vertex_table,
	                                    const shared_ptr<PropertyGraphTable>& next_vertex_table,
	                                    PGQMatchType edge_type,
	                                    const string& edge_binding,
	                                    const string& prev_binding,
	                                    const string& next_binding,
	                                    vector<unique_ptr<ParsedExpression>>& conditions,
	                                    unordered_map<string, string>& alias_map,
	                                    int32_t& extra_alias_counter) {
		switch (edge_type) {
			case PGQMatchType::MATCH_EDGE_ANY: {
				select_node->modifiers.push_back(make_uniq<DistinctModifier>());
				EdgeTypeAny(edge_table, edge_binding, prev_binding, next_binding, conditions);
				break;
			}
			case PGQMatchType::MATCH_EDGE_LEFT:
				EdgeTypeLeft(edge_table, next_vertex_table->table_name,
				             previous_vertex_table->table_name,
				             edge_binding, prev_binding, next_binding, conditions);
				break;
			case PGQMatchType::MATCH_EDGE_RIGHT:
				EdgeTypeRight(edge_table, next_vertex_table->table_name,
				              previous_vertex_table->table_name,
				              edge_binding, prev_binding, next_binding, conditions);
				break;
			case PGQMatchType::MATCH_EDGE_LEFT_RIGHT: {
				EdgeTypeLeftRight(edge_table, edge_binding,
				                  prev_binding, next_binding, conditions,
				                  alias_map, extra_alias_counter);
				break;
			}
			default:
				throw InternalException("Unknown match type found");
		}
	}

	void PGQMatchFunction::AddPathFinding(const unique_ptr<SelectNode>& select_node,
	                                      unique_ptr<TableRef>& from_clause,
	                                      vector<unique_ptr<ParsedExpression>>& conditions,
	                                      const string& prev_binding, const string& edge_binding,
	                                      const string& next_binding,
	                                      const shared_ptr<PropertyGraphTable>& edge_table,
	                                      const SubPath* subpath) {
		//! START
		//! FROM (SELECT count(cte1.temp) * 0 as temp from cte1) __x
		select_node->cte_map.map["cte1"] = CreateCSRCTE(
			edge_table, prev_binding,
			edge_binding,
			next_binding);
		// auto cross_join_src_dst = make_uniq<JoinRef>(JoinRefType::CROSS);

		//! src alias (FROM src a)
		// auto src_vertex_ref = make_uniq<BaseTableRef>();
		// src_vertex_ref->table_name = edge_table->source_reference;
		// src_vertex_ref->alias = prev_binding;


		// cross_join_src_dst->left = std::move(src_vertex_ref);

		//! dst alias (FROM dst b)
		// auto dst_vertex_ref = make_uniq<BaseTableRef>();
		// dst_vertex_ref->table_name = edge_table->destination_reference;
		// dst_vertex_ref->alias = next_binding;

		// cross_join_src_dst->right = std::move(dst_vertex_ref);

		//! (SELECT count(cte1.temp) * 0 as temp from cte1) __x
		// auto cross_join_with_cte = make_uniq<JoinRef>(JoinRefType::CROSS);
		// cross_join_with_cte->left = std::move(temp_cte_select_subquery);
		// cross_join_with_cte->right = std::move(cross_join_src_dst);

		auto temp_cte_select_subquery = CreateCountCTESubquery();
		// from_clause = std::move(temp_cte_select_subquery);

		if (from_clause) {
			// create a cross join since there is already something in the
			// from clause
			auto from_join = make_uniq<JoinRef>(JoinRefType::CROSS);
			from_join->left = std::move(from_clause);
			from_join->right = std::move(temp_cte_select_subquery);
			from_clause = std::move(from_join);
		} else {
			from_clause = std::move(temp_cte_select_subquery);
		}
		//! END
		//! FROM (SELECT count(cte1.temp) * 0 as temp from cte1) __x

		//! START
		//! WHERE __x.temp + iterativelength(<csr_id>, (SELECT count(c.id)
		//! from dst c, a.rowid, b.rowid) between lower and upper

		auto src_row_id = make_uniq<ColumnRefExpression>(
			"rowid", prev_binding);
		auto dst_row_id = make_uniq<ColumnRefExpression>(
			"rowid", next_binding);
		auto csr_id =
				make_uniq<ConstantExpression>(Value::INTEGER(0));

		vector<unique_ptr<ParsedExpression>> pathfinding_children;
		pathfinding_children.push_back(std::move(csr_id));
		pathfinding_children.push_back(std::move(GetCountTable(
			edge_table, prev_binding)));
		pathfinding_children.push_back(std::move(src_row_id));
		pathfinding_children.push_back(std::move(dst_row_id));

		auto reachability_function = make_uniq<FunctionExpression>(
			"iterativelength", std::move(pathfinding_children));

		auto cte_col_ref = make_uniq<ColumnRefExpression>("temp", "__x");

		vector<unique_ptr<ParsedExpression>> addition_children;
		addition_children.push_back(std::move(cte_col_ref));
		addition_children.push_back(std::move(reachability_function));

		auto addition_function = make_uniq<FunctionExpression>(
			"add", std::move(addition_children));
		auto lower_limit =
				make_uniq<ConstantExpression>(Value::INTEGER(static_cast<int32_t>(subpath->lower)));
		auto upper_limit =
				make_uniq<ConstantExpression>(Value::INTEGER(static_cast<int32_t>(subpath->upper)));
		auto between_expression = make_uniq<BetweenExpression>(
			std::move(addition_function), std::move(lower_limit),
			std::move(upper_limit));
		conditions.push_back(std::move(between_expression));

		//! END
		//! WHERE __x.temp + iterativelength(<csr_id>, (SELECT count(s.id)
		//! from src s, a.rowid, b.rowid) between lower and upper
	}

	bool PGQMatchFunction::CheckNamedSubpath(SubPath& subpath, vector<unique_ptr<ParsedExpression>>& column_list,
	                                         CreatePropertyGraphInfo& pg_table) {
		bool found = false;
		idx_t idx_named_subpath = 0;
		for (idx_t idx_i = 0; idx_i < column_list.size(); idx_i++) {
			const FunctionExpression* parsed_ref = dynamic_cast<FunctionExpression *>(column_list[idx_i].get());
			if (parsed_ref == nullptr) {
				continue;
			}
			if (parsed_ref->function_name == "element_id") {
				// Check subpath name matches the function name
				auto column_ref = dynamic_cast<ColumnRefExpression*>(parsed_ref->children[0].get());
				if (column_ref->column_names[0] == subpath.path_variable) {
					idx_named_subpath = idx_i;
					found = true;
					break;
				}
			}
		}
		if (found) {
			column_list.erase(column_list.begin() + idx_named_subpath);
			auto shortest_path_function = CreatePathFindingFunction(subpath.path_list, pg_table);
			shortest_path_function->alias = subpath.path_variable;
			column_list.insert(column_list.begin() + idx_named_subpath, std::move(shortest_path_function));
		}
		return found;
	}

	void PGQMatchFunction::ProcessPathList(vector<unique_ptr<PathReference>>& path_list,
	                                       vector<unique_ptr<ParsedExpression>>& conditions,
	                                       unique_ptr<TableRef>& from_clause, unique_ptr<SelectNode>& select_node,
	                                       unordered_map<string, string>& alias_map,
	                                       CreatePropertyGraphInfo& pg_table, int32_t& extra_alias_counter,
	                                       vector<unique_ptr<ParsedExpression>>& column_list) {
		PathElement* previous_vertex_element =
				GetPathElement(path_list[0]);
		if (!previous_vertex_element) {
			// todo(dtenwolde) handle named subpaths.
			const auto previous_vertex_subpath = reinterpret_cast<SubPath *>(path_list[0].get());
			if (previous_vertex_subpath->where_clause) {
				conditions.push_back(std::move(previous_vertex_subpath->where_clause));
			}
			if (previous_vertex_subpath->path_list.size() == 1) {
				previous_vertex_element = GetPathElement(previous_vertex_subpath->path_list[0]);
			} else {
				// Add the shortest path if the name is found in the column_list
				CheckNamedSubpath(*previous_vertex_subpath, column_list, pg_table);
				ProcessPathList(previous_vertex_subpath->path_list, conditions, from_clause, select_node,
				                alias_map, pg_table, extra_alias_counter, column_list);
				return;
			}
		}
		auto previous_vertex_table =
				FindGraphTable(previous_vertex_element->label, pg_table);
		CheckInheritance(previous_vertex_table, previous_vertex_element,
		                 conditions);
		alias_map[previous_vertex_element->variable_binding] =
				previous_vertex_table->table_name;

		for (idx_t idx_j = 1;
		     idx_j < path_list.size();
		     idx_j = idx_j + 2) {
			PathElement* next_vertex_element =
					GetPathElement(path_list[idx_j + 1]);
			if (!next_vertex_element) {
				auto next_vertex_subpath =
						reinterpret_cast<SubPath *>(path_list[idx_j + 1].get());
				if (next_vertex_subpath->path_list.size() > 1) {
					throw NotImplementedException("Recursive patterns are not yet supported.");
				}

				//	Check the size of the subpath path list
				//	if size == 1:
				//		Path Element with a WHERE
				//		(){3} Repeated vertices are not supported
				//	Else:
				//		Unsure if this is possible to reach. Perhaps at some point with a nested pattern?
				//		Will be unsupported for now
				if (next_vertex_subpath->where_clause) {
					conditions.push_back(std::move(next_vertex_subpath->where_clause));
				}
				next_vertex_element =
						GetPathElement(next_vertex_subpath->path_list[0]);
			}
			if (next_vertex_element->match_type != PGQMatchType::MATCH_VERTEX ||
			    previous_vertex_element->match_type != PGQMatchType::MATCH_VERTEX) {
				throw BinderException("Vertex and edge patterns must be alternated.");
			}
			auto next_vertex_table =
					FindGraphTable(next_vertex_element->label, pg_table);
			CheckInheritance(next_vertex_table, next_vertex_element, conditions);
			alias_map[next_vertex_element->variable_binding] = next_vertex_table->table_name;

			PathElement* edge_element =
					GetPathElement(path_list[idx_j]);
			if (!edge_element) {
				// We are dealing with a subpath
				auto edge_subpath = reinterpret_cast<SubPath *>(path_list[idx_j].get());
				if (edge_subpath->where_clause) {
					conditions.push_back(std::move(edge_subpath->where_clause));
				}
				if (edge_subpath->path_list.size() > 1) {
					// todo(dtenwolde) deal with multiple elements in subpath
					throw NotImplementedException("Subpath on an edge is not yet supported.");
				}
				edge_element = GetPathElement(edge_subpath->path_list[0]);
				auto edge_table = FindGraphTable(edge_element->label, pg_table);

				if (edge_subpath->upper > 1) {
					// Add the path-finding
					AddPathFinding(select_node, from_clause, conditions,
					               previous_vertex_element->variable_binding,
					               edge_element->variable_binding,
					               next_vertex_element->variable_binding,
					               edge_table, edge_subpath);
				} else {
					alias_map[edge_element->variable_binding] = edge_table->source_reference;
					AddEdgeJoins(select_node, edge_table, previous_vertex_table,
					             next_vertex_table, edge_element->match_type,
					             edge_element->variable_binding, previous_vertex_element->variable_binding,
					             next_vertex_element->variable_binding, conditions, alias_map, extra_alias_counter);
				}
			} else {
				// The edge element is a path element without WHERE or path-finding.
				auto edge_table = FindGraphTable(edge_element->label, pg_table);
				CheckInheritance(edge_table, edge_element, conditions);
				// check aliases
				alias_map[edge_element->variable_binding] = edge_table->table_name;
				AddEdgeJoins(select_node, edge_table, previous_vertex_table,
				             next_vertex_table, edge_element->match_type, edge_element->variable_binding,
				             previous_vertex_element->variable_binding, next_vertex_element->variable_binding,
				             conditions, alias_map, extra_alias_counter);
				// Check the edge type
				// If (a)-[b]->(c) 	-> 	b.src = a.id AND b.dst = c.id
				// If (a)<-[b]-(c) 	-> 	b.dst = a.id AND b.src = c.id
				// If (a)-[b]-(c)  	-> 	(b.src = a.id AND b.dst = c.id) OR
				// 						(b.dst = a.id AND b.src
				// = c.id) If (a)<-[b]->(c)	->  (b.src = a.id AND b.dst = c.id) AND
				//						(b.dst = a.id AND b.src
				//= c.id)
			}
			previous_vertex_element = next_vertex_element;
			previous_vertex_table = next_vertex_table;
		}
	}


	unique_ptr<TableRef> PGQMatchFunction::MatchBindReplace(ClientContext& context,
	                                                        TableFunctionBindInput&) {
		auto duckpgq_state_entry = context.registered_state.find("duckpgq");
		auto duckpgq_state = dynamic_cast<DuckPGQState *>(duckpgq_state_entry->second.get());

		auto ref = dynamic_cast<MatchExpression *>(
			duckpgq_state->transform_expression.get());
		auto pg_table = duckpgq_state->GetPropertyGraph(ref->pg_name);

		auto data = make_uniq<MatchBindData>();

		vector<unique_ptr<ParsedExpression>> conditions;

		auto select_node = make_uniq<SelectNode>();
		unordered_map<string, string> alias_map;
		unique_ptr<TableRef> from_clause;

		int32_t extra_alias_counter = 0;
		for (idx_t idx_i = 0; idx_i < ref->path_patterns.size(); idx_i++) {
			auto& path_pattern = ref->path_patterns[idx_i];
			// Check if the element is PathElement or a Subpath with potentially many items
			ProcessPathList(path_pattern->path_elements, conditions, from_clause, select_node,
			                alias_map, *pg_table, extra_alias_counter, ref->column_list);
		}

		// Go through all aliases encountered
		for (auto& table_alias_entry: alias_map) {
			auto table_ref = make_uniq<BaseTableRef>();
			table_ref->table_name = table_alias_entry.second;
			table_ref->alias = table_alias_entry.first;

			if (from_clause) {
				auto new_root = make_uniq<JoinRef>(JoinRefType::CROSS);
				new_root->left = std::move(from_clause);
				new_root->right = std::move(table_ref);
				from_clause = std::move(new_root);
			} else {
				from_clause = std::move(table_ref);
			}
		}

		select_node->from_table = std::move(from_clause);

		if (ref->where_clause) {
			conditions.push_back(std::move(ref->where_clause));
		}
		std::vector<unique_ptr<ParsedExpression>> final_column_list;

		for (auto& expression: ref->column_list) {
			unordered_set<string> named_subpaths;
			auto column_ref = dynamic_cast<ColumnRefExpression *>(expression.get());
			if (column_ref != nullptr) {
				if (named_subpaths.count(column_ref->column_names[0]) &&
				    column_ref->column_names.size() == 1) {
					final_column_list.emplace_back(make_uniq<ColumnRefExpression>(
						"path", column_ref->column_names[0]));
				} else {
					final_column_list.push_back(std::move(expression));
				}
				continue;
			}
			auto function_ref = dynamic_cast<FunctionExpression *>(expression.get());
			if (function_ref != nullptr) {
				if (function_ref->function_name == "path_length") {
					column_ref = dynamic_cast<ColumnRefExpression *>(
						function_ref->children[0].get());
					if (column_ref == nullptr) {
						continue;
					}
					if (named_subpaths.count(column_ref->column_names[0]) &&
					    column_ref->column_names.size() == 1) {
						auto path_ref = make_uniq<ColumnRefExpression>(
							"path", column_ref->column_names[0]);
						vector<unique_ptr<ParsedExpression>> path_children;
						path_children.push_back(std::move(path_ref));
						auto path_len =
								make_uniq<FunctionExpression>("len", std::move(path_children));
						auto constant_two = make_uniq<ConstantExpression>(Value::INTEGER(2));
						vector<unique_ptr<ParsedExpression>> div_children;
						div_children.push_back(std::move(path_len));
						div_children.push_back(std::move(constant_two));
						auto div_expression =
								make_uniq<FunctionExpression>("//", std::move(div_children));
						div_expression->alias =
								"path_length_" + column_ref->column_names[0];
						final_column_list.emplace_back(std::move(div_expression));
					}
				} else {
					final_column_list.push_back(std::move(expression));
				}

				continue;
			}

			final_column_list.push_back(std::move(expression));
		}

		select_node->where_clause = CreateWhereClause(conditions);
		select_node->select_list = std::move(final_column_list);

		auto subquery = make_uniq<SelectStatement>();
		subquery->node = std::move(select_node);

		auto result = make_uniq<SubqueryRef>(std::move(subquery), ref->alias);

		return std::move(result);
	}

	//
	//unique_ptr<SubqueryRef> PGQMatchFunction::GenerateSubpathPatternSubquery(
	//    unique_ptr<PathPattern> &path_pattern, CreatePropertyGraphInfo *pg_table,
	//    vector<unique_ptr<ParsedExpression>> &column_list,
	//    unordered_set<string> &named_subpaths) {
	//  vector<unique_ptr<ParsedExpression>> conditions;
	//  auto path_element =
	//      reinterpret_cast<SubPath *>(path_pattern->path_elements[0].get());
	//  auto select_node = make_uniq<SelectNode>();
	//  unordered_map<string, string> alias_map;
	//  string named_subpath = path_element->path_variable;
	//  named_subpaths.insert(named_subpath);
	//  int32_t extra_alias_counter = 0;
	//  bool path_finding = false;
	//  auto previous_vertex_element =
	//      GetPathElement(path_element->path_list[0], conditions);
	//  auto previous_vertex_table =
	//      FindGraphTable(previous_vertex_element->label, *pg_table);
	//  CheckInheritance(previous_vertex_table, previous_vertex_element, conditions);
	//  alias_map[previous_vertex_element->variable_binding] =
	//      previous_vertex_table->table_name;
	//  for (idx_t idx_j = 1; idx_j < path_element->path_list.size();
	//       idx_j = idx_j + 2) {
	//    PathElement *edge_element =
	//        GetPathElement(path_element->path_list[idx_j], conditions);
	//    PathElement *next_vertex_element =
	//        GetPathElement(path_element->path_list[idx_j + 1], conditions);
	//    if (next_vertex_element->match_type != PGQMatchType::MATCH_VERTEX ||
	//        previous_vertex_element->match_type != PGQMatchType::MATCH_VERTEX) {
	//      throw BinderException("Vertex and edge patterns must be alternated.");
	//    }
	//
	//    auto edge_table = FindGraphTable(edge_element->label, *pg_table);
	//    CheckInheritance(edge_table, edge_element, conditions);
	//    auto next_vertex_table =
	//        FindGraphTable(next_vertex_element->label, *pg_table);
	//    CheckInheritance(next_vertex_table, next_vertex_element, conditions);
	//
	//    if (path_element->path_list[idx_j]->path_reference_type ==
	//        PGQPathReferenceType::SUBPATH) {
	//      auto *subpath =
	//          reinterpret_cast<SubPath *>(path_element->path_list[idx_j].get());
	//      if (subpath->upper > 1) {
	//        path_finding = true;
	//        if (!named_subpath.empty() && path_pattern->shortest) {
	//          // todo(dtenwolde) does not necessarily have to be a shortest path
	//          // query if it is a named subpath. It can also be a basic pattern
	//          // matching that is named.
	//          auto shortest_path_function = CreatePathFindingFunction(
	//              previous_vertex_element->variable_binding,
	//              next_vertex_element->variable_binding, edge_table,
	//              "shortestpath");
	//          shortest_path_function->alias = "path";
	//          select_node->select_list.push_back(std::move(shortest_path_function));
	//        }
	//        select_node->cte_map.map["cte1"] =
	//            CreateCSRCTE(edge_table, previous_vertex_element->variable_binding,
	//                         edge_element->variable_binding,
	//                         next_vertex_element->variable_binding);
	//
	//        //! (SELECT count(cte1.temp) * 0 as temp from cte1) __x
	//        auto temp_cte_select_subquery = CreateCountCTESubquery();
	//
	//        auto cross_join_src_dst = make_uniq<JoinRef>(JoinRefType::CROSS);
	//
	//        //! src alias (FROM src a)
	//        auto src_vertex_ref = make_uniq<BaseTableRef>();
	//        src_vertex_ref->table_name = edge_table->source_reference;
	//        src_vertex_ref->alias = previous_vertex_element->variable_binding;
	//
	//        cross_join_src_dst->left = std::move(src_vertex_ref);
	//
	//        //! dst alias (FROM dst b)
	//        auto dst_vertex_ref = make_uniq<BaseTableRef>();
	//        dst_vertex_ref->table_name = edge_table->destination_reference;
	//        dst_vertex_ref->alias = next_vertex_element->variable_binding;
	//
	//        cross_join_src_dst->right = std::move(dst_vertex_ref);
	//
	//        auto cross_join_with_cte = make_uniq<JoinRef>(JoinRefType::CROSS);
	//        cross_join_with_cte->left = std::move(temp_cte_select_subquery);
	//        cross_join_with_cte->right = std::move(cross_join_src_dst);
	//
	//        if (select_node->from_table) {
	//          // create a cross join since there is already something in the from
	//          // clause
	//          auto from_join = make_uniq<JoinRef>(JoinRefType::CROSS);
	//          from_join->left = std::move(select_node->from_table);
	//          from_join->right = std::move(cross_join_with_cte);
	//          select_node->from_table = std::move(from_join);
	//        } else {
	//          select_node->from_table = std::move(cross_join_with_cte);
	//        }
	//        //! END
	//        //! FROM (SELECT count(cte1.temp) * 0 as temp from cte1) __x, src a, dst b
	//
	//        //! START
	//        //! WHERE __x.temp + iterativelength(<csr_id>, (SELECT count(c.id) from
	//        //! dst c, a.rowid, b.rowid) between lower and upper
	//        auto reachability_function =
	//            CreatePathFindingFunction(previous_vertex_element->variable_binding,
	//                                      next_vertex_element->variable_binding,
	//                                      edge_table, "iterativelength");
	//
	//        auto cte_col_ref = make_uniq<ColumnRefExpression>("temp", "__x");
	//
	//        vector<unique_ptr<ParsedExpression>> addition_children;
	//        addition_children.push_back(std::move(cte_col_ref));
	//        addition_children.push_back(std::move(reachability_function));
	//
	//        auto addition_function =
	//            make_uniq<FunctionExpression>("add", std::move(addition_children));
	//        auto lower_limit =
	//            make_uniq<ConstantExpression>(Value::BIGINT(subpath->lower));
	//        auto upper_limit =
	//            make_uniq<ConstantExpression>(Value::BIGINT(subpath->upper));
	//        auto between_expression = make_uniq<BetweenExpression>(
	//            std::move(addition_function), std::move(lower_limit),
	//            std::move(upper_limit));
	//        conditions.push_back(std::move(between_expression));
	//
	//        //! END
	//        //! WHERE __x.temp + iterativelength(<csr_id>, (SELECT count(s.id) from
	//        //! src s, a.rowid, b.rowid) between lower and upper
	//      }
	//      // check aliases
	//      alias_map[next_vertex_element->variable_binding] =
	//          next_vertex_table->table_name;
	//      alias_map[edge_element->variable_binding] = edge_table->table_name;
	//      if (!path_finding) {
	//        switch (edge_element->match_type) {
	//        case PGQMatchType::MATCH_EDGE_ANY: {
	//          select_node->modifiers.push_back(make_uniq<DistinctModifier>());
	//          EdgeTypeAny(edge_table, edge_element->variable_binding,
	//                      previous_vertex_element->variable_binding,
	//                      next_vertex_element->variable_binding, conditions);
	//          break;
	//        }
	//        case PGQMatchType::MATCH_EDGE_LEFT:
	//          EdgeTypeLeft(edge_table, next_vertex_table->table_name,
	//                       previous_vertex_table->table_name,
	//                       edge_element->variable_binding,
	//                       previous_vertex_element->variable_binding,
	//                       next_vertex_element->variable_binding, conditions);
	//          break;
	//        case PGQMatchType::MATCH_EDGE_RIGHT:
	//          EdgeTypeRight(edge_table, next_vertex_table->table_name,
	//                        previous_vertex_table->table_name,
	//                        edge_element->variable_binding,
	//                        previous_vertex_element->variable_binding,
	//                        next_vertex_element->variable_binding, conditions);
	//          break;
	//        case PGQMatchType::MATCH_EDGE_LEFT_RIGHT: {
	//          EdgeTypeLeftRight(edge_table, edge_element->variable_binding,
	//                            previous_vertex_element->variable_binding,
	//                            next_vertex_element->variable_binding, conditions,
	//                            alias_map, extra_alias_counter);
	//          break;
	//        }
	//        default:
	//          throw InternalException("Unknown match type found");
	//        }
	//      }
	//      previous_vertex_element = next_vertex_element;
	//      previous_vertex_table = next_vertex_table;
	//    }
	//  }
	//
	//  select_node->where_clause = CreateWhereClause(conditions);
	//  vector<unique_ptr<ParsedExpression>> substitute_column_list;
	//  for (auto &expression : column_list) {
	//    const auto &column_ref =
	//        dynamic_cast<ColumnRefExpression *>(expression.get());
	//    if (column_ref == nullptr) {
	//      continue;
	//    }
	//    // If the table is referenced in this subquery (count() > 0)
	//    if (alias_map.count(column_ref->column_names[0])) {
	//      select_node->select_list.push_back(std::move(expression));
	//      // Create a substitute
	//      unique_ptr<ColumnRefExpression> new_upper_column_ref;
	//      if (column_ref->alias.empty()) {
	//        new_upper_column_ref = make_uniq<ColumnRefExpression>(
	//            column_ref->column_names[1], named_subpath);
	//      } else {
	//        new_upper_column_ref =
	//            make_uniq<ColumnRefExpression>(column_ref->alias, named_subpath);
	//      }
	//      new_upper_column_ref->alias = column_ref->alias;
	//      substitute_column_list.push_back(std::move(new_upper_column_ref));
	//    }
	//  }
	//  // Remove the elements from the original column_list that are now NULL
	//  for (auto it = column_list.begin(); it != column_list.end();) {
	//    if (!*it) {
	//      it = column_list.erase(it);
	//    } else {
	//      ++it;
	//    }
	//  }
	//  // Add the ColumnRefs that were previously moved to the subquery with the
	//  // subquery name as table_name
	//  for (auto &expression : substitute_column_list) {
	//    column_list.push_back(std::move(expression));
	//  }
	//  auto subquery = make_uniq<SelectStatement>();
	//  subquery->node = std::move(select_node);
	//
	//  return make_uniq<SubqueryRef>(std::move(subquery), named_subpath);
	//}
} // namespace duckdb
