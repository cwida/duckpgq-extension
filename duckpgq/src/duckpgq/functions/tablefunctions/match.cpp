#include "duckpgq/functions/tablefunctions/match.hpp"
#include <duckpgq_extension.hpp>

#include "duckdb/parser/tableref/matchref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"

#include "duckdb/parser/query_node/select_node.hpp"

#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"

namespace duckdb {

		shared_ptr<PropertyGraphTable>
		MatchFunction::FindGraphTable(const string &label,
																	CreatePropertyGraphInfo &pg_table) {
			auto graph_table_entry = pg_table.label_map.find(label);
			if (graph_table_entry == pg_table.label_map.end()) {
				throw BinderException("The label %s is not registered in property graph %s",
															label, pg_table.property_graph_name);
			}

			return graph_table_entry->second;
		}

		void MatchFunction::CheckInheritance(
						shared_ptr<PropertyGraphTable> &tableref, PathElement *element,
						vector<unique_ptr<ParsedExpression>> &conditions) {
			if (tableref->main_label == element->label) {
				return;
			}
			auto constant_expression_two =
							make_uniq<ConstantExpression>(Value::INTEGER((int32_t) 2));
			auto itr = std::find(tableref->sub_labels.begin(), tableref->sub_labels.end(),
													 element->label);

			auto idx_of_element = std::distance(tableref->sub_labels.begin(), itr);
			auto constant_expression_idx_label =
							make_uniq<ConstantExpression>(Value::INTEGER((int32_t) idx_of_element));

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
							Value::INTEGER((int32_t) idx_of_element + 1));

			auto subset_compare = make_uniq<ComparisonExpression>(
							ExpressionType::COMPARE_EQUAL, std::move(and_expression),
							std::move(constant_expression_idx_label_comparison));
			conditions.push_back(std::move(subset_compare));
		}

		void MatchFunction::CheckEdgeTableConstraints(
						const string &src_reference, const string &dst_reference,
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

		unique_ptr<ParsedExpression> MatchFunction::CreateMatchJoinExpression(
						vector<string> vertex_keys, vector<string> edge_keys,
						const string &vertex_alias, const string &edge_alias) {
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

			for (auto &condition: conditions) {
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

		PathElement *MatchFunction::GetPathElement(
						unique_ptr<PathReference> &path_reference,
						vector<unique_ptr<ParsedExpression>> &conditions) {
			if (path_reference->path_reference_type ==
					PGQPathReferenceType::PATH_ELEMENT) {
				return reinterpret_cast<PathElement *>(path_reference.get());
			} else if (path_reference->path_reference_type ==
								 PGQPathReferenceType::SUBPATH) {
				auto subpath = reinterpret_cast<SubPath *>(path_reference.get());

				if (subpath->where_clause) {
					conditions.push_back(std::move(subpath->where_clause));
				}
				// If the subpath has only one element (the case when there is a WHERE in the element)
				// we unpack the subpath into a PathElement.
				if (subpath->path_list.size() == 1) {
					return reinterpret_cast<PathElement *>(subpath->path_list[0].get());
				} else {
					return nullptr;
				}
			} else {
				throw InternalException("Unknown path reference type detected");
			}
		}

		unique_ptr<SubqueryExpression>
		MatchFunction::GetCountTable(const shared_ptr<PropertyGraphTable> &edge_table,
																 const string &prev_binding) {
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
		MatchFunction::GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table,
															const string &edge_binding,
															const string &prev_binding,
															const string &next_binding) {
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

		unique_ptr<SubqueryRef> MatchFunction::CreateCountCTESubquery() {
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
		MatchFunction::CreateCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
																const string &edge_binding,
																const string &prev_binding,
																const string &next_binding) {
			auto csr_edge_id_constant =
							make_uniq<ConstantExpression>(Value::INTEGER((int32_t) 0));
			auto count_create_edge_select = GetCountTable(edge_table, prev_binding);

			auto cast_subquery_expr = make_uniq<SubqueryExpression>();
			auto cast_select_node = make_uniq<SelectNode>();

			vector<unique_ptr<ParsedExpression>> csr_vertex_children;
			csr_vertex_children.push_back(
							make_uniq<ConstantExpression>(Value::INTEGER((int32_t) 0)));

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

		void MatchFunction::EdgeTypeAny(
						shared_ptr<PropertyGraphTable> &edge_table,
						const string &edge_binding,
						const string &prev_binding,
						const string &next_binding,
						vector<unique_ptr<ParsedExpression>> &conditions) {
			auto src_left_expr = CreateMatchJoinExpression(
							edge_table->source_pk, edge_table->source_fk,
							prev_binding,
							edge_binding);
			auto dst_left_expr = CreateMatchJoinExpression(
							edge_table->destination_pk, edge_table->destination_fk,
							next_binding,
							edge_binding);

			auto combined_left_expr = make_uniq<ConjunctionExpression>(
							ExpressionType::CONJUNCTION_AND, std::move(src_left_expr),
							std::move(dst_left_expr));

			auto src_right_expr = CreateMatchJoinExpression(
							edge_table->source_pk, edge_table->destination_fk,
							prev_binding,
							edge_binding);
			auto dst_right_expr = CreateMatchJoinExpression(
							edge_table->destination_pk, edge_table->source_fk,
							next_binding,
							edge_binding);
			auto combined_right_expr = make_uniq<ConjunctionExpression>(
							ExpressionType::CONJUNCTION_AND, std::move(src_right_expr),
							std::move(dst_right_expr));

			auto combined_expr = make_uniq<ConjunctionExpression>(
							ExpressionType::CONJUNCTION_OR, std::move(combined_left_expr),
							std::move(combined_right_expr));
			conditions.push_back(std::move(combined_expr));
		}

		void MatchFunction::EdgeTypeLeft(shared_ptr<PropertyGraphTable> &edge_table,
																		 const string &next_table_name, const string &prev_table_name,
																		 const string &edge_binding,
																		 const string &prev_binding,
																		 const string &next_binding,
																		 vector<unique_ptr<ParsedExpression>> &conditions) {
			CheckEdgeTableConstraints(next_table_name,
																prev_table_name,
																edge_table);
			conditions.push_back(CreateMatchJoinExpression(
							edge_table->source_pk, edge_table->source_fk,
							next_binding,
							edge_binding));
			conditions.push_back(CreateMatchJoinExpression(
							edge_table->destination_pk, edge_table->destination_fk,
							prev_binding,
							edge_binding));
		}

		void MatchFunction::EdgeTypeRight(shared_ptr<PropertyGraphTable> &edge_table,
																			const string &next_table_name, const string &prev_table_name,
																			const string &edge_binding,
																			const string &prev_binding,
																			const string &next_binding,
																			vector<unique_ptr<ParsedExpression>> &conditions) {
			CheckEdgeTableConstraints(prev_table_name,
																next_table_name, edge_table);
			conditions.push_back(CreateMatchJoinExpression(
							edge_table->source_pk, edge_table->source_fk,
							prev_binding,
							edge_binding));
			conditions.push_back(CreateMatchJoinExpression(
							edge_table->destination_pk, edge_table->destination_fk,
							next_binding,
							edge_binding));
		}

		void MatchFunction::EdgeTypeLeftRight(shared_ptr<PropertyGraphTable> &edge_table,
																					const string &edge_binding,
																					const string &prev_binding,
																					const string &next_binding,
																					vector<unique_ptr<ParsedExpression>> &conditions,
																					unordered_map<string, string> &alias_map,
																					int32_t &extra_alias_counter) {
			auto src_left_expr = CreateMatchJoinExpression(
							edge_table->source_pk, edge_table->source_fk,
							next_binding,
							edge_binding);
			auto dst_left_expr = CreateMatchJoinExpression(
							edge_table->destination_pk, edge_table->destination_fk,
							prev_binding,
							edge_binding);

			auto combined_left_expr = make_uniq<ConjunctionExpression>(
							ExpressionType::CONJUNCTION_AND, std::move(src_left_expr),
							std::move(dst_left_expr));

			auto additional_edge_alias = edge_binding +
																	 std::to_string(extra_alias_counter);
			extra_alias_counter++;

			alias_map[additional_edge_alias] = edge_table->table_name;

			auto src_right_expr = CreateMatchJoinExpression(
							edge_table->source_pk, edge_table->source_fk,
							prev_binding, additional_edge_alias);
			auto dst_right_expr = CreateMatchJoinExpression(
							edge_table->destination_pk, edge_table->destination_fk,
							next_binding, additional_edge_alias);
			auto combined_right_expr = make_uniq<ConjunctionExpression>(
							ExpressionType::CONJUNCTION_AND, std::move(src_right_expr),
							std::move(dst_right_expr));

			auto combined_expr = make_uniq<ConjunctionExpression>(
							ExpressionType::CONJUNCTION_AND, std::move(combined_left_expr),
							std::move(combined_right_expr));
			conditions.push_back(std::move(combined_expr));
		}

		PathElement* MatchFunction::HandleNestedSubPath(unique_ptr<PathReference> &path_reference, vector<unique_ptr<ParsedExpression>> &conditions, idx_t element_idx) {
			auto subpath = reinterpret_cast<SubPath*>(path_reference.get());
			return GetPathElement(subpath->path_list[element_idx], conditions);
		}
		unique_ptr<TableRef>
		MatchFunction::MatchBindReplace(ClientContext &context,
																		TableFunctionBindInput &) {
			auto data = make_uniq<MatchFunction::MatchBindData>();
			auto duckpgq_state_entry = context.registered_state.find("duckpgq");
			auto duckpgq_state = (DuckPGQState *) duckpgq_state_entry->second.get();

			auto ref = dynamic_cast<MatchExpression *>(
							duckpgq_state->transform_expression.get());
			auto pg_table = duckpgq_state->GetPropertyGraph(ref->pg_name);


			vector<unique_ptr<ParsedExpression>> conditions;

			auto select_node = make_uniq<SelectNode>();
			unordered_map<string, string> alias_map;

			int32_t extra_alias_counter = 0;
			bool path_finding = false;
			string named_subpath;
			for (idx_t idx_i = 0; idx_i < ref->path_list.size(); idx_i++) {
				auto &path_list = ref->path_list[idx_i];
				// Check if the element is PathElement or a Subpath with potentially many
				// items
				PathElement *previous_vertex_element =
								GetPathElement(path_list->path_elements[0], conditions);
				if (!previous_vertex_element) {
					auto subpath = reinterpret_cast<SubPath*>(path_list->path_elements[0].get());
					previous_vertex_element = GetPathElement(subpath->path_list[0], conditions);
					auto subpath_subquery = GenerateSubpathSubquery(subpath, previous_vertex_element, pg_table);
				}
				auto previous_vertex_table =
								FindGraphTable(previous_vertex_element->label, *pg_table);
				CheckInheritance(previous_vertex_table, previous_vertex_element,
												 conditions);
				alias_map[previous_vertex_element->variable_binding] =
								previous_vertex_table->table_name;

				for (idx_t idx_j = 1; idx_j < ref->path_list[idx_i]->path_elements.size();
						 idx_j = idx_j + 2) {
					PathElement *edge_element =
									GetPathElement(path_list->path_elements[idx_j], conditions);
					if (!edge_element) {
						auto subpath = reinterpret_cast<SubPath*>(path_list->path_elements[0].get());
						edge_element = GetPathElement(subpath->path_list[idx_j], conditions);
					}
					PathElement *next_vertex_element =
									GetPathElement(path_list->path_elements[idx_j + 1], conditions);
					if (!next_vertex_element) {
						auto subpath = reinterpret_cast<SubPath*>(path_list->path_elements[0].get());
						next_vertex_element = GetPathElement(subpath->path_list[idx_j + 1], conditions);
					}
					if (next_vertex_element->match_type != PGQMatchType::MATCH_VERTEX ||
							previous_vertex_element->match_type != PGQMatchType::MATCH_VERTEX) {
						throw BinderException("Vertex and edge patterns must be alternated.");
					}

					auto edge_table = FindGraphTable(edge_element->label, *pg_table);
					CheckInheritance(edge_table, edge_element, conditions);
					auto next_vertex_table =
									FindGraphTable(next_vertex_element->label, *pg_table);
					CheckInheritance(next_vertex_table, next_vertex_element, conditions);

					if (path_list->path_elements[idx_j]->path_reference_type == PGQPathReferenceType::SUBPATH) {
						auto *subpath =
										reinterpret_cast<SubPath *>(path_list->path_elements[idx_j].get());
						if (subpath->upper > 1) {
							path_finding = true;

							//! START
							//! FROM (SELECT count(cte1.temp) * 0 as temp from cte1) __x, src a, dst b
							select_node->cte_map.map["cte1"] = CreateCSRCTE(
											edge_table, previous_vertex_element->variable_binding,
											edge_element->variable_binding,
											next_vertex_element->variable_binding);

							//! (SELECT count(cte1.temp) * 0 as temp from cte1) __x
							auto temp_cte_select_subquery = CreateCountCTESubquery();

							auto cross_join_src_dst = make_uniq<JoinRef>(JoinRefType::CROSS);

							//! src alias (FROM src a)
							auto src_vertex_ref = make_uniq<BaseTableRef>();
							src_vertex_ref->table_name = edge_table->source_reference;
							src_vertex_ref->alias = previous_vertex_element->variable_binding;

							cross_join_src_dst->left = std::move(src_vertex_ref);

							//! dst alias (FROM dst b)
							auto dst_vertex_ref = make_uniq<BaseTableRef>();
							dst_vertex_ref->table_name = edge_table->destination_reference;
							dst_vertex_ref->alias = next_vertex_element->variable_binding;

							cross_join_src_dst->right = std::move(dst_vertex_ref);

							auto cross_join_with_cte = make_uniq<JoinRef>(JoinRefType::CROSS);
							cross_join_with_cte->left = std::move(temp_cte_select_subquery);
							cross_join_with_cte->right = std::move(cross_join_src_dst);

							if (select_node->from_table) {
								// create a cross join since there is already something in the from clause
								auto from_join = make_uniq<JoinRef>(JoinRefType::CROSS);
								from_join->left = std::move(select_node->from_table);
								from_join->right = std::move(cross_join_with_cte);
								select_node->from_table = std::move(from_join);
							} else {
								select_node->from_table = std::move(cross_join_with_cte);
							}
							//! END
							//! FROM (SELECT count(cte1.temp) * 0 as temp from cte1) __x, src a, dst b

							//! START
							//! WHERE __x.temp + iterativelength(<csr_id>, (SELECT count(c.id) from dst c, a.rowid, b.rowid) between lower and upper

							auto src_row_id = make_uniq<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
							auto dst_row_id = make_uniq<ColumnRefExpression>("rowid", next_vertex_element->variable_binding);
							auto csr_id = make_uniq<ConstantExpression>(Value::INTEGER((int32_t) 0));

							vector<unique_ptr<ParsedExpression>> pathfinding_children;
							pathfinding_children.push_back(std::move(csr_id));
							pathfinding_children.push_back(std::move(GetCountTable(edge_table, previous_vertex_element->variable_binding)));
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
											make_uniq<ConstantExpression>(Value::INTEGER(subpath->lower));
							auto upper_limit =
											make_uniq<ConstantExpression>(Value::INTEGER(subpath->upper));
							auto between_expression = make_uniq<BetweenExpression>(
											std::move(addition_function), std::move(lower_limit),
											std::move(upper_limit));
							conditions.push_back(std::move(between_expression));

							//! END
							//! WHERE __x.temp + iterativelength(<csr_id>, (SELECT count(s.id) from src s, a.rowid, b.rowid) between lower and upper

						}
					}
					// check aliases
					alias_map[next_vertex_element->variable_binding] =
									next_vertex_table->table_name;
					alias_map[edge_element->variable_binding] = edge_table->table_name;
					if (!path_finding) {
						switch (edge_element->match_type) {
							case PGQMatchType::MATCH_EDGE_ANY: {
								select_node->modifiers.push_back(make_uniq<DistinctModifier>());
								EdgeTypeAny(edge_table,
														edge_element->variable_binding,
														previous_vertex_element->variable_binding,
														next_vertex_element->variable_binding,
														conditions);
								break;
							}
							case PGQMatchType::MATCH_EDGE_LEFT:
								EdgeTypeLeft(edge_table, next_vertex_table->table_name, previous_vertex_table->table_name,
														 edge_element->variable_binding, previous_vertex_element->variable_binding,
														 next_vertex_element->variable_binding, conditions);
								break;
							case PGQMatchType::MATCH_EDGE_RIGHT:
								EdgeTypeRight(edge_table, next_vertex_table->table_name, previous_vertex_table->table_name,
															edge_element->variable_binding, previous_vertex_element->variable_binding,
															next_vertex_element->variable_binding, conditions);
								break;
							case PGQMatchType::MATCH_EDGE_LEFT_RIGHT: {
								EdgeTypeLeftRight(edge_table, edge_element->variable_binding, previous_vertex_element->variable_binding,
																	next_vertex_element->variable_binding, conditions, alias_map, extra_alias_counter);
								break;
							}

							default:
								throw InternalException("Unknown match type found");
						}
					}


					previous_vertex_element = next_vertex_element;
					previous_vertex_table = next_vertex_table;

					// Check the edge type
					// If (a)-[b]->(c) 	-> 	b.src = a.id AND b.dst = c.id
					// If (a)<-[b]-(c) 	-> 	b.dst = a.id AND b.src = c.id
					// If (a)-[b]-(c)  	-> 	(b.src = a.id AND b.dst = c.id) OR
					// 						(b.dst = a.id AND b.src
					// = c.id) If (a)<-[b]->(c)	->  (b.src = a.id AND b.dst = c.id) AND
					//						(b.dst = a.id AND b.src
					//= c.id)
				}
			}

			unique_ptr<TableRef> from_clause;

			if (!path_finding) {
				// Go through all aliases encountered
				for (auto &table_alias_entry: alias_map) {
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
			}

			if (ref->where_clause) {
				conditions.push_back(std::move(ref->where_clause));
			}

			unique_ptr<ParsedExpression> where_clause;

			for (auto &condition: conditions) {
				if (where_clause) {
					where_clause = make_uniq<ConjunctionExpression>(
									ExpressionType::CONJUNCTION_AND, std::move(where_clause),
									std::move(condition));
				} else {
					where_clause = std::move(condition);
				}
			}

			select_node->where_clause = std::move(where_clause);

			select_node->select_list = std::move(ref->column_list);

			auto subquery = make_uniq<SelectStatement>();
			subquery->node = std::move(select_node);

			auto result = make_uniq<SubqueryRef>(std::move(subquery), ref->alias);

			return std::move(result);
		}

		unique_ptr<SubqueryRef> MatchFunction::GenerateSubpathSubquery(SubPath *pPath, PathElement *pElement, CreatePropertyGraphInfo* pg_table) {
			vector<unique_ptr<ParsedExpression>> conditions;

			auto select_node = make_uniq<SelectNode>();
			unordered_map<string, string> alias_map;
			string named_subpath = pPath->path_variable;

			bool path_finding = false;

			auto previous_vertex_table =
							FindGraphTable(pElement->label, *pg_table);
			CheckInheritance(previous_vertex_table, pElement,
											 conditions);
			alias_map[pElement->variable_binding] =
							previous_vertex_table->table_name;
			for (idx_t idx_j = 1; idx_j < pPath->path_list.size();
					 idx_j = idx_j + 2) {
				PathElement *edge_element =
								GetPathElement(pPath->path_list[idx_j], conditions);
				PathElement *next_vertex_element =
								GetPathElement(pPath->path_list[idx_j + 1], conditions);
				if (next_vertex_element->match_type != PGQMatchType::MATCH_VERTEX ||
						pElement->match_type != PGQMatchType::MATCH_VERTEX) {
					throw BinderException("Vertex and edge patterns must be alternated.");
				}

				auto edge_table = FindGraphTable(edge_element->label, *pg_table);
				CheckInheritance(edge_table, edge_element, conditions);
				auto next_vertex_table =
								FindGraphTable(next_vertex_element->label, *pg_table);
				CheckInheritance(next_vertex_table, next_vertex_element, conditions);

				if (pPath->path_list[idx_j]->path_reference_type == PGQPathReferenceType::SUBPATH) {
					auto *subpath = reinterpret_cast<SubPath *>(pPath->path_list[idx_j].get());
					if (subpath->upper > 1) {
						path_finding = true;
						select_node->cte_map.map["cte1"] = CreateCSRCTE(
										edge_table, pElement->variable_binding,
										edge_element->variable_binding,
										next_vertex_element->variable_binding);

						//! (SELECT count(cte1.temp) * 0 as temp from cte1) __x
						auto temp_cte_select_subquery = CreateCountCTESubquery();

						auto cross_join_src_dst = make_uniq<JoinRef>(JoinRefType::CROSS);

						//! src alias (FROM src a)
						auto src_vertex_ref = make_uniq<BaseTableRef>();
						src_vertex_ref->table_name = edge_table->source_reference;
						src_vertex_ref->alias = pElement->variable_binding;

						cross_join_src_dst->left = std::move(src_vertex_ref);

						//! dst alias (FROM dst b)
						auto dst_vertex_ref = make_uniq<BaseTableRef>();
						dst_vertex_ref->table_name = edge_table->destination_reference;
						dst_vertex_ref->alias = next_vertex_element->variable_binding;

						cross_join_src_dst->right = std::move(dst_vertex_ref);

						auto cross_join_with_cte = make_uniq<JoinRef>(JoinRefType::CROSS);
						cross_join_with_cte->left = std::move(temp_cte_select_subquery);
						cross_join_with_cte->right = std::move(cross_join_src_dst);

						if (select_node->from_table) {
							// create a cross join since there is already something in the from clause
							auto from_join = make_uniq<JoinRef>(JoinRefType::CROSS);
							from_join->left = std::move(select_node->from_table);
							from_join->right = std::move(cross_join_with_cte);
							select_node->from_table = std::move(from_join);
						} else {
							select_node->from_table = std::move(cross_join_with_cte);
						}
						//! END
						//! FROM (SELECT count(cte1.temp) * 0 as temp from cte1) __x, src a, dst b

						//! START
						//! WHERE __x.temp + iterativelength(<csr_id>, (SELECT count(c.id) from dst c, a.rowid, b.rowid) between lower and upper

						auto src_row_id = make_uniq<ColumnRefExpression>("rowid", pElement->variable_binding);
						auto dst_row_id = make_uniq<ColumnRefExpression>("rowid", next_vertex_element->variable_binding);
						auto csr_id = make_uniq<ConstantExpression>(Value::INTEGER((int32_t) 0));

						vector<unique_ptr<ParsedExpression>> pathfinding_children;
						pathfinding_children.push_back(std::move(csr_id));
						pathfinding_children.push_back(std::move(GetCountTable(edge_table, pElement->variable_binding)));
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
										make_uniq<ConstantExpression>(Value::INTEGER(subpath->lower));
						auto upper_limit =
										make_uniq<ConstantExpression>(Value::INTEGER(subpath->upper));
						auto between_expression = make_uniq<BetweenExpression>(
										std::move(addition_function), std::move(lower_limit),
										std::move(upper_limit));
						conditions.push_back(std::move(between_expression));

						//! END
						//! WHERE __x.temp + iterativelength(<csr_id>, (SELECT count(s.id) from src s, a.rowid, b.rowid) between lower and upper

					}
				}
			}
			auto subquery = make_uniq<SelectStatement>();
			subquery->node = std::move(select_node);

			auto result = make_uniq<SubqueryRef>(std::move(subquery), named_subpath);
			return std::move(result);
		}
} // namespace duckdb
