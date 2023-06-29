#define DUCKDB_EXTENSION_MAIN


#include "duckpgq_extension.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckpgq/duckpgq_functions.hpp"

#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/result_modifier.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

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

#include "duckdb/parser/statement/extension_statement.hpp"

#include "duckdb/parser/query_node/select_node.hpp"

#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/common/enums/joinref_type.hpp"

#include "duckpgq/functions/tablefunctions/drop_property_graph.hpp"
#include "duckpgq/functions/tablefunctions/create_property_graph.hpp"


namespace duckdb {

inline void DuckpgqScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "Duckpgq "+name.GetString()+" 🐥");;
        });
}

struct MatchFunction : public TableFunction {
public:
    MatchFunction() {
        name = "duckpgq_match";
        bind_replace = MatchBindReplace;
    }
    struct MatchBindData : public TableFunctionData {
        bool done = false;
    };

    static PropertyGraphTable* FindGraphTable(const string &label, CreatePropertyGraphInfo &pg_table) {
        auto graph_table_entry = pg_table.label_map.find(label);
        if (graph_table_entry == pg_table.label_map.end()) {
            throw BinderException("The label %s is not registered in property graph %s", label,
                                  pg_table.property_graph_name);
        }

        return graph_table_entry->second;
    }

    static void CheckInheritance(PropertyGraphTable* &tableref, PathElement *element,
                          vector<unique_ptr<ParsedExpression>> &conditions) {
        if (tableref->main_label == element->label) {
            return;
        }
        auto constant_expression_two = make_uniq<ConstantExpression>(Value::INTEGER((int32_t)2));
       auto itr =
                std::find(tableref->sub_labels.begin(), tableref->sub_labels.end(), element->label);

        auto idx_of_element = std::distance(tableref->sub_labels.begin(), itr);
        auto constant_expression_idx_label = make_uniq<ConstantExpression>(Value::INTEGER((int32_t)idx_of_element));

        vector<unique_ptr<ParsedExpression>> power_of_children;
        power_of_children.push_back(std::move(constant_expression_two));
        power_of_children.push_back(std::move(constant_expression_idx_label));
        auto power_of_term = make_uniq<FunctionExpression>("power", std::move(power_of_children));
        auto bigint_cast = make_uniq<CastExpression>(LogicalType::BIGINT, std::move(power_of_term));
        auto subcategory_colref = make_uniq<ColumnRefExpression>(tableref->discriminator, element->variable_binding);
        vector<unique_ptr<ParsedExpression>> and_children;
        and_children.push_back(std::move(subcategory_colref));
        and_children.push_back(std::move(bigint_cast));

        auto and_expression = make_uniq<FunctionExpression>("&", std::move(and_children));

        auto constant_expression_idx_label_comparison = make_uniq<ConstantExpression>(Value::INTEGER((int32_t)idx_of_element + 1));

        auto subset_compare = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
                                                                std::move(and_expression), std::move(constant_expression_idx_label_comparison));
        conditions.push_back(std::move(subset_compare));
    }

    static void CheckEdgeTableConstraints(const string &src_reference, const string &dst_reference,
                                          PropertyGraphTable*  &edge_table) {
        if (src_reference != edge_table->source_reference) {
            throw BinderException("Label %s is not registered as a source reference for edge pattern of table %s",
                                  src_reference, edge_table->table_name);
        }
        if (dst_reference != edge_table->destination_reference) {
            throw BinderException("Label %s is not registered as a destination reference for edge pattern of table %s",
                                  src_reference, edge_table->table_name);
        }
    }

    static unique_ptr<ParsedExpression> CreateMatchJoinExpression(vector<string> vertex_keys, vector<string> edge_keys,
                                                           const string &vertex_alias, const string &edge_alias) {
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

    static PathElement *GetPathElement(unique_ptr<PathReference> &path_reference,
                                vector<unique_ptr<ParsedExpression>> &conditions) {
        if (path_reference->path_reference_type == PGQPathReferenceType::PATH_ELEMENT) {
            return reinterpret_cast<PathElement *>(path_reference.get());
        } else if (path_reference->path_reference_type == PGQPathReferenceType::SUBPATH) {
            auto subpath = reinterpret_cast<SubPath *>(path_reference.get());
            if (subpath->where_clause) {
                conditions.push_back(std::move(subpath->where_clause));
            }
            return reinterpret_cast<PathElement *>(subpath->path_list[0].get());
        } else {
            throw InternalException("Unknown path reference type detected");
        }
    }

    static unique_ptr<SelectStatement> GetCountTable(PropertyGraphTable*  &edge_table,
                                                     const string &prev_binding) {
        auto select_count = make_uniq<SelectStatement>();
        auto select_inner = make_uniq<SelectNode>();
        auto ref = make_uniq<BaseTableRef>();

        ref->table_name = edge_table->source_reference;
        ref->alias = prev_binding;
        select_inner->from_table = std::move(ref);
        vector<unique_ptr<ParsedExpression>> children;
        children.push_back(make_uniq<ColumnRefExpression>(edge_table->source_pk[0], prev_binding));

        auto count_function = make_uniq<FunctionExpression>("count", std::move(children));
        select_inner->select_list.push_back(std::move(count_function));
        select_count->node = std::move(select_inner);
        return select_count;
    }

    static unique_ptr<JoinRef> GetJoinRef(PropertyGraphTable* &edge_table, const string &edge_binding,
                                          const string &prev_binding, const string &next_binding) {
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
        auto t_from_ref = make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_binding);
        auto src_cid_ref = make_uniq<ColumnRefExpression>(edge_table->source_pk[0], prev_binding);
        second_join_ref->condition =
                make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(t_from_ref), std::move(src_cid_ref));
        auto dst_base_ref = make_uniq<BaseTableRef>();
        dst_base_ref->table_name = edge_table->destination_reference;
        dst_base_ref->alias = next_binding;
        first_join_ref->left = std::move(second_join_ref);
        first_join_ref->right = std::move(dst_base_ref);

        auto t_to_ref = make_uniq<ColumnRefExpression>(edge_table->destination_fk[0], edge_binding);
        auto dst_cid_ref = make_uniq<ColumnRefExpression>(edge_table->destination_pk[0], next_binding);
        first_join_ref->condition =
                make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(t_to_ref), std::move(dst_cid_ref));
        return first_join_ref;
    }

    static unique_ptr<SubqueryRef> CreateCountCTESubquery() {
        //! BEGIN OF (SELECT count(cte1.temp) * 0 from cte1) __x

        auto temp_cte_select_node = make_uniq<SelectNode>();

        auto cte_table_ref = make_uniq<BaseTableRef>();

        cte_table_ref->table_name = "cte1";
        temp_cte_select_node->from_table = std::move(cte_table_ref);
        vector<unique_ptr<ParsedExpression>> children;
        children.push_back(make_uniq<ColumnRefExpression>("temp", "cte1"));

        auto count_function = make_uniq<FunctionExpression>("count", std::move(children));

        auto zero = make_uniq<ConstantExpression>(Value::INTEGER((int32_t)0));

        vector<unique_ptr<ParsedExpression>> multiply_children;

        multiply_children.push_back(std::move(zero));
        multiply_children.push_back(std::move(count_function));
        auto multiply_function = make_uniq<FunctionExpression>("multiply", std::move(multiply_children));
        multiply_function->alias = "temp";
        temp_cte_select_node->select_list.push_back(std::move(multiply_function));
        auto temp_cte_select_statement = make_uniq<SelectStatement>();
        temp_cte_select_statement->node = std::move(temp_cte_select_node);

        auto temp_cte_select_subquery = make_uniq<SubqueryRef>(std::move(temp_cte_select_statement), "__x");
        //! END OF (SELECT count(cte1.temp) from cte1) __x
        return temp_cte_select_subquery;
    }

    static unique_ptr<SubqueryRef> CreateSrcDstPairsSubquery(vector<unique_ptr<ParsedExpression>> &column_list,
                                                      const string &prev_binding, const string &next_binding,
                                                      PropertyGraphTable*  &edge_table,
                                                      unique_ptr<ParsedExpression> &where_clause) {
        auto src_dst_pairs_node = make_uniq<SelectNode>();
        //! src.rowid
        auto src_rowid = make_uniq<ColumnRefExpression>("rowid", prev_binding);
        src_rowid->alias = "__src";
        src_dst_pairs_node->select_list.push_back(std::move(src_rowid));
        //! dst.rowid
        auto dst_rowid = make_uniq<ColumnRefExpression>("rowid", next_binding);
        dst_rowid->alias = "__dst";
        src_dst_pairs_node->select_list.push_back(std::move(dst_rowid));

        //! Select all columns that we need
        //! (Needs to be reworked in the future since we maybe don't want to select _all_ columns in this subquery)
        for (auto &column : column_list) {
            src_dst_pairs_node->select_list.push_back(std::move(column));
        }

        //! src alias
        auto src_vertex_ref = make_uniq<BaseTableRef>();
        src_vertex_ref->table_name = edge_table->source_reference;
        src_vertex_ref->alias = prev_binding;

        //! dst alias
        auto dst_vertex_ref = make_uniq<BaseTableRef>();
        dst_vertex_ref->table_name = edge_table->destination_reference;
        dst_vertex_ref->alias = next_binding;

        //! FROM src alias, dst alias (represented as a cross join between the two)
        auto cross_join_src_dst = make_uniq<JoinRef>(JoinRefType::CROSS);
        cross_join_src_dst->left = std::move(src_vertex_ref);
        cross_join_src_dst->right = std::move(dst_vertex_ref);

        //! Adding to the from clause
        src_dst_pairs_node->from_table = std::move(cross_join_src_dst);

        //! Adding the where clause that we filter on
        src_dst_pairs_node->where_clause = std::move(where_clause);

        auto src_dst_pairs_statement = make_uniq<SelectStatement>();
        src_dst_pairs_statement->node = std::move(src_dst_pairs_node);

        auto src_dst_pairs_subquery = make_uniq<SubqueryRef>(std::move(src_dst_pairs_statement), "__p");
        return src_dst_pairs_subquery;
    }

    static unique_ptr<TableRef> MatchBindReplace(ClientContext &context, TableFunctionBindInput &input) {
        auto data = make_uniq<MatchFunction::MatchBindData>();
        auto duckpgq_state_entry = context.registered_state.find("duckpgq");
        auto duckpgq_state = (DuckPGQState *)duckpgq_state_entry->second.get();

        auto ref = dynamic_cast<MatchExpression*>(duckpgq_state->transform_expression.get());
        auto pg_table = duckpgq_state->GetPropertyGraph(ref->pg_name);

        auto outer_select_statement = make_uniq<SelectStatement>();
        auto cte_select_statement = make_uniq<SelectStatement>();

        vector<unique_ptr<ParsedExpression>> conditions;

        auto select_node = make_uniq<SelectNode>();
        unordered_map<string, string> alias_map;

        auto extra_alias_counter = 0;
        bool path_finding = false;
        for (idx_t idx_i = 0; idx_i < ref->path_list.size(); idx_i++) {
            auto &path_list = ref->path_list[idx_i];

            PathElement *previous_vertex_element = GetPathElement(path_list->path_elements[0], conditions);

            auto previous_vertex_table = FindGraphTable(previous_vertex_element->label, *pg_table);
            CheckInheritance(previous_vertex_table, previous_vertex_element, conditions);
            alias_map[previous_vertex_element->variable_binding] = previous_vertex_table->table_name;

            for (idx_t idx_j = 1; idx_j < ref->path_list[idx_i]->path_elements.size(); idx_j = idx_j + 2) {
                PathElement *edge_element = GetPathElement(path_list->path_elements[idx_j], conditions);
                PathElement *next_vertex_element = GetPathElement(path_list->path_elements[idx_j + 1], conditions);
                if (next_vertex_element->match_type != PGQMatchType::MATCH_VERTEX ||
                    previous_vertex_element->match_type != PGQMatchType::MATCH_VERTEX) {
                    throw BinderException("Vertex and edge patterns must be alternated.");
                }

                auto edge_table = FindGraphTable(edge_element->label, *pg_table);
                CheckInheritance(edge_table, edge_element, conditions);
                auto next_vertex_table = FindGraphTable(next_vertex_element->label, *pg_table);
                CheckInheritance(next_vertex_table, next_vertex_element, conditions);
                if (next_vertex_table->main_label != next_vertex_element->label) {
                    auto constant_expression_two = make_uniq<ConstantExpression>(Value::INTEGER((int32_t)2));
                    auto itr =
                            std::find(next_vertex_table->sub_labels.begin(), next_vertex_table->sub_labels.end(),
                                      next_vertex_element->label);

                    auto idx_of_element = std::distance(next_vertex_table->sub_labels.begin(), itr);
                    auto constant_expression_idx_label =
                            make_uniq<ConstantExpression>(Value::INTEGER((int32_t)idx_of_element));

                    vector<unique_ptr<ParsedExpression>> power_of_children;
                    power_of_children.push_back(std::move(constant_expression_two));
                    power_of_children.push_back(std::move(constant_expression_idx_label));
                    auto power_of_term = make_uniq<FunctionExpression>("power", std::move(power_of_children));

                    auto subcategory_colref = make_uniq<ColumnRefExpression>(next_vertex_table->discriminator,
                                                                               next_vertex_element->variable_binding);
                    auto subset_compare = make_uniq<ComparisonExpression>(
                            ExpressionType::COMPARE_EQUAL, std::move(subcategory_colref), std::move(power_of_term));
                    conditions.push_back(std::move(subset_compare));
                }
                if (path_list->path_elements[idx_j]->path_reference_type == PGQPathReferenceType::SUBPATH) {
                    auto *subpath = reinterpret_cast<SubPath *>(path_list->path_elements[idx_j].get());
                    if (subpath->upper > 1) {
                        path_finding = true;
                        auto csr_edge_id_constant = make_uniq<ConstantExpression>(Value::INTEGER((int32_t)0));
                        auto count_create_edge_select = make_uniq<SubqueryExpression>();

                        count_create_edge_select->subquery =
                                GetCountTable(edge_table, previous_vertex_element->variable_binding);
                        count_create_edge_select->subquery_type = SubqueryType::SCALAR;

                        auto cast_subquery_expr = make_uniq<SubqueryExpression>();
                        auto cast_select_node = make_uniq<SelectNode>();

                        vector<unique_ptr<ParsedExpression>> csr_vertex_children;
                        csr_vertex_children.push_back(make_uniq<ConstantExpression>(Value::INTEGER((int32_t)0)));

                        auto count_create_vertex_expr = make_uniq<SubqueryExpression>();
                        count_create_vertex_expr->subquery =
                                GetCountTable(edge_table, previous_vertex_element->variable_binding);
                        count_create_vertex_expr->subquery_type = SubqueryType::SCALAR;
                        csr_vertex_children.push_back(std::move(count_create_vertex_expr));

                        csr_vertex_children.push_back(make_uniq<ColumnRefExpression>("dense_id", "sub"));
                        csr_vertex_children.push_back(make_uniq<ColumnRefExpression>("cnt", "sub"));

                        auto create_vertex_function =
                                make_uniq<FunctionExpression>("create_csr_vertex", std::move(csr_vertex_children));
                        vector<unique_ptr<ParsedExpression>> sum_children;
                        sum_children.push_back(std::move(create_vertex_function));
                        auto sum_function = make_uniq<FunctionExpression>("sum", std::move(sum_children));

                        auto inner_select_statement = make_uniq<SelectStatement>();
                        auto inner_select_node = make_uniq<SelectNode>();

                        auto source_rowid_colref =
                                make_uniq<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
                        source_rowid_colref->alias = "dense_id";

                        auto count_create_inner_expr = make_uniq<SubqueryExpression>();
                        count_create_inner_expr->subquery_type = SubqueryType::SCALAR;
                        auto edge_src_colref =
                                make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_element->variable_binding);
                        vector<unique_ptr<ParsedExpression>> inner_count_children;
                        inner_count_children.push_back(std::move(edge_src_colref));
                        auto inner_count_function =
                                make_uniq<FunctionExpression>("count", std::move(inner_count_children));
                        inner_count_function->alias = "cnt";

                        inner_select_node->select_list.push_back(std::move(source_rowid_colref));
                        inner_select_node->select_list.push_back(std::move(inner_count_function));
                        auto source_rowid_colref_1 =
                                make_uniq<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
                        expression_map_t<idx_t> grouping_expression_map;
                        inner_select_node->groups.group_expressions.push_back(std::move(source_rowid_colref_1));
                        GroupingSet grouping_set = {0};
                        inner_select_node->groups.grouping_sets.push_back(grouping_set);

                        auto inner_join_ref = make_uniq<JoinRef>(JoinRefType::REGULAR);
                        inner_join_ref->type = JoinType::LEFT;
                        auto left_base_ref = make_uniq<BaseTableRef>();
                        left_base_ref->table_name = edge_table->source_reference;
                        left_base_ref->alias = previous_vertex_element->variable_binding;
                        auto right_base_ref = make_uniq<BaseTableRef>();
                        right_base_ref->table_name = edge_table->table_name;
                        right_base_ref->alias = edge_element->variable_binding;
                        inner_join_ref->left = std::move(left_base_ref);
                        inner_join_ref->right = std::move(right_base_ref);

                        auto edge_join_colref =
                                make_uniq<ColumnRefExpression>(edge_table->source_fk[0], edge_element->variable_binding);
                        auto vertex_join_colref = make_uniq<ColumnRefExpression>(
                                edge_table->source_pk[0], previous_vertex_element->variable_binding);

                        inner_join_ref->condition = make_uniq<ComparisonExpression>(
                                ExpressionType::COMPARE_EQUAL, std::move(edge_join_colref), std::move(vertex_join_colref));
                        inner_select_node->from_table = std::move(inner_join_ref);
                        inner_select_statement->node = std::move(inner_select_node);

                        auto inner_from_subquery = make_uniq<SubqueryRef>(std::move(inner_select_statement), "sub");

                        cast_select_node->from_table = std::move(inner_from_subquery);

                        cast_select_node->select_list.push_back(std::move(sum_function));
                        auto cast_select_stmt = make_uniq<SelectStatement>();
                        cast_select_stmt->node = std::move(cast_select_node);
                        cast_subquery_expr->subquery = std::move(cast_select_stmt);
                        cast_subquery_expr->subquery_type = SubqueryType::SCALAR;

                        auto src_rowid_colref =
                                make_uniq<ColumnRefExpression>("rowid", previous_vertex_element->variable_binding);
                        auto dst_rowid_colref =
                                make_uniq<ColumnRefExpression>("rowid", next_vertex_element->variable_binding);
                        auto edge_rowid_colref = make_uniq<ColumnRefExpression>("rowid", edge_element->variable_binding);

                        auto cast_expression =
                                make_uniq<CastExpression>(LogicalType::BIGINT, std::move(cast_subquery_expr));

                        vector<unique_ptr<ParsedExpression>> csr_edge_children;
                        csr_edge_children.push_back(std::move(csr_edge_id_constant));
                        csr_edge_children.push_back(std::move(count_create_edge_select));
                        csr_edge_children.push_back(std::move(cast_expression));
                        csr_edge_children.push_back(std::move(src_rowid_colref));
                        csr_edge_children.push_back(std::move(dst_rowid_colref));
                        csr_edge_children.push_back(std::move(edge_rowid_colref));

                        auto outer_select_node = make_uniq<SelectNode>();

                        auto create_csr_edge_function =
                                make_uniq<FunctionExpression>("create_csr_edge", std::move(csr_edge_children));
                        create_csr_edge_function->alias = "temp";

                        outer_select_node->select_list.push_back(std::move(create_csr_edge_function));
                        outer_select_node->from_table =
                                GetJoinRef(edge_table, edge_element->variable_binding,
                                           previous_vertex_element->variable_binding, next_vertex_element->variable_binding);

                        outer_select_statement->node = std::move(outer_select_node);
                        auto info = make_uniq<CommonTableExpressionInfo>();
                        info->query = std::move(outer_select_statement);

                        auto cte_select_node = make_uniq<SelectNode>();
                        cte_select_node->cte_map.map["cte1"] = std::move(info);

                        for (auto &col : ref->column_list) {
                            auto col_ref = reinterpret_cast<ColumnRefExpression *>(col.get());
                            auto new_col_ref = make_uniq<ColumnRefExpression>(col_ref->alias, "__p");
                            cte_select_node->select_list.push_back(std::move(new_col_ref));
                        }

                        //! (SELECT count(cte1.temp) * 0 from cte1) __x

                        auto temp_cte_select_subquery = CreateCountCTESubquery();

                        //! (SELECT src.rowid as __src, b.rowid as __dst, <...> FROM src_table src, dst_table dst <where
                        //! ...>) __p
                        auto source_destination_pairs_subquery =
                                CreateSrcDstPairsSubquery(ref->column_list, previous_vertex_element->variable_binding,
                                                          next_vertex_element->variable_binding, edge_table, ref->where_clause);
                        auto cross_join_src_dst = make_uniq<JoinRef>(JoinRefType::CROSS);

                        cross_join_src_dst->left = std::move(temp_cte_select_subquery);
                        cross_join_src_dst->right = std::move(source_destination_pairs_subquery);

                        cte_select_node->from_table = std::move(cross_join_src_dst);

                        vector<unique_ptr<ParsedExpression>> reachability_children;
                        auto cte_where_src_row = make_uniq<ColumnRefExpression>("__src", "__p");
                        auto cte_where_dst_row = make_uniq<ColumnRefExpression>("__dst", "__p");
                        auto reachability_subquery_expr = make_uniq<SubqueryExpression>();
                        reachability_subquery_expr->subquery =
                                GetCountTable(edge_table, previous_vertex_element->variable_binding);
                        reachability_subquery_expr->subquery_type = SubqueryType::SCALAR;

                        auto reachability_id_constant = make_uniq<ConstantExpression>(Value::INTEGER((int32_t)0));

                        reachability_children.push_back(std::move(reachability_id_constant));
                        reachability_children.push_back(std::move(reachability_subquery_expr));
                        reachability_children.push_back(std::move(cte_where_src_row));
                        reachability_children.push_back(std::move(cte_where_dst_row));

                        auto reachability_function =
                                make_uniq<FunctionExpression>("iterativelength", std::move(reachability_children));
                        auto cte_col_ref = make_uniq<ColumnRefExpression>("temp", "__x");

                        vector<unique_ptr<ParsedExpression>> addition_children;
                        addition_children.push_back(std::move(cte_col_ref));
                        addition_children.push_back(std::move(reachability_function));

                        auto addition_function = make_uniq<FunctionExpression>("add", std::move(addition_children));
                        auto lower_limit = make_uniq<ConstantExpression>(Value::INTEGER(subpath->lower));
                        auto upper_limit = make_uniq<ConstantExpression>(Value::INTEGER(subpath->upper));
                        auto between_expression = make_uniq<BetweenExpression>(
                                std::move(addition_function), std::move(lower_limit), std::move(upper_limit));
                        conditions.push_back(std::move(between_expression));

                        unique_ptr<ParsedExpression> cte_and_expression;
                        for (auto &condition : conditions) {
                            if (cte_and_expression) {
                                cte_and_expression = make_uniq<ConjunctionExpression>(
                                        ExpressionType::CONJUNCTION_AND, std::move(cte_and_expression), std::move(condition));
                            } else {
                                cte_and_expression = std::move(condition);
                            }
                        }
                        cte_select_node->where_clause = std::move(cte_and_expression);
                        cte_select_statement->node = std::move(cte_select_node);

                        //                    auto result = make_uniq<SubqueryRef>(std::move(cte_select_statement),
                        //                    ref.alias); return Bind(*result);
                    }
                }

                // check aliases
                alias_map[next_vertex_element->variable_binding] = next_vertex_table->table_name;
                alias_map[edge_element->variable_binding] = edge_table->table_name;

                switch (edge_element->match_type) {
                    case PGQMatchType::MATCH_EDGE_ANY: {
                        select_node->modifiers.push_back(make_uniq<DistinctModifier>());

                        auto src_left_expr = CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
                                                                       previous_vertex_element->variable_binding,
                                                                       edge_element->variable_binding);
                        auto dst_left_expr =
                                CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
                                                          next_vertex_element->variable_binding, edge_element->variable_binding);

                        auto combined_left_expr = make_uniq<ConjunctionExpression>(
                                ExpressionType::CONJUNCTION_AND, std::move(src_left_expr), std::move(dst_left_expr));

                        auto src_right_expr = CreateMatchJoinExpression(edge_table->source_pk, edge_table->destination_fk,
                                                                        previous_vertex_element->variable_binding,
                                                                        edge_element->variable_binding);
                        auto dst_right_expr =
                                CreateMatchJoinExpression(edge_table->destination_pk, edge_table->source_fk,
                                                          next_vertex_element->variable_binding, edge_element->variable_binding);
                        auto combined_right_expr = make_uniq<ConjunctionExpression>(
                                ExpressionType::CONJUNCTION_AND, std::move(src_right_expr), std::move(dst_right_expr));

                        auto combined_expr = make_uniq<ConjunctionExpression>(
                                ExpressionType::CONJUNCTION_OR, std::move(combined_left_expr), std::move(combined_right_expr));
                        conditions.push_back(std::move(combined_expr));
                        break;
                    }
                    case PGQMatchType::MATCH_EDGE_LEFT:
                        CheckEdgeTableConstraints(next_vertex_table->table_name, previous_vertex_table->table_name, edge_table);
                        conditions.push_back(CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
                                                                       next_vertex_element->variable_binding,
                                                                       edge_element->variable_binding));
                        conditions.push_back(CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
                                                                       previous_vertex_element->variable_binding,
                                                                       edge_element->variable_binding));
                        break;
                    case PGQMatchType::MATCH_EDGE_RIGHT:
                        CheckEdgeTableConstraints(previous_vertex_table->table_name, next_vertex_table->table_name, edge_table);
                        conditions.push_back(CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
                                                                       previous_vertex_element->variable_binding,
                                                                       edge_element->variable_binding));
                        conditions.push_back(CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
                                                                       next_vertex_element->variable_binding,
                                                                       edge_element->variable_binding));
                        break;
                    case PGQMatchType::MATCH_EDGE_LEFT_RIGHT: {
                        auto src_left_expr =
                                CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
                                                          next_vertex_element->variable_binding, edge_element->variable_binding);
                        auto dst_left_expr = CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
                                                                       previous_vertex_element->variable_binding,
                                                                       edge_element->variable_binding);

                        auto combined_left_expr = make_uniq<ConjunctionExpression>(
                                ExpressionType::CONJUNCTION_AND, std::move(src_left_expr), std::move(dst_left_expr));

                        auto additional_edge_alias = edge_element->variable_binding + std::to_string(extra_alias_counter);
                        extra_alias_counter++;

                        alias_map[additional_edge_alias] = edge_table->table_name;

                        auto src_right_expr =
                                CreateMatchJoinExpression(edge_table->source_pk, edge_table->source_fk,
                                                          previous_vertex_element->variable_binding, additional_edge_alias);
                        auto dst_right_expr =
                                CreateMatchJoinExpression(edge_table->destination_pk, edge_table->destination_fk,
                                                          next_vertex_element->variable_binding, additional_edge_alias);
                        auto combined_right_expr = make_uniq<ConjunctionExpression>(
                                ExpressionType::CONJUNCTION_AND, std::move(src_right_expr), std::move(dst_right_expr));

                        auto combined_expr = make_uniq<ConjunctionExpression>(
                                ExpressionType::CONJUNCTION_AND, std::move(combined_left_expr), std::move(combined_right_expr));
                        conditions.push_back(std::move(combined_expr));
                        break;
                    }

                    default:
                        throw InternalException("Unknown match type found");
                }
                previous_vertex_element = next_vertex_element;
                previous_vertex_table = next_vertex_table;

                // Check the edge type
                // If (a)-[b]->(c) 	-> 	b.src = a.id AND b.dst = c.id
                // If (a)<-[b]-(c) 	-> 	b.dst = a.id AND b.src = c.id
                // If (a)-[b]-(c)  	-> 	(b.src = a.id AND b.dst = c.id) OR
                // 						(b.dst = a.id AND b.src = c.id)
                // If (a)<-[b]->(c)	->  (b.src = a.id AND b.dst = c.id) AND
                //						(b.dst = a.id AND b.src = c.id)
            }
        }

        if (path_finding) {
            auto result = make_uniq<SubqueryRef>(std::move(cte_select_statement), ref->alias);
            return std::move(result);
        }

        unique_ptr<TableRef> from_clause;

        for (auto &table_alias_entry : alias_map) {
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

        unique_ptr<ParsedExpression> where_clause;
        if (ref->where_clause) {
            conditions.push_back(std::move(ref->where_clause));
        }

        for (auto &condition : conditions) {
            if (where_clause) {
                where_clause = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(where_clause),
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
};




static void LoadInternal(DatabaseInstance &instance) {
    auto &config = DBConfig::GetConfig(instance);
    DuckPGQParserExtension pgq_parser;
    config.parser_extensions.push_back(pgq_parser);
    config.operator_extensions.push_back(make_uniq<DuckPGQOperatorExtension>());

	Connection con(instance);
    con.BeginTransaction();

    auto &catalog = Catalog::GetSystemCatalog(*con.context);

    MatchFunction match_pg_function;
    CreateTableFunctionInfo match_pg_info(match_pg_function);
    catalog.CreateTableFunction(*con.context, match_pg_info);

    CreatePropertyGraphFunction create_pg_function;
    CreateTableFunctionInfo create_pg_info(create_pg_function);
    catalog.CreateTableFunction(*con.context, create_pg_info);

    DropPropertyGraphFunction drop_pg_function;
    CreateTableFunctionInfo drop_pg_info(drop_pg_function);
    catalog.CreateTableFunction(*con.context, drop_pg_info);

    for (auto &fun : DuckPGQFunctions::GetFunctions()) {
        catalog.CreateFunction(*con.context, fun);
    }

    CreateScalarFunctionInfo duckpgq_fun_info(
            ScalarFunction("duckpgq", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DuckpgqScalarFun));
    duckpgq_fun_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
    catalog.CreateFunction(*con.context, duckpgq_fun_info);
    con.Commit();
}

void DuckpgqExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

ParserExtensionParseResult duckpgq_parse(ParserExtensionInfo *info,
                                         const std::string &query) {
    auto parse_info = (DuckPGQParserExtensionInfo &)(info);
    Parser parser;
    parser.ParseQuery((query[0] == '-') ? query.substr(1, query.length()) : query);
    if (parser.statements.size() != 1) {
        throw ParserException("More than 1 statement detected, please only give one.");
    }
    return {make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(std::move(parser.statements[0]))};
}


BoundStatement duckpgq_bind(ClientContext &context, Binder &binder,
                            OperatorExtensionInfo *info, SQLStatement &statement) {
    auto lookup = context.registered_state.find("duckpgq");
    if (lookup != context.registered_state.end()) {
        auto duckpgq_state = (DuckPGQState *)lookup->second.get();
        auto duckpgq_binder = Binder::CreateBinder(context);
        auto duckpgq_parse_data =
                dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());
        return duckpgq_binder->Bind(*(duckpgq_parse_data->statement));
    }
    throw BinderException("Registered state not found");
}


ParserExtensionPlanResult duckpgq_plan(ParserExtensionInfo *info, ClientContext &context,
                                           unique_ptr<ParserExtensionParseData> parse_data) {
    auto duckpgq_state_entry = context.registered_state.find("duckpgq");
    DuckPGQState *duckpgq_state;
    if (duckpgq_state_entry == context.registered_state.end()) {
        auto state = make_shared<DuckPGQState>(std::move(parse_data));
        context.registered_state["duckpgq"] = state;
        duckpgq_state = state.get();
    } else {
        duckpgq_state = (DuckPGQState *) duckpgq_state_entry->second.get();
        duckpgq_state->parse_data = std::move(parse_data);
    }
    auto duckpgq_parse_data = dynamic_cast<DuckPGQParseData *>(duckpgq_state->parse_data.get());

    if (!duckpgq_parse_data) {
        throw BinderException("Not DuckPGQ parse data");
    }
    auto statement = dynamic_cast<SQLStatement *>(duckpgq_parse_data->statement.get());
    if (statement->type == StatementType::SELECT_STATEMENT) {
        auto select_statement = dynamic_cast<SelectStatement*>(statement);
        auto select_node = dynamic_cast<SelectNode*>(select_statement->node.get());
        auto from_table_function = dynamic_cast<TableFunctionRef*>(select_node->from_table.get());
        auto function = dynamic_cast<FunctionExpression*>(from_table_function->function.get());
        if (function->function_name == "duckpgq_match") {
            duckpgq_state->transform_expression = std::move(std::move(function->children[0]));
            function->children.pop_back();
        }
        throw Exception("use duckpgq_bind instead");
    } else if (statement->type == StatementType::CREATE_STATEMENT) {
        ParserExtensionPlanResult result;
        result.function = CreatePropertyGraphFunction();
        result.requires_valid_transaction = true;
        result.return_type = StatementReturnType::QUERY_RESULT;
        return result;
    } else if (statement->type == StatementType::DROP_STATEMENT) {
        ParserExtensionPlanResult result;
        result.function = DropPropertyGraphFunction();
        result.requires_valid_transaction = true;
        result.return_type = StatementReturnType::QUERY_RESULT;
        return result;
    }
    throw BinderException("Unknown DuckPGQ query encountered");
}


std::string DuckpgqExtension::Name() {
	return "duckpgq";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void duckpgq_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *duckpgq_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
