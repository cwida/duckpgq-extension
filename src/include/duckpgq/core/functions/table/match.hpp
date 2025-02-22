//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/functions/table/match.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckpgq/common.hpp"
#include <duckdb/parser/parsed_data/create_pragma_function_info.hpp>

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/parser/path_element.hpp"
#include "duckdb/parser/path_pattern.hpp"
#include "duckdb/parser/subpath_element.hpp"
#include "duckdb/parser/property_graph_table.hpp"

#include <duckdb/parser/tableref/matchref.hpp>

namespace duckpgq {

namespace core {

struct PGQMatchFunction : public TableFunction {
public:
  PGQMatchFunction() {
    name = "duckpgq_match";
    arguments.push_back(LogicalType::INTEGER);
    bind_replace = MatchBindReplace;
  }

  struct MatchBindData : public TableFunctionData {
    bool done = false;
  };

  static shared_ptr<PropertyGraphTable>
  FindGraphTable(const string &label, CreatePropertyGraphInfo &pg_table);

  static void
  CheckInheritance(const shared_ptr<PropertyGraphTable> &tableref,
                   PathElement *element,
                   vector<unique_ptr<ParsedExpression>> &conditions);

  static void
  CheckEdgeTableConstraints(const string &src_reference,
                            const string &dst_reference,
                            const shared_ptr<PropertyGraphTable> &edge_table);

  static unique_ptr<ParsedExpression> CreateMatchJoinExpression(
      vector<string> vertex_keys, vector<string> edge_keys,
      const string &vertex_alias, const string &edge_alias);

  // Populate all vertex and edge tables and their alias into
  // [alias_to_vertex_and_edge_tables], from paths information from
  // [path_reference].
  static void PopulateGraphTableAliasMap(
      const CreatePropertyGraphInfo &pg_table,
      const unique_ptr<PathReference> &path_reference,
      case_insensitive_map_t<shared_ptr<PropertyGraphTable>>
          &alias_to_vertex_and_edge_tables);

  static case_insensitive_map_t<shared_ptr<PropertyGraphTable>>
  PopulateGraphTableAliasMap(const CreatePropertyGraphInfo &pg_table,
                             const MatchExpression &match_expr);

  static PathElement *
  GetPathElement(const unique_ptr<PathReference> &path_reference);

  static SubPath *GetSubPath(const unique_ptr<PathReference> &path_reference);

  static unique_ptr<JoinRef>
  GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table,
             const string &edge_binding, const string &prev_binding,
             const string &next_binding);

  static unique_ptr<SubqueryRef> CreateCountCTESubquery();

  static unique_ptr<ParsedExpression>
  CreateWhereClause(vector<unique_ptr<ParsedExpression>> &conditions);

  static void EdgeTypeAny(const shared_ptr<PropertyGraphTable> &edge_table,
                          const string &edge_binding,
                          const string &prev_binding,
                          const string &next_binding,
                          vector<unique_ptr<ParsedExpression>> &conditions,
                          unique_ptr<TableRef> &from_clause);

  static void EdgeTypeLeft(const shared_ptr<PropertyGraphTable> &edge_table,
                           const string &next_table_name,
                           const string &prev_table_name,
                           const string &edge_binding,
                           const string &prev_binding,
                           const string &next_binding,
                           vector<unique_ptr<ParsedExpression>> &conditions);

  static void EdgeTypeRight(const shared_ptr<PropertyGraphTable> &edge_table,
                            const string &next_table_name,
                            const string &prev_table_name,
                            const string &edge_binding,
                            const string &prev_binding,
                            const string &next_binding,
                            vector<unique_ptr<ParsedExpression>> &conditions);

  static void EdgeTypeLeftRight(
      const shared_ptr<PropertyGraphTable> &edge_table,
      const string &edge_binding, const string &prev_binding,
      const string &next_binding,
      vector<unique_ptr<ParsedExpression>> &conditions,
      case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
      int32_t &extra_alias_counter);

  static PathElement *
  HandleNestedSubPath(unique_ptr<PathReference> &path_reference,
                      vector<unique_ptr<ParsedExpression>> &conditions,
                      idx_t element_idx);

  static unique_ptr<ParsedExpression> AddPathQuantifierCondition(
      const string &prev_binding, const string &next_binding,
      const shared_ptr<PropertyGraphTable> &edge_table, const SubPath *subpath);

  static unique_ptr<TableRef> MatchBindReplace(ClientContext &context,
                                               TableFunctionBindInput &input);

  static unique_ptr<SubqueryRef> GenerateSubpathPatternSubquery(
      unique_ptr<PathPattern> &path_pattern, CreatePropertyGraphInfo *pg_table,
      vector<unique_ptr<ParsedExpression>> &column_list,
      unordered_set<string> &named_subpaths);

  static unique_ptr<CommonTableExpressionInfo> GenerateShortestPathCTE(
      CreatePropertyGraphInfo &pg_table, SubPath *edge_subpath,
      PathElement *path_element, PathElement *next_vertex_element,
      vector<unique_ptr<ParsedExpression>> &path_finding_conditions);
  static unique_ptr<ParsedExpression>
  CreatePathFindingFunction(vector<unique_ptr<PathReference>> &path_list,
                            CreatePropertyGraphInfo &pg_table,
                            const string &path_variable,
                            unique_ptr<SelectNode> &final_select_node,
                            vector<unique_ptr<ParsedExpression>> &conditions);

  static void AddPathFinding(unique_ptr<SelectNode> &select_node,
                             vector<unique_ptr<ParsedExpression>> &conditions,
                             const string &prev_binding,
                             const string &edge_binding,
                             const string &next_binding,
                             const shared_ptr<PropertyGraphTable> &edge_table,
                             CreatePropertyGraphInfo &pg_table,
                             SubPath *subpath, PGQMatchType edge_type);

  static void AddEdgeJoins(
      const shared_ptr<PropertyGraphTable> &edge_table,
      const shared_ptr<PropertyGraphTable> &previous_vertex_table,
      const shared_ptr<PropertyGraphTable> &next_vertex_table,
      PGQMatchType edge_type, const string &edge_binding,
      const string &prev_binding, const string &next_binding,
      vector<unique_ptr<ParsedExpression>> &conditions,
      case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
      int32_t &extra_alias_counter, unique_ptr<TableRef> &from_clause);

  static void ProcessPathList(
      vector<unique_ptr<PathReference>> &path_pattern,
      vector<unique_ptr<ParsedExpression>> &conditions,
      unique_ptr<SelectNode> &select_node,
      case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
      CreatePropertyGraphInfo &pg_table, int32_t &extra_alias_counter,
      MatchExpression &original_ref);

  static void
  CheckNamedSubpath(SubPath &subpath, MatchExpression &original_ref,
                    CreatePropertyGraphInfo &pg_table,
                    unique_ptr<SelectNode> &final_select_node,
                    vector<unique_ptr<ParsedExpression>> &conditions);

  // Check whether columns to query are valid against the property graph, throws
  // BinderException if error.
  static void CheckColumnBinding(
      const CreatePropertyGraphInfo &pg_table, const MatchExpression &ref,
      const case_insensitive_map_t<shared_ptr<PropertyGraphTable>>
          &alias_to_vertex_and_edge_tables);
};

} // namespace core

} // namespace duckpgq
