//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/tablefunctions/match.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include <duckdb/parser/parsed_data/create_pragma_function_info.hpp>

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/parser/path_element.hpp"
#include "duckdb/parser/subpath_element.hpp"
#include "duckdb/parser/path_pattern.hpp"

namespace duckdb {
struct PGQMatchFunction : public TableFunction {
public:
  PGQMatchFunction() {
    name = "duckpgq_match";
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

  static PathElement *
  GetPathElement(const unique_ptr<PathReference> &path_reference);

  static unique_ptr<SubqueryExpression>
  GetCountTable(const shared_ptr<PropertyGraphTable> &edge_table,
                const string &prev_binding);

  static unique_ptr<JoinRef>
  GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table,
             const string &edge_binding, const string &prev_binding,
             const string &next_binding);

  static unique_ptr<SubqueryRef> CreateCountCTESubquery();

  static unique_ptr<CommonTableExpressionInfo>
  CreateCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
               const string &edge_binding, const string &prev_binding,
               const string &next_binding);

  static void EdgeTypeAny(const shared_ptr<PropertyGraphTable> &edge_table,
                          const string &edge_binding,
                          const string &prev_binding,
                          const string &next_binding,
                          vector<unique_ptr<ParsedExpression>> &conditions);

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

  static void
  EdgeTypeLeftRight(const shared_ptr<PropertyGraphTable> &edge_table,
                    const string &edge_binding, const string &prev_binding,
                    const string &next_binding,
                    vector<unique_ptr<ParsedExpression>> &conditions,
                    unordered_map<string, string> &alias_map,
                    int32_t &extra_alias_counter);

  static PathElement *
  HandleNestedSubPath(unique_ptr<PathReference> &path_reference,
                      vector<unique_ptr<ParsedExpression>> &conditions,
                      idx_t element_idx);

  static unique_ptr<TableRef> MatchBindReplace(ClientContext &context,
                                               TableFunctionBindInput &input);

  static unique_ptr<SubqueryRef> GenerateSubpathPatternSubquery(
      unique_ptr<PathPattern> &path_pattern, CreatePropertyGraphInfo *pg_table,
      vector<unique_ptr<ParsedExpression>> &column_list,
      unordered_set<string> &named_subpaths);

  static unique_ptr<ParsedExpression>
  CreatePathFindingFunction(vector<unique_ptr<PathReference>> &path_list,
                            CreatePropertyGraphInfo &pg_table);

  static void AddPathFinding(const unique_ptr<SelectNode> &select_node,
                             unique_ptr<TableRef> &from_clause,
                             vector<unique_ptr<ParsedExpression>> &conditions,
                             const string &prev_binding,
                             const string &edge_binding,
                             const string &next_binding,
                             const shared_ptr<PropertyGraphTable> &edge_table,
                             const SubPath *subpath);

  static void
  AddEdgeJoins(const unique_ptr<SelectNode> &select_node,
               const shared_ptr<PropertyGraphTable> &edge_table,
               const shared_ptr<PropertyGraphTable> &previous_vertex_table,
               const shared_ptr<PropertyGraphTable> &next_vertex_table,
               PGQMatchType edge_type, const string &edge_binding,
               const string &prev_binding, const string &next_binding,
               vector<unique_ptr<ParsedExpression>> &conditions,
               unordered_map<string, string> &alias_map,
               int32_t &extra_alias_counter);

  static void ProcessPathList(
      vector<unique_ptr<PathReference>> &path_pattern,
      vector<unique_ptr<ParsedExpression>> &conditions,
      unique_ptr<TableRef> &from_clause, unique_ptr<SelectNode> &select_node,
      unordered_map<string, string> &alias_map,
      CreatePropertyGraphInfo &pg_table, int32_t &extra_alias_counter,
      vector<unique_ptr<ParsedExpression>> &column_list);

  static void
  CheckNamedSubpath(SubPath &subpath,
                    vector<unique_ptr<ParsedExpression>> &column_list,
                    CreatePropertyGraphInfo &pg_table);
};
} // namespace duckdb
