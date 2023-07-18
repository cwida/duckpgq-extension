//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/tablefunctions/match.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/parser/path_element.hpp"
#include "duckdb/parser/subpath_element.hpp"

namespace duckdb {

struct MatchFunction : public TableFunction {
public:
  MatchFunction() {
    name = "duckpgq_match";
    bind_replace = MatchBindReplace;
  }
  struct MatchBindData : public TableFunctionData {
    bool done = false;
  };

  static shared_ptr<PropertyGraphTable> FindGraphTable(const string &label,
                                            CreatePropertyGraphInfo &pg_table);
  static void
  CheckInheritance(shared_ptr<PropertyGraphTable> &tableref, PathElement *element,
                   vector<unique_ptr<ParsedExpression>> &conditions);

  static void CheckEdgeTableConstraints(const string &src_reference,
                                        const string &dst_reference,
																				const shared_ptr<PropertyGraphTable> &edge_table);

  static unique_ptr<ParsedExpression> CreateMatchJoinExpression(
      vector<string> vertex_keys, vector<string> edge_keys,
      const string &vertex_alias, const string &edge_alias);

  static PathElement *
  GetPathElement(unique_ptr<PathReference> &path_reference,
                 vector<unique_ptr<ParsedExpression>> &conditions);

  static unique_ptr<SelectStatement>
  GetCountTable(const shared_ptr<PropertyGraphTable> &edge_table, const string &prev_binding);

  static unique_ptr<JoinRef> GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table,
                                        const string &edge_binding,
                                        const string &prev_binding,
                                        const string &next_binding);

  static unique_ptr<SubqueryRef> CreateCountCTESubquery();

  static unique_ptr<SubqueryRef>
  CreateSrcDstPairsSubquery(vector<unique_ptr<ParsedExpression>> &column_list,
                            const string &prev_binding,
                            const string &next_binding,
														const shared_ptr<PropertyGraphTable> &edge_table,
                            unique_ptr<ParsedExpression> &where_clause);

	static unique_ptr<CommonTableExpressionInfo> CreateCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
																			 const string &edge_binding,
																			 const string &prev_binding,
																			 const string &next_binding);

  static unique_ptr<TableRef> MatchBindReplace(ClientContext &context,
                                               TableFunctionBindInput &input);
};
} // namespace duckdb