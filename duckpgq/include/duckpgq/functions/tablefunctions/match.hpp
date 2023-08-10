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

  static shared_ptr<PropertyGraphTable>
  FindGraphTable(const string &label, CreatePropertyGraphInfo &pg_table);
  static void
  CheckInheritance(shared_ptr<PropertyGraphTable> &tableref,
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
  GetPathElement(unique_ptr<PathReference> &path_reference,
                 vector<unique_ptr<ParsedExpression>> &conditions);

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

	static void EdgeTypeAny(
					shared_ptr<PropertyGraphTable> &edge_table,
					const string &edge_binding,
					const string &prev_binding,
					const string &next_binding,
					vector<unique_ptr<ParsedExpression>> &conditions);

	static void EdgeTypeLeft(shared_ptr<PropertyGraphTable> &edge_table,
													 const string &next_table_name, const string &prev_table_name,
													 const string &edge_binding,
													 const string &prev_binding,
													 const string &next_binding,
													 vector<unique_ptr<ParsedExpression>> &conditions);

	static void EdgeTypeRight(shared_ptr<PropertyGraphTable> &edge_table,
																			const string &next_table_name, const string &prev_table_name,
																			const string &edge_binding,
																			const string &prev_binding,
																			const string &next_binding,
																			vector<unique_ptr<ParsedExpression>> &conditions);

	static void EdgeTypeLeftRight(shared_ptr<PropertyGraphTable> &edge_table,
																							 const string &edge_binding,
																							 const string &prev_binding,
																							 const string &next_binding,
																							 vector<unique_ptr<ParsedExpression>> &conditions,
																							 unordered_map<string, string> &alias_map,
																							 int32_t &extra_alias_counter);

	static PathElement* HandleNestedSubPath(unique_ptr<PathReference> &path_reference,
																						 vector<unique_ptr<ParsedExpression>> &conditions,
																						 idx_t element_idx);

	static unique_ptr<TableRef> MatchBindReplace(ClientContext &context,
                                               TableFunctionBindInput &input);

	static unique_ptr<SubqueryRef> GenerateSubpathSubquery(SubPath *pPath, CreatePropertyGraphInfo* pg_table);
};
} // namespace duckdb