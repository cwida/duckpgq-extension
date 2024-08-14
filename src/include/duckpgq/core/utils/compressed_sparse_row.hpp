//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/utils/compressed_sparse_row.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include <duckdb/function/function.hpp>

#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"

#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/property_graph_table.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/tableref/joinref.hpp>
#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

class CSR {
public:
  CSR() = default;
  ~CSR() { delete[] v; }

  atomic<int64_t> *v{};

  vector<int64_t> e;
  vector<int64_t> edge_ids;

  vector<int64_t> w;
  vector<double> w_double;

  bool initialized_v = false;
  bool initialized_e = false;
  bool initialized_w = false;

  size_t vsize{};

  string ToString() const;
};

// CSR BindReplace functions
unique_ptr<CommonTableExpressionInfo> CreateUndirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table);
unique_ptr<CommonTableExpressionInfo> CreateDirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table, const string &prev_binding, const string &edge_binding, const string &next_binding);

// Helper functions
unique_ptr<SubqueryExpression> CreateDirectedCSRVertexSubquery(const shared_ptr<PropertyGraphTable> &edge_table, const std::string &binding);
unique_ptr<SubqueryExpression> CreateUndirectedCSRVertexSubquery(const shared_ptr<PropertyGraphTable> &edge_table, const std::string &binding);
unique_ptr<SelectNode> CreateOuterSelectEdgesNode();
unique_ptr<SelectNode> CreateOuterSelectNode(unique_ptr<FunctionExpression> create_csr_edge_function);
unique_ptr<JoinRef> GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table,const string &edge_binding, const string &prev_binding, const string &next_binding);
unique_ptr<SubqueryExpression> GetCountTable(const shared_ptr<PropertyGraphTable> &edge_table, const string &prev_binding);
unique_ptr<ColumnRefExpression> CreateColumnRef(const std::string &column_name, const std::string &table_name, const std::string &alias);
void SetupSelectNode(unique_ptr<SelectNode> &select_node, const shared_ptr<PropertyGraphTable> &edge_table, bool reverse = false);
int32_t GetCSRId(const unique_ptr<Expression> &expr, ClientContext &context);

} // namespace core

} // namespace duckpgq
