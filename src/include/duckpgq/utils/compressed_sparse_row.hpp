//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/utils/compressed_sparse_row.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/vector.hpp"
#include <duckdb/function/function.hpp>

#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"

#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/property_graph_table.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/joinref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>

namespace duckdb {
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

struct CSRFunctionData : FunctionData {
  CSRFunctionData(ClientContext &context, int32_t id, LogicalType weight_type);
  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
  static unique_ptr<FunctionData>
  CSRVertexBind(ClientContext &context, ScalarFunction &bound_function,
                vector<unique_ptr<Expression>> &arguments);
  static unique_ptr<FunctionData>
  CSREdgeBind(ClientContext &context, ScalarFunction &bound_function,
              vector<unique_ptr<Expression>> &arguments);
  static unique_ptr<FunctionData>
  CSRBind(ClientContext &context, ScalarFunction &bound_function,
          vector<unique_ptr<Expression>> &arguments);

  ClientContext &context;
  const int32_t id;
  const LogicalType weight_type;
};

// CSR BindReplace functions
unique_ptr<CommonTableExpressionInfo> CreateUndirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table);
 unique_ptr<CommonTableExpressionInfo> CreateDirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table, const string &prev_binding, const string &edge_binding, const string &next_binding);

// Helper functions
unique_ptr<JoinRef> GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table,const string &edge_binding, const string &prev_binding, const string &next_binding);
unique_ptr<SubqueryExpression> GetCountTable(const shared_ptr<PropertyGraphTable> &edge_table, const string &prev_binding);
unique_ptr<ColumnRefExpression> CreateColumnRef(const std::string &column_name, const std::string &table_name, const std::string &alias);
void SetupSelectNode(unique_ptr<SelectNode> &select_node, const shared_ptr<PropertyGraphTable> &edge_table, bool reverse = false);
} // namespace duckdb
