//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/utils/compressed_sparse_row.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"

#include "duckdb/parser/property_graph_table.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckpgq/common.hpp"
#include <vector>
#include <iostream>
#include <limits>
#include <cstdint>
#include <type_traits>


namespace duckpgq {

namespace core {

class LocalCSR {
public:
  explicit LocalCSR(idx_t start_vertex_p, idx_t end_vertex_p, size_t number_of_vertices)
      : start_vertex(start_vertex_p), end_vertex(end_vertex_p), v_array_size(number_of_vertices + 2) {
    v = new std::atomic<uint32_t>[v_array_size](); // Zero-initialize
    initialized_v = true;
  }

  ~LocalCSR() {
    delete[] v;
  }

  std::atomic<uint32_t>* GetVertexArray() { return v; }
  std::vector<uint16_t>* GetEdgeVector() { return &e; }

  string ToString() const;

  size_t GetVertexSize() const { return v_array_size - 2; }
  size_t GetEdgeSize() const { return e.size(); }

  bool PartitioningDone(size_t partition_size) const {
    return GetEdgeSize() <= partition_size;
  }

  std::atomic<uint32_t>* v{};
  size_t v_array_size;
  std::vector<uint16_t> e;

  idx_t start_vertex;
  idx_t end_vertex;
  bool initialized_v = false;
  bool initialized_e = false;
};

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
unique_ptr<CommonTableExpressionInfo>
CreateUndirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                       const unique_ptr<SelectNode> &select_node);
unique_ptr<CommonTableExpressionInfo>
CreateDirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                     const string &prev_binding, const string &edge_binding,
                     const string &next_binding);

// Helper functions
unique_ptr<CommonTableExpressionInfo>
MakeEdgesCTE(const shared_ptr<PropertyGraphTable> &edge_table);
unique_ptr<SubqueryExpression> CreateDirectedCSRVertexSubquery(
    const shared_ptr<PropertyGraphTable> &edge_table, const string &binding);
unique_ptr<SubqueryExpression> CreateUndirectedCSRVertexSubquery(
    const shared_ptr<PropertyGraphTable> &edge_table, const string &binding);
unique_ptr<SelectNode> CreateOuterSelectEdgesNode();
unique_ptr<SelectNode>
CreateOuterSelectNode(unique_ptr<FunctionExpression> create_csr_edge_function);
unique_ptr<JoinRef> GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table,
                               const string &edge_binding,
                               const string &prev_binding,
                               const string &next_binding);
unique_ptr<SubqueryExpression>
GetCountTable(const shared_ptr<PropertyGraphTable> &table,
              const string &table_alias, const string &primary_key);
void SetupSelectNode(unique_ptr<SelectNode> &select_node,
                     const shared_ptr<PropertyGraphTable> &edge_table,
                     bool reverse = false);
unique_ptr<SubqueryRef> CreateCountCTESubquery();
unique_ptr<SubqueryExpression> GetCountUndirectedEdgeTable();
unique_ptr<SubqueryExpression>
GetCountEdgeTable(const shared_ptr<PropertyGraphTable> &edge_table);

int32_t GetCSRId(const unique_ptr<Expression> &expr, ClientContext &context);
} // namespace core

} // namespace duckpgq
