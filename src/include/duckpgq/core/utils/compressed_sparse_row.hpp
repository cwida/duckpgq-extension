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
  explicit LocalCSR(int64_t esize)
      : data_type(DetermineType(esize)) {
  }

  // Returns a pointer to the correct vector
  void* GetVertexVector() {
    if (data_type == 16) return &v_int16;
    if (data_type == 32) return &v_int32;
    return &v_int64;
  }

  void* GetEdgeVector() {
    if (data_type == 16) return &e_int16;
    if (data_type == 32) return &e_int32;
    return &e_int64;
  }

  void PrintInfo() const {
    std::cout << "Data storage type: int" << data_type << "_t\n";
  }

  size_t GetVertexSize() {
    if (data_type == 16) return v_int16.size() - 2;
    if (data_type == 32) return v_int32.size() - 2;
    return v_int64.size() - 2;
  }

  size_t GetEdgeSize() {
    if (data_type == 16) return e_int16.size();
    if (data_type == 32) return e_int32.size();
    return e_int64.size();
  }

  template <typename T>
  std::vector<T>& GetVertexVectorTyped() {
    if (data_type == 16 && std::is_same<T, int16_t>::value) return reinterpret_cast<std::vector<T>&>(v_int16);
    if (data_type == 32 && std::is_same<T, int32_t>::value) return reinterpret_cast<std::vector<T>&>(v_int32);
    if (data_type == 64 && std::is_same<T, int64_t>::value) return reinterpret_cast<std::vector<T>&>(v_int64);
    throw std::runtime_error("Requested type does not match stored type");
  }

  template <typename T>
  std::vector<T>& GetEdgeVectorTyped() {
    if (data_type == 16 && std::is_same<T, int16_t>::value) return reinterpret_cast<std::vector<T>&>(e_int16);
    if (data_type == 32 && std::is_same<T, int32_t>::value) return reinterpret_cast<std::vector<T>&>(e_int32);
    if (data_type == 64 && std::is_same<T, int64_t>::value) return reinterpret_cast<std::vector<T>&>(e_int64);
    throw std::runtime_error("Requested type does not match stored type");
  }
public:
  std::vector<int16_t> v_int16, e_int16;
  std::vector<int32_t> v_int32, e_int32;
  std::vector<int64_t> v_int64, e_int64;


  static int DetermineType(size_t max_value) {
    if (max_value <= std::numeric_limits<int16_t>::max()) return 16;
    if (max_value <= std::numeric_limits<int32_t>::max()) return 32;
    return 64;
  }
public:
  int data_type;  // Stores the type of each array
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
