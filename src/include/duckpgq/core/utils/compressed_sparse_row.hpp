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
  explicit LocalCSR(int64_t vsize, int64_t esize, size_t num_vertices, size_t num_edges)
      : v_type(DetermineType(vsize)), e_type(DetermineType(esize)) {

    // Allocate the correct vector based on type
    if (v_type == 16) {
      v_int16.resize(num_vertices);
    } else if (v_type == 32) {
      v_int32.resize(num_vertices);
    } else {
      v_int64.resize(num_vertices);
    }

    if (e_type == 16) {
      e_int16.resize(num_edges);
    } else if (e_type == 32) {
      e_int32.resize(num_edges);
    } else {
      e_int64.resize(num_edges);
    }
  }

  void PrintInfo() const {
    std::cout << "Vertex storage type: int" << v_type << "_t\n";
    std::cout << "Edge storage type: int" << e_type << "_t\n";
  }

  size_t GetVertexSize() {
    if (v_type == 16) return v_int16.size() - 2;
    if (v_type == 32) return v_int32.size() - 2;
    return v_int64.size() - 2;
  }

  size_t GetEdgeSize() {
    if (e_type == 16) return e_int16.size();
    if (e_type == 32) return e_int32.size();
    return e_int64.size();
  }

private:
  std::vector<int16_t> v_int16, e_int16;
  std::vector<int32_t> v_int32, e_int32;
  std::vector<int64_t> v_int64, e_int64;

  int v_type, e_type;  // Stores the type of each array

  static int DetermineType(size_t max_value) {
    if (max_value <= std::numeric_limits<int16_t>::max()) return 16;
    if (max_value <= std::numeric_limits<int32_t>::max()) return 32;
    return 64;
  }
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

// CSR BindReplace functions
unique_ptr<CommonTableExpressionInfo> CreateUndirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                       const unique_ptr<SelectNode> &select_node);
unique_ptr<CommonTableExpressionInfo> CreateDirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table, const string &prev_binding, const string &edge_binding, const string &next_binding);

// Helper functions
unique_ptr<CommonTableExpressionInfo> MakeEdgesCTE(const shared_ptr<PropertyGraphTable> &edge_table);
unique_ptr<SubqueryExpression> CreateDirectedCSRVertexSubquery(const shared_ptr<PropertyGraphTable> &edge_table, const string &binding);
unique_ptr<SubqueryExpression> CreateUndirectedCSRVertexSubquery(const shared_ptr<PropertyGraphTable> &edge_table, const string &binding);
unique_ptr<SelectNode> CreateOuterSelectEdgesNode();
unique_ptr<SelectNode> CreateOuterSelectNode(unique_ptr<FunctionExpression> create_csr_edge_function);
unique_ptr<JoinRef> GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table,const string &edge_binding, const string &prev_binding, const string &next_binding);
unique_ptr<SubqueryExpression> GetCountTable(const shared_ptr<PropertyGraphTable> &table, const string &table_alias, const string &primary_key);
void SetupSelectNode(unique_ptr<SelectNode> &select_node, const shared_ptr<PropertyGraphTable> &edge_table, bool reverse = false);
int32_t GetCSRId(const unique_ptr<Expression> &expr, ClientContext &context);

unique_ptr<SubqueryRef> CreateCountCTESubquery();
unique_ptr<SubqueryExpression> GetCountUndirectedEdgeTable();
unique_ptr<SubqueryExpression> GetCountEdgeTable(const shared_ptr<PropertyGraphTable> &edge_table);
} // namespace core

} // namespace duckpgq
