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
} // namespace duckdb
