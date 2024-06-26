//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/compressed_sparse_row.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"

namespace duckdb {

class DuckPGQBitmap {
public:
  explicit DuckPGQBitmap(size_t size);
  void set(size_t index);
  bool test(size_t index) const;
  void reset();

private:
  size_t size;
  std::vector<uint64_t> bitmap;

};



struct CSRFunctionData : public FunctionData {
public:
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

public:
  ClientContext &context;
  const int32_t id;
  const LogicalType weight_type; // TODO Make sure type is LogicalType::SQLNULL
                                 // when no type is provided
};

struct LocalClusteringCoefficientFunctionData : public FunctionData {
public:
  ClientContext &context;
  int32_t csr_id;

  LocalClusteringCoefficientFunctionData(ClientContext &context, int32_t csr_id)
      : context(context), csr_id(csr_id) {}
  static unique_ptr<FunctionData>
  LocalClusteringCoefficientBind(ClientContext &context, ScalarFunction &bound_function,
                      vector<unique_ptr<Expression>> &arguments);

  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
};


struct IterativeLengthFunctionData : public FunctionData {
public:
  ClientContext &context;
  int32_t csr_id;

  IterativeLengthFunctionData(ClientContext &context, int32_t csr_id)
      : context(context), csr_id(csr_id) {}
  static unique_ptr<FunctionData>
  IterativeLengthBind(ClientContext &context, ScalarFunction &bound_function,
                      vector<unique_ptr<Expression>> &arguments);

  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
};

struct CheapestPathLengthFunctionData : public FunctionData {
  ClientContext &context;
  int32_t csr_id;

  CheapestPathLengthFunctionData(ClientContext &context, int32_t csr_id)
      : context(context), csr_id(csr_id) {}
  static unique_ptr<FunctionData>
  CheapestPathLengthBind(ClientContext &context, ScalarFunction &bound_function,
                         vector<unique_ptr<Expression>> &arguments);

  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
};

} // namespace duckdb
