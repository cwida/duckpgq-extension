#include "chrono"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/duckpgq_functions.hpp"
#include "duckpgq/utils/duckpgq_bitmap.hpp"
#include "duckpgq/utils/duckpgq_utils.hpp"
#include "duckpgq_extension.hpp"

#include "duckpgq/functions/function_data/local_clustering_coefficient_function_data.hpp"

namespace duckdb {
static void LocalClusteringCoefficientFunction(DataChunk &args, ExpressionState &state,
                                    Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (LocalClusteringCoefficientFunctionData &)*func_expr.bind_info;
  auto duckpgq_state = GetDuckPGQState(info.context);

  auto csr_entry = duckpgq_state->csr_list.find((uint64_t)info.csr_id);
  if (csr_entry == duckpgq_state->csr_list.end()) {
    throw ConstraintException(
        "CSR not found. Is the graph populated?");
  }

  if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e)) {
    throw ConstraintException(
        "Need to initialize CSR before doing local clustering coefficient.");
  }
  int64_t *v = (int64_t *)duckpgq_state->csr_list[info.csr_id]->v;
  vector<int64_t> &e = duckpgq_state->csr_list[info.csr_id]->e;
  size_t v_size = duckpgq_state->csr_list[info.csr_id]->vsize;
  // get src and dst vectors for searches
  auto &src = args.data[1];
  UnifiedVectorFormat vdata_src;
  src.ToUnifiedFormat(args.size(), vdata_src);
  auto src_data = (int64_t *)vdata_src.data;

  ValidityMask &result_validity = FlatVector::Validity(result);
  // create result vector
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<float_t>(result);

  DuckPGQBitmap neighbors(v_size);


  for (int32_t n = 0; n < args.size(); n++) {
    auto src_sel = vdata_src.sel->get_index(n);
    if (!vdata_src.validity.RowIsValid(src_sel)) {
      result_validity.SetInvalid(n);
    }
    int64_t src_node = src_data[src_sel];
    int64_t number_of_edges = v[src_node + 1] - v[src_node];
    if (number_of_edges < 2) {
      result_data[n] = static_cast<float_t>(0.0);
      continue;
    }
    neighbors.reset();
    for (size_t offset = v[src_node]; offset < v[src_node + 1]; offset++) {
      neighbors.set(e[offset]);
    }

    // Count connections between neighbors
    int64_t count = 0;
    for (size_t offset = v[src_node]; offset < v[src_node + 1]; offset++) {
      int64_t neighbor = e[offset];
      for (size_t offset2 = v[neighbor]; offset2 < v[neighbor + 1]; offset2++) {
        int is_connected = neighbors.test(e[offset2]);
        count += is_connected; // Add 1 if connected, 0 otherwise
      }
    }

    float_t local_result =  static_cast<float_t>(count) / (number_of_edges * (number_of_edges - 1));
    result_data[n] = local_result;
  }
  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

CreateScalarFunctionInfo DuckPGQFunctions::GetLocalClusteringCoefficientFunction() {
  auto fun = ScalarFunction("local_clustering_coefficient",
                            {LogicalType::INTEGER, LogicalType::BIGINT},
                            LogicalType::FLOAT, LocalClusteringCoefficientFunction,
                            LocalClusteringCoefficientFunctionData::LocalClusteringCoefficientBind);
  return CreateScalarFunctionInfo(fun);
}

}

