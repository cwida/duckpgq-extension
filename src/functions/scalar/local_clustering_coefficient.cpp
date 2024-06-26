#include <duckpgq_extension.hpp>
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/duckpgq_functions.hpp"


namespace duckdb {
static void LocalClusteringCoefficientFunction(DataChunk &args, ExpressionState &state,
                                    Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (IterativeLengthFunctionData &)*func_expr.bind_info;
  auto duckpgq_state_entry = info.context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == info.context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());

  D_ASSERT(duckpgq_state->csr_list[info.csr_id]);

  if ((uint64_t)info.csr_id + 1 > duckpgq_state->csr_list.size()) {
    throw ConstraintException("Invalid ID");
  }
  auto csr_entry = duckpgq_state->csr_list.find((uint64_t)info.csr_id);
  if (csr_entry == duckpgq_state->csr_list.end()) {
    throw ConstraintException(
        "Need to initialize CSR before doing shortest path");
  }

  if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e)) {
    throw ConstraintException(
        "Need to initialize CSR before doing shortest path");
  }
  int64_t *v = (int64_t *)duckpgq_state->csr_list[info.csr_id]->v;
  vector<int64_t> &e = duckpgq_state->csr_list[info.csr_id]->e;
  // get src and dst vectors for searches
  auto &src = args.data[1];
  UnifiedVectorFormat vdata_src;
  src.ToUnifiedFormat(args.size(), vdata_src);
  auto src_data = (int64_t *)vdata_src.data;

  ValidityMask &result_validity = FlatVector::Validity(result);
  // create result vector
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<float_t>(result);

  for (int32_t n = 0; n < args.size(); n++) {
    auto src_sel = vdata_src.sel->get_index(n);
    int64_t src_node = src_data[src_sel];
    int32_t number_of_edges = v[src_node + 1] - v[src_node];
    if (number_of_edges < 2) {
      result_data[n] = static_cast<float_t>(0.0);
      continue;
    }
    int32_t count = 0;
    for (auto offset = v[src_node]; offset < v[src_node + 1]; offset++) {
      auto neighbour = e[offset];
      for (auto offset2 = v[neighbour]; offset2 < v[neighbour + 1]; offset2++) {
        auto n2 = e[offset2];
        for (auto offset_src = v[src_node]; offset_src < v[src_node + 1]; offset_src++) {
          if (n2 == e[offset_src]) {
            count++;
            break;
          }
        }
      }
    }
    float_t local_result =  static_cast<float_t>(count) / (number_of_edges * (number_of_edges - 1));
    std::cout << src_node << " " << count << " " << number_of_edges << " " << local_result << std::endl;
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

