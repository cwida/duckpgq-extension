#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include <duckpgq/core/functions/function_data/triangle_counting_function_data.hpp>
#include <duckpgq/core/functions/scalar.hpp>
#include <duckpgq/core/functions/table/triangle_counting.hpp>
#include <duckpgq/core/utils/duckpgq_bitmap.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {

static void TriangleCountingFunction(DataChunk &args, ExpressionState &state,
                                     Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (TriangleCountingFunctionData &)*func_expr.bind_info;
  auto duckpgq_state = GetDuckPGQState(info.context);

  auto csr_entry = duckpgq_state->csr_list.find((uint64_t)info.csr_id);
  if (csr_entry == duckpgq_state->csr_list.end()) {
    throw ConstraintException("CSR not found. Is the graph populated?");
  }

  if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e)) {
    throw ConstraintException("Need to initialize CSR before counting triangles.");
  }

  int64_t *v = (int64_t *)duckpgq_state->csr_list[info.csr_id]->v;
  vector<int64_t> &e = duckpgq_state->csr_list[info.csr_id]->e;

  // Get source vector for searches
  auto &src = args.data[1];
  UnifiedVectorFormat vdata_src;
  src.ToUnifiedFormat(args.size(), vdata_src);
  auto src_data = (int64_t *)vdata_src.data;
  ValidityMask &result_validity = FlatVector::Validity(result);

  // Create result vector
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<int64_t>(result);

  // Process each source
  for (idx_t i = 0; i < args.size(); i++) {
    int64_t src_pos = vdata_src.sel->get_index(i);
    int64_t src_node = src_data[src_pos];

    if (!vdata_src.validity.RowIsValid(src_pos)) {
      result_validity.SetInvalid(i);
      result_data[i] = 0;
      continue;
    }

    // Initialize triangle count for the source
    int64_t triangle_count = 0;

    // Get the neighbors of the source node
    int64_t src_start = v[src_node];
    int64_t src_end = v[src_node + 1];

    // Iterate over all neighbor pairs to count triangles
    for (int64_t ni = src_start; ni < src_end; ni++) {
      int64_t neighbor1 = e[ni];
      int64_t neighbor1_start = v[neighbor1];
      int64_t neighbor1_end = v[neighbor1 + 1];

      for (int64_t nj = ni + 1; nj < src_end; nj++) {
        int64_t neighbor2 = e[nj];
        int64_t neighbor2_start = v[neighbor2];
        int64_t neighbor2_end = v[neighbor2 + 1];

        // Use two-pointer intersection to count common neighbors
        int64_t p1 = neighbor1_start;
        int64_t p2 = neighbor2_start;
        while (p1 < neighbor1_end && p2 < neighbor2_end) {
          int64_t n1 = e[p1];
          int64_t n2 = e[p2];

          // SIMD-friendly comparison to avoid branching
          bool match = (n1 == n2);
          triangle_count += match;
          p1 += (n1 <= n2);
          p2 += (n1 >= n2);
        }
      }
    }

    // Store the result
    result_data[i] = triangle_count;
  }

  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterTriangleCountingScalarFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(
      db, ScalarFunction("triangle_counting",
                         {LogicalType::INTEGER, LogicalType::BIGINT},
                         LogicalType::BIGINT, TriangleCountingFunction,
                         TriangleCountingFunctionData::TriangleCountingBind));
}

} // namespace core
} // namespace duckpgq