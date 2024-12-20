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
    throw ConstraintException(
        "Need to initialize CSR before counting triangles.");
  }

  int64_t *v = (int64_t *)duckpgq_state->csr_list[info.csr_id]->v;
  vector<int64_t> &e = duckpgq_state->csr_list[info.csr_id]->e;
  size_t v_size = duckpgq_state->csr_list[info.csr_id]->vsize;

  // Get source vector for searches
  auto &src = args.data[1];
  UnifiedVectorFormat vdata_src;
  src.ToUnifiedFormat(args.size(), vdata_src);
  auto src_data = (int64_t *)vdata_src.data;
  ValidityMask &result_validity = FlatVector::Validity(result);

  // Create result vector
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<int64_t>(result);

  // Temporary bitsets for batch processing
  vector<std::bitset<LANE_LIMIT>> active(v_size);
  vector<std::bitset<LANE_LIMIT>> seen(v_size);

  // Map lanes to source indices
  short lane_to_num[LANE_LIMIT];
  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    lane_to_num[lane] = -1; // Mark as inactive
  }

  vector<int64_t> triangle_counts(args.size(), 0);

  idx_t started_searches = 0;
  while (started_searches < args.size()) {
    // Reset bitsets
    for (size_t i = 0; i < v_size; i++) {
      active[i].reset();
      seen[i].reset();
    }

    // Assign free lanes to new sources
    uint64_t active_lanes = 0;
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      lane_to_num[lane] = -1;
      while (started_searches < args.size()) {
        int64_t search_num = started_searches++;
        int64_t src_pos = vdata_src.sel->get_index(search_num);
        int64_t src_node = src_data[src_pos];
        if (!vdata_src.validity.RowIsValid(src_pos)) {
          result_validity.SetInvalid(search_num);
          result_data[search_num] = (uint64_t)-1;
          continue;
        }

        // Initialize active set for this source
        active[src_node][lane] = true;
        lane_to_num[lane] = search_num; // Map lane to source index
        active_lanes++;
        break;
      }
    }

    // Process active vertices
    while (active_lanes > 0) {
      for (size_t u = 0; u < v_size; u++) {
        if (!active[u].any()) {
          continue;
        }

        // Mark vertex as seen
        seen[u] |= active[u];

        // Iterate over neighbors of vertex u
        for (int64_t ei = v[u]; ei < v[u + 1]; ei++) {
          int64_t neighbor = e[ei];

          // Skip already seen vertices
          if ((seen[neighbor] & active[u]).any()) {
            continue;
          }

          // Count triangles for all active lanes
          for (int64_t ej = ei + 1; ej < v[u + 1]; ej++) {
            int64_t neighbor2 = e[ej];

            // Find common neighbors between neighbor1 and neighbor2 for active
            // lanes
            for (int64_t ek = v[neighbor]; ek < v[neighbor + 1]; ek++) {
              int64_t neighbor3 = e[ek];
              if (neighbor3 == neighbor2) {
                for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
                  if ((active[u] & active[neighbor2]).test(lane)) {
                    triangle_counts[lane_to_num[lane]]++;
                  }
                }
              }
            }
          }

          // Mark the neighbor as active for the next iteration
          active[neighbor] |= active[u];
        }
      }

      // Clear inactive lanes
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        int64_t search_num = lane_to_num[lane];
        if (search_num == -1) {
          continue;
        }
        if (!active[search_num].any()) {
          lane_to_num[lane] = -1;
          active_lanes--;
        }
      }
    }
  }

  // Assign triangle counts to result
  for (idx_t i = 0; i < args.size(); i++) {
    int64_t src_pos = vdata_src.sel->get_index(i);
    if (!vdata_src.validity.RowIsValid(src_pos)) {
      result_validity.SetInvalid(i);
    } else {
      result_data[i] = triangle_counts[i];
    }
  }

  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterWeaklyConnectedComponentScalarFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(
      db, ScalarFunction("weakly_connected_component",
                         {LogicalType::INTEGER, LogicalType::BIGINT},
                         LogicalType::BIGINT, TriangleCountingFunction,
                         TriangleCountingFunctionData::TriangleCountingBind));
}

} // namespace core
} // namespace duckpgq