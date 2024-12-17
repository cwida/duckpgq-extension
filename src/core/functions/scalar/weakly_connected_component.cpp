#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/functions/function_data/local_clustering_coefficient_function_data.hpp"
#include <duckpgq/core/functions/function_data/weakly_connected_component_function_data.hpp>
#include <duckpgq/core/functions/scalar.hpp>
#include <duckpgq/core/functions/table/weakly_connected_component.hpp>
#include <duckpgq/core/utils/duckpgq_bitmap.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {
namespace core {

void InitializeComponentVector(size_t v_count,
                               WeaklyConnectedComponentFunctionData &info) {
  if (info.component_id_initialized) {
    return;
  }
  info.component_lock.lock();
  info.componentId.reserve(v_count);
  for (idx_t i = 0; i < v_count; i++) {
    info.componentId.push_back(-1);
  }
  info.component_id_initialized = true;
  info.component_lock.unlock();
}

static bool IterativeLength(int64_t v_size, int64_t *v, vector<int64_t> &e,
                            vector<std::bitset<LANE_LIMIT>> &seen,
                            vector<std::bitset<LANE_LIMIT>> &visit,
                            vector<std::bitset<LANE_LIMIT>> &next) {
  bool change = false;
  for (auto i = 0; i < v_size; i++) {
    next[i] = 0;
  }
  for (auto i = 0; i < v_size; i++) {
    if (visit[i].any()) {
      for (auto offset = v[i]; offset < v[i + 1]; offset++) {
        auto n = e[offset];
        next[n] = next[n] | visit[i];
      }
    }
  }
  for (auto i = 0; i < v_size; i++) {
    next[i] = next[i] & ~seen[i];
    seen[i] = seen[i] | next[i];
    change |= next[i].any();
  }
  return change;
}

static void UpdateComponentId(int64_t node, int64_t component_id,
                              WeaklyConnectedComponentFunctionData &info) {
  if (info.componentId[node] == -1) {
    info.componentId[node] = component_id;
  } else {
    info.componentId[node] = std::min(info.componentId[node], component_id);
  }
}

static void WeaklyConnectedComponentFunction(DataChunk &args,
                                             ExpressionState &state,
                                             Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (WeaklyConnectedComponentFunctionData &)*func_expr.bind_info;
  auto duckpgq_state = GetDuckPGQState(info.context);

  auto csr_entry = duckpgq_state->csr_list.find((uint64_t)info.csr_id);
  if (csr_entry == duckpgq_state->csr_list.end()) {
    throw ConstraintException("CSR not found. Is the graph populated?");
  }

  if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e)) {
    throw ConstraintException(
        "Need to initialize CSR before doing local clustering coefficient.");
  }
  int64_t *v = (int64_t *)duckpgq_state->csr_list[info.csr_id]->v;
  vector<int64_t> &e = duckpgq_state->csr_list[info.csr_id]->e;
  size_t v_size = duckpgq_state->csr_list[info.csr_id]->vsize;
  // get src vector for searches
  auto &src = args.data[1];
  UnifiedVectorFormat vdata_src;
  src.ToUnifiedFormat(args.size(), vdata_src);
  auto src_data = (int64_t *)vdata_src.data;
  ValidityMask &result_validity = FlatVector::Validity(result);
  // create result vector
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<int64_t>(result);
  InitializeComponentVector(v_size, info);

  // create temp SIMD arrays
  vector<std::bitset<LANE_LIMIT>> seen(v_size);
  vector<std::bitset<LANE_LIMIT>> visit1(v_size);
  vector<std::bitset<LANE_LIMIT>> visit2(v_size);

  // maps lane to search number
  short lane_to_num[LANE_LIMIT];
  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    lane_to_num[lane] = -1; // inactive
  }

  idx_t started_searches = 0;
  while (started_searches < args.size()) {
    // empty visit vectors
    for (size_t i = 0; i < v_size; i++) {
      seen[i] = 0;
      visit1[i] = 0;
    }

    // add search jobs to free lanes
    uint64_t active = 0;
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      lane_to_num[lane] = -1;
      while (started_searches < args.size()) {
        int64_t search_num = started_searches++;
        int64_t src_pos = vdata_src.sel->get_index(search_num);
        int64_t src_node = src_data[src_pos];
        // Check if the node is already part of a component
        if (info.componentId[src_node] != -1) {
          result_data[search_num] =
              info.componentId[src_node]; // Already known component
          continue;
        }

        if (!vdata_src.validity.RowIsValid(src_pos)) {
          result_validity.SetInvalid(search_num);
          result_data[search_num] = (uint64_t)-1; /* no path */
        } else {
          visit1[src_data[src_pos]][lane] = true;
          lane_to_num[lane] = search_num; // active lane
          active++;
          break;
        }
      }
    }

    // make passes while a lane is still active
    for (int64_t iter = 1; active; iter++) {
      if (!IterativeLength(v_size, v, e, seen, (iter & 1) ? visit1 : visit2,
                           (iter & 1) ? visit2 : visit1)) {
        break;
      }
      // detect lanes that finished
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        int64_t search_num = lane_to_num[lane];
        if (search_num == -1) {
          continue;
        }
        // Update component IDs
        for (size_t i = 0; i < v_size; i++) {
          if (seen[i][lane]) {
            UpdateComponentId(i, src_data[search_num], info);
          }
        }
      }
    }
  }
  // Assign component IDs to result
  for (idx_t i = 0; i < args.size(); i++) {
    int64_t src_pos = vdata_src.sel->get_index(i);
    if (!vdata_src.validity.RowIsValid(src_pos)) {
      result_validity.SetInvalid(i);
    } else {
      auto component_id = info.componentId[src_data[src_pos]];
      if (component_id == -1) {
        info.componentId[src_data[src_pos]] = src_data[src_pos];
      }
      result_data[i] = info.componentId[src_data[src_pos]];
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
      db,
      ScalarFunction(
          "weakly_connected_component",
          {LogicalType::INTEGER, LogicalType::BIGINT}, LogicalType::BIGINT,
          WeaklyConnectedComponentFunction,
          WeaklyConnectedComponentFunctionData::WeaklyConnectedComponentBind));
}

} // namespace core
} // namespace duckpgq