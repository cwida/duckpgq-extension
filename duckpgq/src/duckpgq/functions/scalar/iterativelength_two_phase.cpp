#include <duckpgq_extension.hpp>
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/duckpgq_functions.hpp"

namespace duckdb {

static bool IterativeLengthPhaseOne(int64_t v_size, int64_t *v, vector<int64_t> &e,
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
    change |= next[i].any();
  }
  
  return change;
}

static bool IterativeLengthPhaseTwo(int64_t v_size, int64_t *v, vector<int64_t> &e,
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

static int64_t IterativeLengthInternal(int64_t lane, int64_t v_size, int64_t destination, 
                                       int64_t bound, 
                                       int64_t *v, vector<int64_t> &e, 
                                       vector<std::bitset<LANE_LIMIT>> &visit) {
  vector<int64_t> src;
  for (int64_t v = 0; v < v_size; v++) {
    if (visit[v][lane]) {
      src.push_back(v);
    }
  }
  vector<std::bitset<LANE_LIMIT>> seen(v_size);
  vector<std::bitset<LANE_LIMIT>> visit1(v_size);
  vector<std::bitset<LANE_LIMIT>> visit2(v_size);

  idx_t started_searches = 0;
  while (started_searches < src.size()) {
    for (auto i = 0; i < v_size; i++) {
      seen[i] = 0;
      visit1[i] = 0;
    }
    // add search jobs to free lanes
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      while (started_searches < src.size()) {
        int64_t search_num = started_searches++;
        visit1[src[search_num]][lane] = true;
      }
    }

    for (int64_t iter = 1; iter <= bound; iter++) {
      if (!IterativeLengthPhaseTwo(v_size, v, e, seen, (iter & 1) ? visit1 : visit2,
                                   (iter & 1) ? visit2 : visit1)) {
        break;
      }
      // detect lanes that found the destination
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        if (seen[destination][lane]) {
          return iter;
        }
      }
    }
  }
  return -1;
}

static void IterativeLengthLowerBoundFunction(DataChunk &args, ExpressionState &state,
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
  int64_t v_size = args.data[1].GetValue(0).GetValue<int64_t>();
  int64_t *v = (int64_t *)duckpgq_state->csr_list[info.csr_id]->v;
  vector<int64_t> &e = duckpgq_state->csr_list[info.csr_id]->e;

  // get src and dst vectors for searches
  auto &src = args.data[2];
  auto &dst = args.data[3];
  UnifiedVectorFormat vdata_src;
  UnifiedVectorFormat vdata_dst;
  src.ToUnifiedFormat(args.size(), vdata_src);
  dst.ToUnifiedFormat(args.size(), vdata_dst);
  auto src_data = (int64_t *)vdata_src.data;
  auto dst_data = (int64_t *)vdata_dst.data;

  // get lowerbound and upperbound
  auto &lower = args.data[4];
  auto &upper = args.data[5];
  UnifiedVectorFormat vdata_lower_bound;
  UnifiedVectorFormat vdata_upper_bound;
  lower.ToUnifiedFormat(args.size(), vdata_lower_bound);
  upper.ToUnifiedFormat(args.size(), vdata_upper_bound);
  auto lower_bound = ((int64_t *)vdata_lower_bound.data)[0];
  auto upper_bound = ((int64_t *)vdata_upper_bound.data)[0];

  ValidityMask &result_validity = FlatVector::Validity(result);

  // create result vector
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<int64_t>(result);

  // create temp SIMD arrays
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
    for (auto i = 0; i < v_size; i++) {
      visit1[i] = 0;
    }

    // add search jobs to free lanes
    uint64_t active = 0;
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      lane_to_num[lane] = -1;
      while (started_searches < args.size()) {
        int64_t search_num = started_searches++;
        int64_t src_pos = vdata_src.sel->get_index(search_num);
        if (!vdata_src.validity.RowIsValid(src_pos)) {
          result_validity.SetInvalid(search_num);
          result_data[search_num] = (int64_t)-1; /* no path */
        } else {
          result_data[search_num] = (int64_t)-1; /* initialize to no path */
          visit1[src_data[src_pos]][lane] = true;
          lane_to_num[lane] = search_num; // active lane
          active++;
          break;
        }
      }
    }

    int64_t iter = 1;
    // phase one: search without seen until lower bound - 1
    for (; iter < lower_bound; iter++) {
      IterativeLengthPhaseOne(v_size, v, e, (iter & 1) ? visit1 : visit2,
                              (iter & 1) ? visit2 : visit1);
    }

    // phase two: search with seen until upper bound
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      auto search_num = lane_to_num[lane];
      if (search_num >= 0) {
        int64_t dst_pos = vdata_dst.sel->get_index(search_num);
        auto length = IterativeLengthInternal(lane, v_size, 
          dst_data[dst_pos], upper_bound - lower_bound + 1, v, e, (iter & 1) ? visit1 : visit2);
        if (length >= 0) {
          result_data[search_num] = length + lower_bound - 1;
          lane_to_num[lane] = -1; // mark inactive
        }
      }
    }

    // no changes anymore: any still active searches have no path
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = lane_to_num[lane];
      if (search_num >= 0) { // active lane
        result_validity.SetInvalid(search_num);
        result_data[search_num] = (int64_t)-1; /* no path */
        lane_to_num[lane] = -1;                // mark inactive
      }
    }
  }
  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

CreateScalarFunctionInfo 
DuckPGQFunctions::GetIterativeLengthLowerBoundFunction() {
  auto fun = ScalarFunction(
      "iterativelength_lowerbound",
      {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, 
        LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
      LogicalType::BIGINT, IterativeLengthLowerBoundFunction,
      IterativeLengthFunctionData::IterativeLengthBind);
  return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb
