#include "duckdb/common/fstream.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/duckpgq_functions.hpp"

#include <algorithm>
#include <duckpgq_extension.hpp>

namespace duckdb {

static bool IterativeLengthPhaseOne(int64_t v_size, int64_t *V, vector<int64_t> &E,
                                    int64_t iter, vector<int64_t> &edge_ids,
                                    vector<vector<unordered_map<int64_t, int64_t>>> &paths_v,
                                    vector<vector<unordered_map<int64_t, int64_t>>> &paths_e,
                                    vector<std::bitset<LANE_LIMIT>> &visit,
                                    vector<std::bitset<LANE_LIMIT>> &next) {
  bool change = false;
  for (auto v = 0; v < v_size; v++) {
    next[v] = 0;
  }
  //! Keep track of edge id through which the node was reached
  for (auto v = 0; v < v_size; v++) {
    if (visit[v].any()) {
      for (auto e = V[v]; e < V[v + 1]; e++) {
        auto n = E[e];
        auto edge_id = edge_ids[e];
        next[n] = next[n] | visit[v];
        for (auto lane = 0; lane < LANE_LIMIT; lane++) {
          if (visit[v][lane]) {
            paths_v[n][lane][iter] = v;
            paths_e[n][lane][iter] = edge_id;
          }
        }
      }
    }
  }

  for (auto v = 0; v < v_size; v++) {
    change |= next[v].any();
  }
  return change;
}

static bool IterativeLengthPhaseTwo(int64_t v_size, int64_t *V, vector<int64_t> &E,
                                    vector<int64_t> &edge_ids,
                                    vector<std::vector<int64_t>> &parents_v,
                                    vector<std::vector<int64_t>> &parents_e,
                                    vector<std::bitset<LANE_LIMIT>> &seen,
                                    vector<std::bitset<LANE_LIMIT>> &visit,
                                    vector<std::bitset<LANE_LIMIT>> &next) {
  bool change = false;
  for (auto v = 0; v < v_size; v++) {
    next[v] = 0;
  }
  //! Keep track of edge id through which the node was reached
  for (auto v = 0; v < v_size; v++) {
    if (visit[v].any()) {
      for (auto e = V[v]; e < V[v + 1]; e++) {
        auto n = E[e];
        auto edge_id = edge_ids[e];
        next[n] = next[n] | visit[v];
        for (auto l = 0; l < LANE_LIMIT; l++) {
          parents_v[n][l] =
              ((parents_v[n][l] == -1) && visit[v][l]) ? v : parents_v[n][l];
          parents_e[n][l] = ((parents_e[n][l] == -1) && visit[v][l])
                                ? edge_id
                                : parents_e[n][l];
        }
      }
    }
  }

  for (auto v = 0; v < v_size; v++) {
    next[v] = next[v] & ~seen[v];
    seen[v] = seen[v] | next[v];
    change |= next[v].any();
  }
  return change;
}

static std::tuple<int64_t, vector<int64_t>> ShortestPathInternal(int64_t lane, int64_t v_size, int64_t destination, 
                                            int64_t bound, 
                                            int64_t *v, vector<int64_t> &e, vector<int64_t> &edge_ids,
                                            vector<std::bitset<LANE_LIMIT>> &visit) {
  vector<int64_t> src;
  vector<int64_t> result;
  for (int64_t v = 0; v < v_size; v++) {
    if (visit[v][lane]) {
      src.push_back(v);
    }
  }
  vector<std::bitset<LANE_LIMIT>> seen(v_size);
  vector<std::bitset<LANE_LIMIT>> visit1(v_size);
  vector<std::bitset<LANE_LIMIT>> visit2(v_size);

  vector<std::vector<int64_t>> parents_v(v_size,
                                         std::vector<int64_t>(LANE_LIMIT, -1));
  vector<std::vector<int64_t>> parents_e(v_size,
                                         std::vector<int64_t>(LANE_LIMIT, -1));


  // maps lane to search number
  int16_t lane_to_num[LANE_LIMIT];
  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    lane_to_num[lane] = -1; // inactive
  }

  idx_t started_searches = 0;
  while (started_searches < src.size()) {
    for (auto i = 0; i < v_size; i++) {
      seen[i] = 0;
      visit1[i] = 0;
      for (auto j = 0; j < LANE_LIMIT; j++) {
        parents_v[i][j] = -1;
        parents_e[i][j] = -1;
      }
    }
    // add search jobs to free lanes
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      if (started_searches < src.size()) {
        int64_t search_num = started_searches++;
        visit1[src[search_num]][lane] = true;
        lane_to_num[lane] = search_num;
      } else {
        break;
      }
    }

    for (int64_t iter = 1; iter <= bound; iter++) {
      if (!IterativeLengthPhaseTwo(v_size, v, e, edge_ids, parents_v, parents_e, 
                                   seen, (iter & 1) ? visit1 : visit2,
                                   (iter & 1) ? visit2 : visit1)) {
        break;
      }
      // detect lanes that found the destination
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        if (seen[destination][lane]) {
          auto search_num = lane_to_num[lane];

          // found the destination, reconstruct the path
          auto parent_vertex = parents_v[destination][lane];
          auto parent_edge = parents_e[destination][lane];

          result.push_back(destination);
          result.push_back(parent_edge);
          while (parent_vertex != src[search_num]) {
            
            result.push_back(parent_vertex);
            parent_edge = parents_e[parent_vertex][lane];
            parent_vertex = parents_v[parent_vertex][lane];
            result.push_back(parent_edge);
          }
          result.push_back(src[search_num]);
          std::reverse(result.begin(), result.end());
          return std::make_tuple(src[search_num], result);
        }
      }
    }
  }
  return std::make_tuple(-1, result);
}

static void ShortestPathTwoPhaseFunction(DataChunk &args, ExpressionState &state,
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
  int32_t id = args.data[0].GetValue(0).GetValue<int32_t>();
  int64_t v_size = args.data[1].GetValue(0).GetValue<int64_t>();

  int64_t *v = (int64_t *)duckpgq_state->csr_list[id]->v;
  vector<int64_t> &e = duckpgq_state->csr_list[id]->e;
  vector<int64_t> &edge_ids = duckpgq_state->csr_list[id]->edge_ids;

  auto &src = args.data[2];
  auto &target = args.data[3];

  UnifiedVectorFormat vdata_src, vdata_dst;
  src.ToUnifiedFormat(args.size(), vdata_src);
  target.ToUnifiedFormat(args.size(), vdata_dst);

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

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  ValidityMask &result_validity = FlatVector::Validity(result);

  // create temp SIMD arrays
  vector<std::bitset<LANE_LIMIT>> visit1(v_size);
  vector<std::bitset<LANE_LIMIT>> visit2(v_size);

  vector<vector<unordered_map<int64_t, int64_t>>> paths_v(v_size, 
    std::vector<unordered_map<int64_t, int64_t>>(LANE_LIMIT));
  vector<vector<unordered_map<int64_t, int64_t>>> paths_e(v_size, 
    std::vector<unordered_map<int64_t, int64_t>>(LANE_LIMIT)); 
  

  // maps lane to search number
  int16_t lane_to_num[LANE_LIMIT];
  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    lane_to_num[lane] = -1; // inactive
  }
  int64_t total_len = 0;

  idx_t started_searches = 0;
  while (started_searches < args.size()) {

    // empty visit vectors
    for (auto i = 0; i < v_size; i++) {
      visit1[i] = 0;
      for (auto j = 0; j < LANE_LIMIT; j++) {
        paths_v[i][j].clear();
        paths_v[i][j].clear();
      }
    }

    // add search jobs to free lanes
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      lane_to_num[lane] = -1;
      while (started_searches < args.size()) {
        int64_t search_num = started_searches++;
        int64_t src_pos = vdata_src.sel->get_index(search_num);
        if (!vdata_src.validity.RowIsValid(src_pos)) {
          result_validity.SetInvalid(search_num);
        } else {
          visit1[src_data[src_pos]][lane] = true;
          lane_to_num[lane] = search_num; // active lane
          break;
        }
      }
    }

    int64_t iter = 1;
    for (; iter < lower_bound; iter++) {
      if (!IterativeLengthPhaseOne(v_size, v, e, iter, edge_ids, paths_v, paths_e,
                           (iter & 1) ? visit1 : visit2,
                           (iter & 1) ? visit2 : visit1)) {
        break;
      }
    }
    if (iter == lower_bound) {
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        auto search_num = lane_to_num[lane];
        if (search_num >= 0) {
          int64_t dst_pos = vdata_dst.sel->get_index(search_num);
          auto phase_two_result = ShortestPathInternal(lane, v_size, dst_data[dst_pos], 
            upper_bound - lower_bound + 1, v, e, edge_ids, (iter & 1) ? visit1 : visit2);
          auto phase_two_src = std::get<0>(phase_two_result);
          auto phase_two_path = std::get<1>(phase_two_result);
          if (phase_two_src >= 0) {
            vector<int64_t> output_vector;
            // construct the path of phase one
            if (paths_v[phase_two_src][lane].size() > 0) {
              auto iterations = lower_bound - 1;
              auto parent_vertex = paths_v[phase_two_src][lane][iterations];
              auto parent_edge = paths_e[phase_two_src][lane][iterations];

              while (iterations > 0) {
                output_vector.push_back(parent_edge);
                output_vector.push_back(parent_vertex);
                iterations--;
                parent_edge = paths_e[parent_vertex][lane][iterations];
                parent_vertex = paths_v[parent_vertex][lane][iterations];
              }
              std::reverse(output_vector.begin(), output_vector.end());
            }

            // construct the path of phase two
            for (auto val : phase_two_path) {
              output_vector.push_back(val);
            }

            // construct the output
            auto output = make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
            for (auto val : output_vector) {
              Value value_to_insert = val;
              ListVector::PushBack(*output, value_to_insert);
            }
            result_data[search_num].length = ListVector::GetListSize(*output);
            result_data[search_num].offset = total_len;
            ListVector::Append(result, ListVector::GetEntry(*output),
                              ListVector::GetListSize(*output));
            total_len += result_data[search_num].length;
            lane_to_num[lane] = -1; // mark inactive
          } else {
            result_validity.SetInvalid(search_num);
            lane_to_num[lane] = -1; // mark inactive
          }
        }
      }
    }
    // no changes anymore: any still active searches have no path
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = lane_to_num[lane];
      if (search_num >= 0) { // active lane
        result_validity.SetInvalid(search_num);
        lane_to_num[lane] = -1;                // mark inactive
      }
    }
  }
  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

CreateScalarFunctionInfo 
DuckPGQFunctions::GetShortestPathTwoPhaseFunction() {
  auto fun = ScalarFunction(
      "shortestpath_two_phase",
      {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, 
       LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
      LogicalType::LIST(LogicalType::BIGINT),
      ShortestPathTwoPhaseFunction,
      IterativeLengthFunctionData::IterativeLengthBind);
  return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb
