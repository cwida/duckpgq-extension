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

static bool IterativeLength(int64_t v_size, int64_t *V, vector<int64_t> &E,
                            vector<int64_t> &edge_ids,
                            // vector<std::vector<int64_t>> &parents_v,
                            // vector<std::vector<int64_t>> &parents_e,
                            vector<vector<unordered_set<int64_t>>> &parents_v,
                            vector<vector<vector<int64_t>>> &paths_v,
                            vector<vector<vector<int64_t>>> &paths_e,
                            vector<std::bitset<LANE_LIMIT>> &seen,
                            vector<std::bitset<LANE_LIMIT>> &visit,
                            vector<std::bitset<LANE_LIMIT>> &next) {
  bool change = false;
  map<pair<int64_t, int64_t>, unordered_set<int64_t>> parents_v_cache;
  map<pair<int64_t, int64_t>, vector<int64_t>> paths_v_cache;
  map<pair<int64_t, int64_t>, vector<int64_t>> paths_e_cache;
  for (auto v = 0; v < v_size; v++) {
    next[v] = 0;
  }
  //! Keep track of edge id through which the node was reached
  for (auto v = 0; v < v_size; v++) {
    if (visit[v].any()) {
      for (auto e = V[v]; e < V[v + 1]; e++) {
        auto n = E[e];
        auto edge_id = edge_ids[e];

        for (auto lane = 0; lane < LANE_LIMIT; lane++) {
          if (visit[v][lane]) {
            //! If the node has not been visited, then update the parent and edge
            if (seen[n][lane] == false || parents_v[v][lane].find(n) == parents_v[v][lane].end()) {
              if (visit[n][lane]) {
                parents_v_cache[make_pair(n, lane)] = parents_v[v][lane];
                parents_v_cache[make_pair(n, lane)].insert(v);
                paths_v_cache[make_pair(n, lane)] = paths_v[v][lane];
                paths_v_cache[make_pair(n, lane)].push_back(v);
                paths_e_cache[make_pair(n, lane)] = paths_e[v][lane];
                paths_e_cache[make_pair(n, lane)].push_back(edge_id);
              } else {
                parents_v[n][lane] = parents_v[v][lane];
                parents_v[n][lane].insert(v);
                paths_v[n][lane] = paths_v[v][lane];
                paths_v[n][lane].push_back(v);
                paths_e[n][lane] = paths_e[v][lane];
                paths_e[n][lane].push_back(edge_id);
              }
              next[n][lane] = true;
            } 
          }
        }

        // next[n] = next[n] | visit[v];
        // for (auto l = 0; l < LANE_LIMIT; l++) {
        //   parents_v[n][l] =
        //       ((parents_v[n][l] == -1) && visit[v][l]) ? v : parents_v[n][l];
        //   parents_e[n][l] = ((parents_e[n][l] == -1) && visit[v][l])
        //                         ? edge_id
        //                         : parents_e[n][l];
        // }
      }
    }
  }

  for (auto const& [node, parents]: parents_v_cache) {
    parents_v[node.first][node.second] = parents;
  }
  for (auto const& [node, path]: paths_v_cache) {
    paths_v[node.first][node.second] = path;
  }
  for (auto const& [node, edge]: paths_e_cache) {
    paths_e[node.first][node.second] = edge;
  }

  for (auto v = 0; v < v_size; v++) {
    // next[v] = next[v] & ~seen[v];
    seen[v] = seen[v] | next[v];
    change |= next[v].any();
  }
  return change;
}

static void ShortestPathFunction(DataChunk &args, ExpressionState &state,
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
  auto &lower_bound = args.data[4];
  auto &upper_bound = args.data[5];
  UnifiedVectorFormat vdata_lower_bound;
  UnifiedVectorFormat vdata_upper_bound;
  lower_bound.ToUnifiedFormat(args.size(), vdata_lower_bound);
  upper_bound.ToUnifiedFormat(args.size(), vdata_upper_bound);
  auto lower_bound_data = (int64_t *)vdata_lower_bound.data;
  auto upper_bound_data = (int64_t *)vdata_upper_bound.data;

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  ValidityMask &result_validity = FlatVector::Validity(result);

  // create temp SIMD arrays
  vector<std::bitset<LANE_LIMIT>> seen(v_size);
  vector<std::bitset<LANE_LIMIT>> visit1(v_size);
  vector<std::bitset<LANE_LIMIT>> visit2(v_size);
  // vector<std::vector<int64_t>> parents_v(v_size,
  //                                        std::vector<int64_t>(LANE_LIMIT, -1));
  // vector<std::vector<int64_t>> parents_e(v_size,
  //                                        std::vector<int64_t>(LANE_LIMIT, -1));
  // vector<std::vector<int64_t>> parents_v_result(v_size,
  //                                         std::vector<int64_t>(LANE_LIMIT, -1));
  // vector<std::vector<int64_t>> parents_e_result(v_size,
  //                                         std::vector<int64_t>(LANE_LIMIT, -1));

  vector<vector<unordered_set<int64_t>>> parents_v(v_size, std::vector<unordered_set<int64_t>>(LANE_LIMIT));
  vector<vector<vector<int64_t>>> paths_v(v_size, std::vector<vector<int64_t>>(LANE_LIMIT));
  vector<vector<vector<int64_t>>> paths_e(v_size, std::vector<vector<int64_t>>(LANE_LIMIT)); 
  

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
      seen[i] = 0;
      visit1[i] = 0;
      // for (auto j = 0; j < LANE_LIMIT; j++) {
      //   parents_v[i][j] = -1;
      //   parents_e[i][j] = -1;
      //   parents_v_result[i][j] = -1;
      //   parents_e_result[i][j] = -1;
      // }
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
        } else if (src_data[src_pos] == dst_data[src_pos]) {
          unique_ptr<Vector> output =
            make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
          ListVector::PushBack(*output, src_data[src_pos]);
          ListVector::Append(result, ListVector::GetEntry(*output),
                            ListVector::GetListSize(*output));
          result_data[search_num].length = ListVector::GetListSize(*output);
          result_data[search_num].offset = total_len;
          total_len += result_data[search_num].length;
        } else {
          visit1[src_data[src_pos]][lane] = true;
          seen[src_data[src_pos]][lane] = true;
          // parents_v[src_data[src_pos]][lane] =
          //     -2; // No incoming vertex for the source
          // parents_e[src_data[src_pos]][lane] =
          //     -2; // Mark the source with -2, there is no incoming edge for the
          //         // source.
          lane_to_num[lane] = search_num; // active lane
          active++;
          break;
        }
      }
    }

    bool stop[LANE_LIMIT] = {false};
    //! make passes while a lane is still active
    for (int64_t iter = 1; active; iter++) {
      //! Perform one step of bfs exploration
      if (!IterativeLength(v_size, v, e, edge_ids, parents_v, paths_v, paths_e, seen,
                           (iter & 1) ? visit1 : visit2,
                           (iter & 1) ? visit2 : visit1)) {
        break;
      }
      int64_t finished_searches = 0;
      // detect lanes that finished
      for (int64_t lane = 0; lane < LANE_LIMIT && (stop[lane] == false); lane++) {
        int64_t search_num = lane_to_num[lane];
        if (search_num >= 0) { // active lane
          //! Check if dst for a source has been seen
          int64_t dst_pos = vdata_dst.sel->get_index(search_num);
          if (seen[dst_data[dst_pos]][lane]) {
            // check if the path length is within bounds
            // bound vector is either a constant or a flat vector
            if (lower_bound.GetVectorType() == VectorType::CONSTANT_VECTOR ? 
                iter < lower_bound_data[0] : iter < lower_bound_data[dst_pos]) {
              // when reach the destination too early, treat destination as null
              // looks like the graph does not have that vertex
              seen[dst_data[dst_pos]][lane] = false;
              (iter & 1) ? visit2[dst_data[dst_pos]][lane] = false
                         : visit1[dst_data[dst_pos]][lane] = false;
              continue;
            } else if (upper_bound.GetVectorType() == VectorType::CONSTANT_VECTOR ? 
                iter > upper_bound_data[0] : iter > upper_bound_data[dst_pos]) {
              result_validity.SetInvalid(search_num);
            } else {
              vector<int64_t> output_vector;
              auto it_v = paths_v[dst_data[dst_pos]][lane].begin(),
                   end_v = paths_v[dst_data[dst_pos]][lane].end();
              auto it_e = paths_e[dst_data[dst_pos]][lane].begin(),
                  end_e = paths_e[dst_data[dst_pos]][lane].end();
              while (it_v != end_v && it_e != end_e) {
                output_vector.push_back(*it_v);
                output_vector.push_back(*it_e);
                it_v++;
                it_e++;
              }
              output_vector.push_back(dst_data[dst_pos]);
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
            }
            stop[lane] = true;
            finished_searches++;
          }
        }
      }
      if (finished_searches == LANE_LIMIT) {
        break;
      }
    }
    // //! Reconstruct the paths
    // for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    //   int64_t search_num = lane_to_num[lane];
    //   if (search_num == -1) { // empty lanes
    //     continue;
    //   }

    //   //! Searches that have stopped have found a path
    //   int64_t src_pos = vdata_src.sel->get_index(search_num);
    //   int64_t dst_pos = vdata_dst.sel->get_index(search_num);

    //   parents_v_result[src_data[src_pos]][lane] = src_data[src_pos];

    //   if (src_data[src_pos] == dst_data[dst_pos]) { // Source == destination
    //     unique_ptr<Vector> output =
    //         make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
    //     ListVector::PushBack(*output, src_data[src_pos]);
    //     ListVector::Append(result, ListVector::GetEntry(*output),
    //                        ListVector::GetListSize(*output));
    //     result_data[search_num].length = ListVector::GetListSize(*output);
    //     result_data[search_num].offset = total_len;
    //     total_len += result_data[search_num].length;
    //     continue;
    //   }
    //   std::vector<int64_t> output_vector;
    //   std::vector<int64_t> output_edge;
    //   auto source_v = src_data[src_pos]; // Take the source

    //   auto parent_vertex =
    //       parents_v_result[dst_data[dst_pos]]
    //                [lane]; // Take the parent vertex of the destination vertex
    //   auto parent_edge =
    //       parents_e_result[dst_data[dst_pos]]
    //                [lane]; // Take the parent edge of the destination vertex

    //   output_vector.push_back(dst_data[dst_pos]); // Add destination vertex
    //   output_vector.push_back(parent_edge);
    //   while (parent_vertex != source_v) { // Continue adding vertices until we
    //                                       // have reached the source vertex
    //     //! -1 is used to signify no parent
    //     if (parent_vertex == -1 ||
    //         parent_vertex == parents_v_result[parent_vertex][lane]) {
    //       result_validity.SetInvalid(search_num);
    //       break;
    //     }
    //     output_vector.push_back(parent_vertex);
    //     parent_edge = parents_e_result[parent_vertex][lane];
    //     parent_vertex = parents_v_result[parent_vertex][lane];
    //     output_vector.push_back(parent_edge);
    //   }

    //   if (!result_validity.RowIsValid(search_num)) {
    //     continue;
    //   }
    //   output_vector.push_back(source_v);
    //   std::reverse(output_vector.begin(), output_vector.end());
    //   auto output = make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
    //   for (auto val : output_vector) {
    //     Value value_to_insert = val;
    //     ListVector::PushBack(*output, value_to_insert);
    //   }

    //   result_data[search_num].length = ListVector::GetListSize(*output);
    //   result_data[search_num].offset = total_len;
    //   ListVector::Append(result, ListVector::GetEntry(*output),
    //                      ListVector::GetListSize(*output));
    //   total_len += result_data[search_num].length;
    // }
  }
  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

CreateScalarFunctionInfo DuckPGQFunctions::GetShortestPathFunction() {
  auto fun = ScalarFunction("shortestpath",
                            {LogicalType::INTEGER, LogicalType::BIGINT,
                             LogicalType::BIGINT, LogicalType::BIGINT,
                             LogicalType::BIGINT, LogicalType::BIGINT},
                            LogicalType::LIST(LogicalType::BIGINT),
                            ShortestPathFunction,
                            IterativeLengthFunctionData::IterativeLengthBind);
  return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb
