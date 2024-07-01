#include <duckpgq_extension.hpp>
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/duckpgq_functions.hpp"
#include "duckpgq/functions/function_data/iterative_length_function_data.hpp"


namespace duckdb {
typedef enum { NO_ARRAY, ARRAY, INTERMEDIATE } msbfs_modes_t;

static int16_t InitialiseBfs(
    idx_t curr_batch, idx_t size, int64_t *src_data,
    const SelectionVector *src_sel, const ValidityMask &src_validity,
    vector<std::bitset<LANE_LIMIT>> &seen,
    vector<std::bitset<LANE_LIMIT>> &visit,
    vector<std::bitset<LANE_LIMIT>> &visit_next,
    unordered_map<int64_t, pair<int16_t, vector<int64_t>>> &lane_map) {
  int16_t lanes = 0;
  int16_t curr_batch_size = 0;

  for (idx_t i = curr_batch; i < size && lanes < LANE_LIMIT; i++) {
    auto src_index = src_sel->get_index(i);

    if (src_validity.RowIsValid(src_index)) {
      int64_t &src_entry = src_data[src_index];
      auto entry = lane_map.find(src_entry);
      if (entry == lane_map.end()) {
        lane_map[src_entry].first = lanes;
        seen[src_entry][lanes] = true;
        visit[src_entry][lanes] = true;
        lanes++;
      }
      lane_map[src_entry].second.push_back(i);
      curr_batch_size++;
    }
  }
  return curr_batch_size;
}

static bool BfsWithoutArrayVariant(bool exit_early, CSR *csr,
                                   int64_t input_size,
                                   vector<std::bitset<LANE_LIMIT>> &seen,
                                   vector<std::bitset<LANE_LIMIT>> &visit,
                                   vector<std::bitset<LANE_LIMIT>> &visit_next,
                                   vector<int64_t> &visit_list) {
  for (int64_t i = 0; i < input_size; i++) {
    if (!visit[i].any()) {
      continue;
    }

    for (auto index = (int64_t)csr->v[i]; index < csr->v[i + 1]; index++) {
      auto n = csr->e[index];
      visit_next[n] = visit_next[n] | visit[i];
    }
  }

  for (int64_t i = 0; i < input_size; i++) {
    if (visit_next[i].none()) {
      continue;
    }
    visit_next[i] = visit_next[i] & ~seen[i];
    seen[i] = seen[i] | visit_next[i];
    if (exit_early && visit_next[i].any()) {
      exit_early = false;
    }
    if (visit_next[i].any()) {
      visit_list.push_back(i);
    }
  }
  return exit_early;
}

static bool BfsWithoutArray(bool exit_early, CSR *csr, int64_t input_size,
                            vector<std::bitset<LANE_LIMIT>> &seen,
                            vector<std::bitset<LANE_LIMIT>> &visit,
                            vector<std::bitset<LANE_LIMIT>> &visit_next) {
  for (int64_t i = 0; i < input_size; i++) {
    if (!visit[i].any()) {
      continue;
    }

    for (auto index = (int64_t)csr->v[i]; index < (int64_t)csr->v[i + 1];
         index++) {
      auto n = csr->e[index];
      visit_next[n] = visit_next[n] | visit[i];
    }
  }

  for (int64_t i = 0; i < input_size; i++) {
    if (visit_next[i].none()) {
      continue;
    }
    visit_next[i] = visit_next[i] & ~seen[i];
    seen[i] = seen[i] | visit_next[i];
    if (exit_early && visit_next[i].any()) {
      exit_early = false;
    }
  }
  return exit_early;
}

static pair<bool, size_t>
BfsTempStateVariant(bool exit_early, CSR *csr, int64_t input_size,
                    vector<std::bitset<LANE_LIMIT>> &seen,
                    vector<std::bitset<LANE_LIMIT>> &visit,
                    vector<std::bitset<LANE_LIMIT>> &visit_next) {
  size_t num_nodes_to_visit = 0;
  for (int64_t i = 0; i < input_size; i++) {
    if (!visit[i].any()) {
      continue;
    }

    for (auto index = (int64_t)csr->v[i]; index < (int64_t)csr->v[i + 1];
         index++) {
      auto n = csr->e[index];
      visit_next[n] = visit_next[n] | visit[i];
    }
  }

  for (int64_t i = 0; i < input_size; i++) {
    if (visit_next[i].none()) {
      continue;
    }
    visit_next[i] = visit_next[i] & ~seen[i];
    seen[i] = seen[i] | visit_next[i];
    if (exit_early && visit_next[i].any()) {
      exit_early = false;
    }
    if (visit_next[i].any()) {
      num_nodes_to_visit++;
    }
  }
  return pair<bool, size_t>(exit_early, num_nodes_to_visit);
}

static bool BfsWithArrayVariant(bool exit_early, CSR *csr,
                                vector<std::bitset<LANE_LIMIT>> &seen,
                                vector<std::bitset<LANE_LIMIT>> &visit,
                                vector<std::bitset<LANE_LIMIT>> &visit_next,
                                vector<int64_t> &visit_list) {
  unordered_set<int64_t> neighbours_set;
  for (int64_t i : visit_list) {
    for (auto index = (int64_t)csr->v[i]; index < (int64_t)csr->v[i + 1];
         index++) {
      auto n = csr->e[index];
      visit_next[n] = visit_next[n] | visit[i];
      neighbours_set.insert(n);
    }
  }
  visit_list.clear();
  for (int64_t i : neighbours_set) {
    visit_next[i] = visit_next[i] & ~seen[i];
    seen[i] = seen[i] | visit_next[i];
    if (exit_early && visit_next[i].any()) {
      exit_early = false;
    }
    if (visit_next[i].any()) {
      visit_list.push_back(i);
    }
  }
  return exit_early;
}

static int FindMode(int mode, size_t visit_list_len, size_t visit_limit,
                    size_t num_nodes_to_visit) {
  if (mode == 0 && visit_list_len > 0) {
    mode = 1;
  } else if (mode == 1 && visit_list_len > visit_limit) {
    mode = 2;
  } else if (mode == 2 && num_nodes_to_visit < visit_limit) {
    mode = 0;
  }
  return mode;
}

static void ReachabilityFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (IterativeLengthFunctionData &)*func_expr.bind_info;

  bool is_variant = args.data[1].GetValue(0).GetValue<bool>();
  int64_t input_size = args.data[2].GetValue(0).GetValue<int64_t>();

  auto &src = args.data[3];

  UnifiedVectorFormat vdata_src, vdata_target;
  src.ToUnifiedFormat(args.size(), vdata_src);

  auto src_data = (int64_t *)vdata_src.data;

  auto &target = args.data[4];
  target.ToUnifiedFormat(args.size(), vdata_target);
  auto target_data = (int64_t *)vdata_target.data;

  idx_t result_size = 0;
  vector<int64_t> visit_list;
  size_t visit_limit = input_size / VISIT_SIZE_DIVISOR;
  size_t num_nodes_to_visit = 0;
  result.SetVectorType(VectorType::FLAT_VECTOR);

  auto result_data = FlatVector::GetData<bool>(result);
  auto duckpgq_state_entry = info.context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == info.context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());

  CSR *csr = duckpgq_state->GetCSR(info.csr_id);

  while (result_size < args.size()) {
    vector<std::bitset<LANE_LIMIT>> seen(input_size);
    vector<std::bitset<LANE_LIMIT>> visit(input_size);
    vector<std::bitset<LANE_LIMIT>> visit_next(input_size);

    //! mapping of src_value ->  (bfs_num/lane, vector of indices in src_data)
    unordered_map<int64_t, pair<int16_t, vector<int64_t>>> lane_map;
    auto curr_batch_size =
        InitialiseBfs(result_size, args.size(), src_data, vdata_src.sel,
                      vdata_src.validity, seen, visit, visit_next, lane_map);
    int mode = 0;
    bool exit_early = false;
    while (!exit_early) {
      exit_early = true;
      if (is_variant) {
        mode =
            FindMode(mode, visit_list.size(), visit_limit, num_nodes_to_visit);
        switch (mode) {
        case 1:
          exit_early = BfsWithArrayVariant(exit_early, csr, seen, visit,
                                           visit_next, visit_list);
          break;
        case 0:
          exit_early = BfsWithoutArrayVariant(exit_early, csr, input_size, seen,
                                              visit, visit_next, visit_list);
          break;
        case 2: {
          auto return_pair = BfsTempStateVariant(exit_early, csr, input_size,
                                                 seen, visit, visit_next);
          exit_early = return_pair.first;
          num_nodes_to_visit = return_pair.second;
          break;
        }
        default:
          throw Exception(ExceptionType::INTERNAL,
                          "Unknown reachability mode encountered");
        }
      } else {
        exit_early = BfsWithoutArray(exit_early, csr, input_size, seen, visit,
                                     visit_next);
      }

      visit = visit_next;
      for (auto i = 0; i < input_size; i++) {
        visit_next[i] = 0;
      }
    }

    for (const auto &iter : lane_map) {
      auto value = iter.first;
      auto bfs_num = iter.second.first;
      auto pos = iter.second.second;
      for (auto index : pos) {
        auto target_index = vdata_target.sel->get_index(index);
        if (seen[target_data[target_index]][bfs_num] && seen[value][bfs_num]) {
          // if(is_bit_set(seen[target_data[index]], bfs_num) &
          // is_bit_set(seen[value], bfs_num) ) {
          result_data[index] = true;
        } else {
          result_data[index] = false;
        }
      }
    }
    result_size = result_size + curr_batch_size;
  }
  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

CreateScalarFunctionInfo DuckPGQFunctions::GetReachabilityFunction() {
  auto fun = ScalarFunction("reachability",
                            {LogicalType::INTEGER, LogicalType::BOOLEAN,
                             LogicalType::BIGINT, LogicalType::BIGINT,
                             LogicalType::BIGINT},
                            LogicalType::BOOLEAN, ReachabilityFunction,
                            IterativeLengthFunctionData::IterativeLengthBind);
  return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb
