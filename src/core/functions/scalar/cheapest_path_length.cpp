#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/duckpgq_functions.hpp"
#include "duckpgq/functions/function_data/cheapest_path_length_function_data.hpp"
#include <duckpgq_extension.hpp>

#include <duckpgq/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {

template <typename T, int16_t lane_limit>
static int16_t InitialiseBellmanFord(const DataChunk &args, int64_t input_size,
                                     const UnifiedVectorFormat &vdata_src,
                                     const int64_t *src_data, idx_t result_size,
                                     vector<vector<T>> &dists) {
  dists.resize(input_size,
               std::vector<T>(lane_limit, std::numeric_limits<T>::max() / 2));

  int16_t lanes = 0;
  for (idx_t i = result_size; i < args.size() && lanes < lane_limit; i++) {
    auto src_index = vdata_src.sel->get_index(i);
    if (vdata_src.validity.RowIsValid(src_index)) {
      const int64_t &src_entry = src_data[src_index];
      dists[src_entry][lanes] = 0;
      lanes++;
    }
  }
  return lanes;
}

template <typename T> int64_t UpdateOneLane(T &n_dist, T v_dist, T weight) {
  T new_dist = v_dist + weight;
  bool better = new_dist < n_dist;
  T min = better ? new_dist : n_dist;
  n_dist = min;
  return better;
}

template <typename T>
bool UpdateLanes(vector<vector<T>> &dists, T v, T n, T weight) {
  std::vector<T> &v_dists = dists[v];
  std::vector<T> &n_dists = dists[n];
  size_t num_lanes = dists[v].size();
  size_t lane_idx = 0;
  bool xor_diff = false;
  while (lane_idx < num_lanes) {
    xor_diff |= UpdateOneLane<T>(n_dists[lane_idx], v_dists[lane_idx], weight);
    ++lane_idx;
  }
  return xor_diff;
}

template <typename T, int16_t lane_limit>
int16_t
TemplatedBatchBellmanFord(CSR *csr, DataChunk &args, int64_t input_size,
                          UnifiedVectorFormat &vdata_src, int64_t *src_data,
                          const UnifiedVectorFormat &vdata_target,
                          int64_t *target_data, std::vector<T> weight_array,
                          int16_t result_size, T *result_data,
                          ValidityMask &result_validity) {
  vector<vector<T>> dists;
  int16_t curr_batch_size = InitialiseBellmanFord<T, lane_limit>(
      args, input_size, vdata_src, src_data, result_size, dists);
  bool changed = true;
  while (changed) {
    changed = false;
    //! For every v in the input
    for (int64_t v = 0; v < input_size; v++) {
      //! Loop through all the n neighbours of v
      for (auto index = (int64_t)csr->v[v]; index < (int64_t)csr->v[v + 1];
           index++) {
        //! Get weight of (v,n)
        changed = UpdateLanes<T>(dists, v, csr->e[index], weight_array[index]) |
                  changed;
      }
    }
  }
  for (idx_t i = result_size; i < (idx_t)(result_size + curr_batch_size); i++) {
    auto target_index = vdata_target.sel->get_index(i);
    if (!vdata_target.validity.RowIsValid(target_index)) {
      result_validity.SetInvalid(i);
    }

    const auto &target_entry = target_data[target_index];
    auto resulting_distance = dists[target_entry][i % lane_limit];

    if (resulting_distance == std::numeric_limits<T>::max() / 2) {
      result_validity.SetInvalid(i);
    } else {
      result_data[i] = resulting_distance;
    }
  }
  dists.clear();
  return curr_batch_size;
}

template <typename T>
void TemplatedBellmanFord(CSR *csr, DataChunk &args, int64_t input_size,
                          Vector &result, UnifiedVectorFormat &vdata_src,
                          int64_t *src_data,
                          const UnifiedVectorFormat &vdata_target,
                          int64_t *target_data, std::vector<T> weight_array) {
  idx_t result_size = 0;
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<T>(result);
  auto &result_validity = FlatVector::Validity(result);
  vector<vector<T>> final_dists(
      input_size,
      std::vector<T>(args.size(), std::numeric_limits<T>::max() / 2));

  while (result_size < args.size()) {
    if ((args.size() - result_size) / 256 >= 1) {
      result_size += TemplatedBatchBellmanFord<T, 256>(
          csr, args, input_size, vdata_src, src_data, vdata_target, target_data,
          weight_array, result_size, result_data, result_validity);
    } else if ((args.size() - result_size) / 128 >= 1) {
      result_size += TemplatedBatchBellmanFord<T, 128>(
          csr, args, input_size, vdata_src, src_data, vdata_target, target_data,
          weight_array, result_size, result_data, result_validity);
    } else if ((args.size() - result_size) / 64 >= 1) {
      result_size += TemplatedBatchBellmanFord<T, 64>(
          csr, args, input_size, vdata_src, src_data, vdata_target, target_data,
          weight_array, result_size, result_data, result_validity);
    } else if ((args.size() - result_size) / 16 >= 1) {
      result_size += TemplatedBatchBellmanFord<T, 16>(
          csr, args, input_size, vdata_src, src_data, vdata_target, target_data,
          weight_array, result_size, result_data, result_validity);
    } else if ((args.size() - result_size) / 8 >= 1) {
      result_size += TemplatedBatchBellmanFord<T, 8>(
          csr, args, input_size, vdata_src, src_data, vdata_target, target_data,
          weight_array, result_size, result_data, result_validity);
    } else if ((args.size() - result_size) / 4 >= 1) {
      result_size += TemplatedBatchBellmanFord<T, 4>(
          csr, args, input_size, vdata_src, src_data, vdata_target, target_data,
          weight_array, result_size, result_data, result_validity);
    } else if ((args.size() - result_size) / 2 >= 1) {
      result_size += TemplatedBatchBellmanFord<T, 2>(
          csr, args, input_size, vdata_src, src_data, vdata_target, target_data,
          weight_array, result_size, result_data, result_validity);
    } else {
      result_size += TemplatedBatchBellmanFord<T, 1>(
          csr, args, input_size, vdata_src, src_data, vdata_target, target_data,
          weight_array, result_size, result_data, result_validity);
    }
  }
}

static void CheapestPathLengthFunction(DataChunk &args, ExpressionState &state,
                                       Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (CheapestPathLengthFunctionData &)*func_expr.bind_info;
  int64_t input_size = args.data[1].GetValue(0).GetValue<int64_t>();
  auto duckpgq_state = GetDuckPGQState(info.context);

  CSR *csr = duckpgq_state->GetCSR(info.csr_id);
  auto &src = args.data[2];

  UnifiedVectorFormat vdata_src, vdata_target;
  src.ToUnifiedFormat(args.size(), vdata_src);

  auto src_data = (int64_t *)vdata_src.data;

  auto &target = args.data[3];
  target.ToUnifiedFormat(args.size(), vdata_target);
  auto target_data = (int64_t *)vdata_target.data;
  if (csr->w.empty()) {
    TemplatedBellmanFord<double>(csr, args, input_size, result, vdata_src,
                                 src_data, vdata_target, target_data,
                                 csr->w_double);
  } else {
    TemplatedBellmanFord<int64_t>(csr, args, input_size, result, vdata_src,
                                  src_data, vdata_target, target_data, csr->w);
  }
  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

CreateScalarFunctionInfo DuckPGQFunctions::GetCheapestPathLengthFunction() {
  ScalarFunctionSet set("cheapest_path_length");

  set.AddFunction(
      ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT,
                      LogicalType::BIGINT, LogicalType::BIGINT},
                     LogicalType::ANY, CheapestPathLengthFunction,
                     CheapestPathLengthFunctionData::CheapestPathLengthBind));

  return CreateScalarFunctionInfo(set);
}

} // namespace core

} // namespace duckpgq
