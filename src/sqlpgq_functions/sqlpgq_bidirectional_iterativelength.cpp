// #include "duckdb/execution/expression_executor.hpp"
// #include "duckdb/main/client_context.hpp"
// #include "duckdb/main/client_data.hpp"
// #include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
// #include "duckdb/planner/expression/bound_function_expression.hpp"
// #include "sqlpgq_common.hpp"
// #include "sqlpgq_functions.hpp"
//
// namespace duckdb {
//
// static bool BidirectionalIterativeLength(int64_t v_size, int64_t *v, vector<int64_t> &e,
// vector<std::bitset<LANE_LIMIT>> &seen,
//                             vector<std::bitset<LANE_LIMIT>> &visit, vector<std::bitset<LANE_LIMIT>> &next) {
//	bool change = false;
//
//     for (auto i = 0; i < v_size; i++) {
//		next[i] = 0;
//	}
//	for (auto i = 0; i < v_size; i++) {
//		if (visit[i].any()) {
//			for (auto offset = v[i]; offset < v[i + 1]; offset++) {
//				auto n = e[offset];
//				next[n] = next[n] | visit[i];
//			}
//		}
//	}
//	for (auto i = 0; i < v_size; i++) {
//		next[i] = next[i] & ~seen[i];
//         seen[i] = seen[i] | visit[i];
//		change |= next[i].any();
//	}
//	return change;
// }
//
// static void BidirectionalIterativeLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
//	auto &func_expr = (BoundFunctionExpression &)state.expr;
//	auto &info = (IterativeLengthFunctionData &)*func_expr.bind_info;
//
//	// get csr info (TODO: do not store in context -- make global map in module that is indexed by id+&context)
//	int32_t id = args.data[0].GetValue(0).GetValue<int32_t>();
//	D_ASSERT(info.context.client_data->csr_list[id]);
//	int64_t v_size = args.data[1].GetValue(0).GetValue<int64_t>();
//	int64_t *v = (int64_t *)info.context.client_data->csr_list[id]->v;
//	vector<int64_t> &e = info.context.client_data->csr_list[id]->e;
//
//	// get src and dst vectors for searches
//	auto &src = args.data[2];
//	auto &dst = args.data[3];
//	UnifiedVectorFormat vdata_src;
//	UnifiedVectorFormat vdata_dst;
//	src.ToUnifiedFormat(args.size(), vdata_src);
//	dst.ToUnifiedFormat(args.size(), vdata_dst);
//	auto src_data = (int64_t *)vdata_src.data;
//	auto dst_data = (int64_t *)vdata_dst.data;
//
//	// create result vector
//	result.SetVectorType(VectorType::FLAT_VECTOR);
//	auto result_data = FlatVector::GetData<int64_t>(result);
//     ValidityMask &result_validity = FlatVector::Validity(result);
//
//	// create temp SIMD arrays
//	vector<std::bitset<LANE_LIMIT>> seen(v_size);
//	vector<std::bitset<LANE_LIMIT>> visit1(v_size);
//	vector<std::bitset<LANE_LIMIT>> visit2(v_size);
//
//	// maps lane to search number
//	int16_t lane_to_num[LANE_LIMIT];
//     int16_t lane_to_idx[LANE_LIMIT];
//
//	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
//		lane_to_num[lane] = -1; // inactive
//	}
//
//     idx_t search_idx = 0;
//     idx_t started_searches = 0;
//     while (started_searches < args.size() * 2) {
//		// empty visit vectors
//		for (auto i = 0; i < v_size; i++) {
//			seen[i] = 0;
//			visit1[i] = 0;
//		}
//
//		// add search jobs to free lanes
//		uint64_t active = 0;
//		for (int64_t lane = 0; lane < LANE_LIMIT; lane+=2) {
//			lane_to_num[lane] = -1;
//             lane_to_num[lane + 1] = -1;
//			while (started_searches < args.size() * 2) {
//                 lane_to_idx[lane] = search_idx;
//                 lane_to_idx[lane+1] = search_idx;
//                 int64_t search_num = search_idx++;
//                 int64_t src_idx = started_searches++;
//                 int64_t dst_idx = started_searches++;
//				int64_t src_pos = vdata_src.sel->get_index(search_num);
//                 int64_t dst_pos = vdata_dst.sel->get_index(search_num);
//				if (!vdata_src.validity.RowIsValid(src_pos)) {
//                     result_validity.SetInvalid(src_pos);
//					result_data[src_pos] = (uint64_t)-1; /* no path */
//				} else if (!vdata_dst.validity.RowIsValid(dst_pos)) {
//                     result_validity.SetInvalid(dst_pos);
//                     result_data[dst_pos] = (uint64_t)-1; /* no path */
//                 } else if (src_data[src_pos] == dst_data[dst_pos]) {
//                     result_data[search_num] = (uint64_t) 0; // path of length 0 does not require a search
//                 } else {
//                     try {
//                         visit1.at(src_data[src_pos])[lane] = true;
//                         seen.at(src_data[src_pos])[lane] = true;
//                         lane_to_num[lane] = src_idx % LANE_LIMIT; // active lane
//                         visit1.at(dst_data[dst_pos])[lane+1] = true;
//                         seen.at(dst_data[dst_pos])[lane+1] = true;
//                         lane_to_num[lane+1] = dst_idx % LANE_LIMIT; // active lane
//                         active++;
//                         break;
//                     }
//					catch (const std::out_of_range& oor) {
//                         throw InvalidInputException("The source or destination ID is out of range");
//                     }
//				}
//			}
//		}
//
//		// make passes while a lane is still active
//		for (int64_t iter = 1; active; iter++) {
//			if (!BidirectionalIterativeLength(v_size, v, e, seen, (iter&1)?visit1:visit2, (iter&1)?visit2:visit1)) {
//				break;
//			}
//             // detect lanes that finished
//             for (int64_t v_idx = 0; v_idx < v_size; v_idx++) {
//                 auto v1 = visit1[v_idx];
//                 auto v2 = visit2[v_idx];
//                 for (int16_t lane = 0; lane < LANE_LIMIT; lane+=2) {
//                     int64_t src_lane = lane_to_num[lane];
//                     int64_t dst_lane = lane_to_num[lane+1];
//                     int16_t row_idx = lane_to_idx[lane];
//                     if ((src_lane < 0) & (dst_lane < 0)) {
//                         // CHECK
//                         continue;
//                     }
//                     bool found = false;
//                     if ((v1[src_lane] && v2[dst_lane]) || (v1[dst_lane] && v2[src_lane])) {
//                         result_data[row_idx] = iter * 2 - 1; /* found at iter => iter = path length */
//                         lane_to_num[lane] = -1;         // mark inactive
//                         lane_to_num[lane + 1] = -1;         // mark inactive
//                         active--;
//                         found = true;
//                     }
//                     if (!found) {
//                         if (v1[src_lane] && v1[dst_lane]) {
//                             result_data[row_idx] = iter * 2; /* found at iter => iter = path length */
//                             lane_to_num[lane] = -1;         // mark inactive
//                             lane_to_num[lane + 1] = -1;         // mark inactive
//                             active--;
//                         } else if (v2[src_lane] && v2[dst_lane]) {
//                             result_data[row_idx] = iter * 2; /* found at iter => iter = path length */
//                             lane_to_num[lane] = -1;         // mark inactive
//                             lane_to_num[lane + 1] = -1;         // mark inactive
//                             active--;
//                         }
//                     }
//                 }
//             }
//         }
//         for (int64_t lane = 0; lane < LANE_LIMIT; lane+=2) {
//             int16_t row_idx = lane_to_idx[lane];
//             int64_t search_num = lane_to_num[lane];
//             if (search_num >= 0) {
//                 result_validity.SetInvalid(row_idx);// active lane
//                 result_data[row_idx] = (int64_t)-1; /* no path */
//                 lane_to_num[lane] = -1;                // mark inactive
//             }
//         }
//	}
// }
//
// CreateScalarFunctionInfo SQLPGQFunctions::GetBidirectionalIterativeLengthFunction() {
//	auto fun = ScalarFunction("bidirectionaliterativelength",
//	                          {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
//	                          LogicalType::BIGINT, BidirectionalIterativeLengthFunction,
// IterativeLengthFunctionData::IterativeLengthBind); 	return CreateScalarFunctionInfo(fun);
// }
//
// } // namespace duckdb
