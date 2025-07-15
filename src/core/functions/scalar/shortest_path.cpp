#include "duckdb/common/fstream.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/functions/function_data/iterative_length_function_data.hpp"

#include <duckpgq/core/functions/scalar.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {

static bool IterativeLength(int64_t v_size, int64_t *V, vector<int64_t> &E, vector<int64_t> &edge_ids,
                            vector<std::vector<int64_t>> &parents_v, vector<std::vector<int64_t>> &parents_e,
                            vector<std::bitset<LANE_LIMIT>> &seen, vector<std::bitset<LANE_LIMIT>> &visit,
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
					parents_v[n][l] = ((parents_v[n][l] == -1) && visit[v][l]) ? v : parents_v[n][l];
					parents_e[n][l] = ((parents_e[n][l] == -1) && visit[v][l]) ? edge_id : parents_e[n][l];
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

static void ShortestPathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (IterativeLengthFunctionData &)*func_expr.bind_info;
	auto duckpgq_state = GetDuckPGQState(info.context);

	D_ASSERT(duckpgq_state->csr_list[info.csr_id]);
	auto csr_entry = duckpgq_state->csr_list.find(info.csr_id);
	if (csr_entry == duckpgq_state->csr_list.end()) {
		throw ConstraintException("Invalid ID");
	}
	auto &csr = csr_entry->second;

	if (!csr->initialized_v) {
		throw ConstraintException("Need to initialize CSR before doing shortest path");
	}
	int64_t v_size = args.data[1].GetValue(0).GetValue<int64_t>();

	auto *v = (int64_t *)csr->v;
	vector<int64_t> &e = csr->e;
	vector<int64_t> &edge_ids = csr->edge_ids;

	auto &src = args.data[2];
	auto &target = args.data[3];

	UnifiedVectorFormat vdata_src, vdata_dst;
	src.ToUnifiedFormat(args.size(), vdata_src);
	target.ToUnifiedFormat(args.size(), vdata_dst);

	auto src_data = (int64_t *)vdata_src.data;
	auto dst_data = (int64_t *)vdata_dst.data;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	ValidityMask &result_validity = FlatVector::Validity(result);

	// create temp SIMD arrays
	vector<std::bitset<LANE_LIMIT>> seen(v_size);
	vector<std::bitset<LANE_LIMIT>> visit1(v_size);
	vector<std::bitset<LANE_LIMIT>> visit2(v_size);
	vector<std::vector<int64_t>> parents_v(v_size, std::vector<int64_t>(LANE_LIMIT, -1));
	vector<std::vector<int64_t>> parents_e(v_size, std::vector<int64_t>(LANE_LIMIT, -1));

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
			for (auto j = 0; j < LANE_LIMIT; j++) {
				parents_v[i][j] = -1;
				parents_e[i][j] = -1;
			}
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
				} else {
					visit1[src_data[src_pos]][lane] = true;
					parents_v[src_data[src_pos]][lane] = src_data[src_pos]; // Mark source with source id
					parents_e[src_data[src_pos]][lane] = -2; // Mark the source with -2, there is no incoming edge for
					                                         // the source.
					lane_to_num[lane] = search_num; // active lane
					active++;
					break;
				}
			}
		}

		//! make passes while a lane is still active
		for (int64_t iter = 1; active; iter++) {
			//! Perform one step of bfs exploration
			if (!IterativeLength(v_size, v, e, edge_ids, parents_v, parents_e, seen, (iter & 1) ? visit1 : visit2,
			                     (iter & 1) ? visit2 : visit1)) {
				break;
			}
			int64_t finished_searches = 0;
			// detect lanes that finished
			for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
				int64_t search_num = lane_to_num[lane];
				if (search_num >= 0) { // active lane
					//! Check if dst for a source has been seen
					int64_t dst_pos = vdata_dst.sel->get_index(search_num);
					if (seen[dst_data[dst_pos]][lane]) {
						finished_searches++;
					}
				}
			}
			if (finished_searches == LANE_LIMIT) {
				break;
			}
		}
		//! Reconstruct the paths
		for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
			int64_t search_num = lane_to_num[lane];
			if (search_num == -1) { // empty lanes
				continue;
			}

			//! Searches that have stopped have found a path
			int64_t src_pos = vdata_src.sel->get_index(search_num);
			int64_t dst_pos = vdata_dst.sel->get_index(search_num);
			if (src_data[src_pos] == dst_data[dst_pos]) { // Source == destination
				unique_ptr<Vector> output = make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
				ListVector::PushBack(*output, src_data[src_pos]);
				ListVector::Append(result, ListVector::GetEntry(*output), ListVector::GetListSize(*output));
				result_data[search_num].length = ListVector::GetListSize(*output);
				result_data[search_num].offset = total_len;
				total_len += result_data[search_num].length;
				continue;
			}
			std::vector<int64_t> output_vector;
			std::vector<int64_t> output_edge;
			auto source_v = src_data[src_pos]; // Take the source

			auto parent_vertex = parents_v[dst_data[dst_pos]][lane]; // Take the parent vertex of the destination vertex
			auto parent_edge = parents_e[dst_data[dst_pos]][lane];   // Take the parent edge of the destination vertex

			output_vector.push_back(dst_data[dst_pos]); // Add destination vertex
			output_vector.push_back(parent_edge);
			while (parent_vertex != source_v) { // Continue adding vertices until we
				                                // have reached the source vertex
				//! -1 is used to signify no parent
				if (parent_vertex == -1 || parent_vertex == parents_v[parent_vertex][lane]) {
					result_validity.SetInvalid(search_num);
					break;
				}
				output_vector.push_back(parent_vertex);
				parent_edge = parents_e[parent_vertex][lane];
				parent_vertex = parents_v[parent_vertex][lane];
				output_vector.push_back(parent_edge);
			}

			if (!result_validity.RowIsValid(search_num)) {
				continue;
			}
			output_vector.push_back(source_v);
			std::reverse(output_vector.begin(), output_vector.end());
			auto output = make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
			for (auto val : output_vector) {
				Value value_to_insert = val;
				ListVector::PushBack(*output, value_to_insert);
			}

			result_data[search_num].length = ListVector::GetListSize(*output);
			result_data[search_num].offset = total_len;
			ListVector::Append(result, ListVector::GetEntry(*output), ListVector::GetListSize(*output));
			total_len += result_data[search_num].length;
		}
	}
	duckpgq_state->csr_to_delete.insert(info.csr_id);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterShortestPathScalarFunction(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(
	    db, ScalarFunction("shortestpath",
	                       {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                       LogicalType::LIST(LogicalType::BIGINT), ShortestPathFunction,
	                       IterativeLengthFunctionData::IterativeLengthBind));
}

} // namespace core

} // namespace duckpgq
