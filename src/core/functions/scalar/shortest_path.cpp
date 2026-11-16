#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/functions/function_data/iterative_length_function_data.hpp"

#include <duckpgq/core/functions/scalar.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckdb {

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
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.BindInfo()->Cast<IterativeLengthFunctionData>();
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

	auto *v = reinterpret_cast<int64_t *>(csr->v);
	vector<int64_t> &e = csr->e;
	vector<int64_t> &edge_ids = csr->edge_ids;

	auto &src = args.data[2];
	auto &target = args.data[3];

	UnifiedVectorFormat vdata_src, vdata_dst;
	src.ToUnifiedFormat(vdata_src);
	target.ToUnifiedFormat(vdata_dst);

	auto src_data = reinterpret_cast<const int64_t *>(vdata_src.data);
	auto dst_data = reinterpret_cast<const int64_t *>(vdata_dst.data);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<list_entry_t>(result);
	ValidityMask &result_validity = FlatVector::ValidityMutable(result);

	// create temp SIMD arrays
	vector<std::bitset<LANE_LIMIT>> seen(v_size);
	vector<std::bitset<LANE_LIMIT>> visit1(v_size);
	vector<std::bitset<LANE_LIMIT>> visit2(v_size);
	vector<std::vector<int64_t>> parents_v(v_size, std::vector<int64_t>(LANE_LIMIT, -1));
	vector<std::vector<int64_t>> parents_e(v_size, std::vector<int64_t>(LANE_LIMIT, -1));

	// maps lane to search number
	int64_t lane_to_num[LANE_LIMIT];
	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		lane_to_num[lane] = -1; // inactive
	}
	uint64_t total_len = 0;

	int64_t started_searches = 0;
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
				auto src_pos = vdata_src.sel->get_index(search_num);
				auto dst_pos_check = vdata_dst.sel->get_index(search_num);
				if (!vdata_src.validity.RowIsValid(src_pos) || !vdata_dst.validity.RowIsValid(dst_pos_check)) {
					result_validity.SetInvalid(search_num);
				} else {
					visit1[src_data[src_pos]][lane] = true;
					parents_v[src_data[src_pos]][lane] = src_data[src_pos]; // Mark source with source id
					parents_e[src_data[src_pos]][lane] = -2; // Mark the source with -2, there is no incoming edge for
					                                         // the source.
					lane_to_num[lane] = search_num;          // active lane
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
					auto dst_pos = vdata_dst.sel->get_index(search_num);
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
			auto src_pos = vdata_src.sel->get_index(search_num);
			auto dst_pos = vdata_dst.sel->get_index(search_num);
			if (src_data[src_pos] == dst_data[dst_pos]) { // Source == destination
				unique_ptr<Vector> output = make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
				ListVector::PushBack(*output, src_data[src_pos]);
				ListVector::Append(result, ListVector::GetChild(*output), ListVector::GetListSize(*output));
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
			ListVector::Append(result, ListVector::GetChild(*output), ListVector::GetListSize(*output));
			total_len += result_data[search_num].length;
		}
	}
	duckpgq_state->csr_to_delete.insert(info.csr_id);
}

//------------------------------------------------------------------------------
// shortestpathbounded(csr_id, v_size, src, dst, lower, upper)
//
// Returns the shortest walk from src to dst whose length lies in [lower, upper], as the same list of alternating
// vertex and edge ids that shortestpath() returns, or NULL when no such walk exists. Walks may revisit vertices, so
// a source reaches itself through a cycle when lower > 0.
//
// Every walk of length >= lower passes, after exactly lower steps, through a vertex of F_lower (the vertices reachable
// in exactly lower steps). The search therefore expands the exact-length frontiers F_0 .. F_lower without pruning,
// keeping one parent per vertex and level, and then runs an ordinary BFS from F_lower up to the upper bound.
//------------------------------------------------------------------------------
namespace {

struct BoundedSearch {
	int64_t lower = 0;
	int64_t upper = 0;
	//! Parents per level of the exact-length phase: level_parent_v[k][v] is the predecessor of v at level k + 1.
	vector<vector<int64_t>> level_parent_v;
	vector<vector<int64_t>> level_parent_e;
	//! BFS phase from F_lower: distance beyond lower (-1 = not reached) and parents.
	vector<int64_t> bfs_dist;
	vector<int64_t> bfs_parent_v;
	vector<int64_t> bfs_parent_e;

	void Run(int64_t v_size, const int64_t *v, const vector<int64_t> &e, const vector<int64_t> &edge_ids, int64_t src,
	         int64_t lower_p, int64_t upper_p) {
		lower = lower_p;
		upper = upper_p;
		level_parent_v.clear();
		level_parent_e.clear();
		bfs_dist.assign(v_size, -1);
		bfs_parent_v.assign(v_size, -1);
		bfs_parent_e.assign(v_size, -1);

		vector<bool> frontier(v_size, false);
		vector<bool> next(v_size, false);
		frontier[src] = true;
		for (int64_t level = 0; level < lower; level++) {
			vector<int64_t> parent_v(v_size, -1);
			vector<int64_t> parent_e(v_size, -1);
			bool any = false;
			std::fill(next.begin(), next.end(), false);
			for (int64_t u = 0; u < v_size; u++) {
				if (!frontier[u]) {
					continue;
				}
				for (auto pos = v[u]; pos < v[u + 1]; pos++) {
					auto n = e[pos];
					if (!next[n]) {
						next[n] = true;
						parent_v[n] = u;
						parent_e[n] = edge_ids[pos];
						any = true;
					}
				}
			}
			level_parent_v.push_back(std::move(parent_v));
			level_parent_e.push_back(std::move(parent_e));
			std::swap(frontier, next);
			if (!any) {
				return; // no walk is long enough
			}
		}

		for (int64_t u = 0; u < v_size; u++) {
			if (frontier[u]) {
				bfs_dist[u] = 0;
			}
		}
		for (int64_t extra = 1; lower + extra <= upper; extra++) {
			bool any = false;
			std::fill(next.begin(), next.end(), false);
			for (int64_t u = 0; u < v_size; u++) {
				if (!frontier[u]) {
					continue;
				}
				for (auto pos = v[u]; pos < v[u + 1]; pos++) {
					auto n = e[pos];
					if (bfs_dist[n] == -1) {
						bfs_dist[n] = extra;
						bfs_parent_v[n] = u;
						bfs_parent_e[n] = edge_ids[pos];
						next[n] = true;
						any = true;
					}
				}
			}
			std::swap(frontier, next);
			if (!any) {
				break;
			}
		}
	}

	//! Writes the walk to dst as [src, e, v, ..., e, dst]; returns false if dst is not reachable within the bounds.
	bool Reconstruct(int64_t dst, vector<int64_t> &output) const {
		output.clear();
		if (bfs_dist[dst] == -1) {
			return false;
		}
		auto current = dst;
		output.push_back(current);
		while (bfs_dist[current] > 0) {
			output.push_back(bfs_parent_e[current]);
			current = bfs_parent_v[current];
			output.push_back(current);
		}
		for (auto level = lower; level > 0; level--) {
			output.push_back(level_parent_e[level - 1][current]);
			current = level_parent_v[level - 1][current];
			output.push_back(current);
		}
		std::reverse(output.begin(), output.end());
		return true;
	}
};

} // namespace

static void ShortestPathBoundedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.BindInfo()->Cast<IterativeLengthFunctionData>();
	auto duckpgq_state = GetDuckPGQState(info.context);

	auto csr_entry = duckpgq_state->csr_list.find(info.csr_id);
	if (csr_entry == duckpgq_state->csr_list.end()) {
		throw ConstraintException("Invalid ID");
	}
	auto &csr = csr_entry->second;
	if (!csr->initialized_v) {
		throw ConstraintException("Need to initialize CSR before doing shortest path");
	}
	int64_t v_size = args.data[1].GetValue(0).GetValue<int64_t>();
	auto *v = reinterpret_cast<int64_t *>(csr->v);
	vector<int64_t> &e = csr->e;
	vector<int64_t> &edge_ids = csr->edge_ids;

	UnifiedVectorFormat vdata_src, vdata_dst, vdata_lower, vdata_upper;
	args.data[2].ToUnifiedFormat(vdata_src);
	args.data[3].ToUnifiedFormat(vdata_dst);
	args.data[4].ToUnifiedFormat(vdata_lower);
	args.data[5].ToUnifiedFormat(vdata_upper);
	auto src_data = reinterpret_cast<const int64_t *>(vdata_src.data);
	auto dst_data = reinterpret_cast<const int64_t *>(vdata_dst.data);
	auto lower_data = reinterpret_cast<const int64_t *>(vdata_lower.data);
	auto upper_data = reinterpret_cast<const int64_t *>(vdata_upper.data);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<list_entry_t>(result);
	ValidityMask &result_validity = FlatVector::ValidityMutable(result);

	// Rows sharing a source and bounds share one search: visit them grouped.
	struct Row {
		idx_t row;
		int64_t src;
		int64_t lower;
		int64_t upper;
	};
	vector<Row> rows;
	for (idx_t i = 0; i < args.size(); i++) {
		auto src_pos = vdata_src.sel->get_index(i);
		auto dst_pos = vdata_dst.sel->get_index(i);
		auto lower_pos = vdata_lower.sel->get_index(i);
		auto upper_pos = vdata_upper.sel->get_index(i);
		if (!vdata_src.validity.RowIsValid(src_pos) || !vdata_dst.validity.RowIsValid(dst_pos) ||
		    !vdata_lower.validity.RowIsValid(lower_pos) || !vdata_upper.validity.RowIsValid(upper_pos)) {
			result_validity.SetInvalid(i);
			continue;
		}
		auto src = src_data[src_pos];
		auto dst = dst_data[dst_pos];
		auto lower = MaxValue<int64_t>(lower_data[lower_pos], 0);
		auto upper = upper_data[upper_pos];
		if (src < 0 || src >= v_size || dst < 0 || dst >= v_size || upper < lower) {
			result_validity.SetInvalid(i);
			continue;
		}
		rows.push_back(Row {i, src, lower, upper});
	}
	std::stable_sort(rows.begin(), rows.end(), [](const Row &a, const Row &b) {
		if (a.src != b.src) {
			return a.src < b.src;
		}
		if (a.lower != b.lower) {
			return a.lower < b.lower;
		}
		return a.upper < b.upper;
	});

	BoundedSearch search;
	vector<int64_t> output_vector;
	uint64_t total_len = 0;
	for (idx_t i = 0; i < rows.size(); i++) {
		auto &row = rows[i];
		if (i == 0 || rows[i - 1].src != row.src || rows[i - 1].lower != row.lower || rows[i - 1].upper != row.upper) {
			search.Run(v_size, v, e, edge_ids, row.src, row.lower, row.upper);
		}
		auto dst = dst_data[vdata_dst.sel->get_index(row.row)];
		if (!search.Reconstruct(dst, output_vector)) {
			result_validity.SetInvalid(row.row);
			continue;
		}
		auto output = make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
		for (auto val : output_vector) {
			ListVector::PushBack(*output, Value::BIGINT(val));
		}
		result_data[row.row].offset = total_len;
		result_data[row.row].length = ListVector::GetListSize(*output);
		ListVector::Append(result, ListVector::GetChild(*output), ListVector::GetListSize(*output));
		total_len += result_data[row.row].length;
	}
	duckpgq_state->csr_to_delete.insert(info.csr_id);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterShortestPathScalarFunction(ExtensionLoader &loader) {
	loader.RegisterFunction(ScalarFunction(
	    "shortestpath", {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    LogicalType::LIST(LogicalType::BIGINT), ShortestPathFunction,
	    IterativeLengthFunctionData::IterativeLengthBind));
	loader.RegisterFunction(ScalarFunction("shortestpathbounded",
	                                       {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT,
	                                        LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                       LogicalType::LIST(LogicalType::BIGINT), ShortestPathBoundedFunction,
	                                       IterativeLengthFunctionData::IterativeLengthBind));
}

} // namespace duckdb
