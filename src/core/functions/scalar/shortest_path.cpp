#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/storage/buffer_manager.hpp"
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
// iterativelengthbounded(csr_id, v_size, src, dst, lower, upper)
//
// shortestpathbounded returns the shortest walk from src to dst whose length lies in [lower, upper], as the same list
// of alternating vertex and edge ids that shortestpath() returns, or NULL when no such walk exists.
// iterativelengthbounded runs the same search and returns only the length of that walk (or NULL), without storing
// parents; it is the filter, shortestpathbounded builds the path for the rows that pass it. Walks may revisit
// vertices, so a source reaches itself through a cycle when lower > 0.
//
// Every walk of length >= lower passes, after exactly lower steps, through a vertex of F_lower (the vertices reachable
// in exactly lower steps). The search therefore expands the exact-length frontiers F_0 .. F_lower without pruning,
// keeping one parent per frontier entry, and then runs an ordinary BFS from F_lower up to the upper bound.
// iterativelengthbounded runs this search for up to LANE_LIMIT sources at once, one bitset lane per source, as
// iterativelength() does; with a single source it uses the single-source search, which is kept between chunks.
//------------------------------------------------------------------------------
namespace {

struct BoundedSearch {
	explicit BoundedSearch(bool store_paths_p) : store_paths(store_paths_p) {
	}

	//! Whether parents are stored, so that Reconstruct can be called (shortestpathbounded) or not
	//! (iterativelengthbounded).
	const bool store_paths;

	//! The last search, which is reused while consecutive rows share the source and bounds.
	bool has_search = false;
	int64_t v_size = -1;
	int64_t src = -1;
	int64_t lower = 0;
	int64_t upper = 0;

	//! Per-vertex buffers of size v_size, kept between searches.
	//! stamp[n] == generation: n is already in the frontier being built.
	vector<idx_t> stamp;
	idx_t generation = 0;
	//! CSR position through which n entered the level being built, and the index of its parent in the previous level.
	vector<int64_t> pending_pos;
	vector<int64_t> pending_parent;
	//! Index of n in F_lower.
	vector<int64_t> index_in_lower;
	//! BFS phase from F_lower: distance beyond lower (-1 = not reached) and parents.
	vector<int64_t> bfs_dist;
	vector<int64_t> bfs_parent_v;
	vector<int64_t> bfs_parent_e;
	//! Vertices whose bfs_dist was set, so that the next search resets only those.
	vector<int64_t> bfs_touched;

	//! Frontiers as sorted vertex lists. Sorting keeps the parent choice of a scan in vertex order.
	vector<int64_t> frontier;
	vector<int64_t> next;

	//! Exact-length phase, stored only for the entries of each frontier: entry j of level k (1 <= k <= lower) is at
	//! level_start[k - 1] + j. level_pos is the CSR position of the edge into the entry, so the entry's vertex is
	//! e[level_pos] and its edge edge_ids[level_pos]; level_parent is the index of its parent in level k - 1.
	vector<idx_t> level_start;
	vector<int64_t> level_pos;
	vector<int64_t> level_parent;

	void Resize(int64_t v_size_p) {
		v_size = v_size_p;
		auto size = NumericCast<idx_t>(v_size);
		stamp.assign(size, 0);
		generation = 0;
		bfs_dist.assign(size, -1);
		bfs_touched.clear();
		if (store_paths) {
			pending_pos.assign(size, -1);
			pending_parent.assign(size, -1);
			index_in_lower.assign(size, -1);
			bfs_parent_v.assign(size, -1);
			bfs_parent_e.assign(size, -1);
		}
	}

	void CheckMemory(ClientContext &context) const {
		auto bytes =
		    (level_pos.capacity() + level_parent.capacity()) * sizeof(int64_t) + level_start.capacity() * sizeof(idx_t);
		auto limit = BufferManager::GetBufferManager(context).GetMaxMemory();
		if (bytes > limit) {
			throw OutOfMemoryException("shortestpathbounded: the walks of length %lld from vertex %lld need more than "
			                           "the memory limit (%llu bytes)",
			                           lower, src, limit);
		}
	}

	void Run(ClientContext &context, int64_t v_size_p, const int64_t *v, const vector<int64_t> &e,
	         const vector<int64_t> &edge_ids, int64_t src_p, int64_t lower_p, int64_t upper_p) {
		if (has_search && v_size == v_size_p && src == src_p && lower == lower_p && upper == upper_p) {
			return;
		}
		has_search = false;
		if (v_size != v_size_p) {
			Resize(v_size_p);
		} else {
			for (auto n : bfs_touched) {
				bfs_dist[n] = -1;
			}
			bfs_touched.clear();
		}
		src = src_p;
		lower = lower_p;
		upper = upper_p;
		level_start.clear();
		level_pos.clear();
		level_parent.clear();

		frontier.clear();
		frontier.push_back(src);
		bool reached_lower = true;
		for (int64_t level = 1; level <= lower; level++) {
			context.InterruptCheck();
			generation++;
			next.clear();
			for (idx_t i = 0; i < frontier.size(); i++) {
				auto u = frontier[i];
				for (auto pos = v[u]; pos < v[u + 1]; pos++) {
					auto n = e[pos];
					if (stamp[n] != generation) {
						stamp[n] = generation;
						next.push_back(n);
						if (store_paths) {
							pending_pos[n] = pos;
							pending_parent[n] = NumericCast<int64_t>(i);
						}
					}
				}
			}
			if (next.empty()) {
				reached_lower = false; // no walk is long enough
				break;
			}
			std::sort(next.begin(), next.end());
			if (store_paths) {
				level_start.push_back(level_pos.size());
				for (auto n : next) {
					level_pos.push_back(pending_pos[n]);
					level_parent.push_back(pending_parent[n]);
				}
				CheckMemory(context);
			}
			std::swap(frontier, next);
		}
		has_search = true;
		if (!reached_lower) {
			return;
		}

		for (idx_t i = 0; i < frontier.size(); i++) {
			auto n = frontier[i];
			bfs_dist[n] = 0;
			bfs_touched.push_back(n);
			if (store_paths) {
				index_in_lower[n] = NumericCast<int64_t>(i);
			}
		}
		for (int64_t extra = 1; extra <= upper - lower; extra++) {
			context.InterruptCheck();
			next.clear();
			for (auto u : frontier) {
				for (auto pos = v[u]; pos < v[u + 1]; pos++) {
					auto n = e[pos];
					if (bfs_dist[n] == -1) {
						bfs_dist[n] = extra;
						bfs_touched.push_back(n);
						if (store_paths) {
							bfs_parent_v[n] = u;
							bfs_parent_e[n] = edge_ids[pos];
						}
						next.push_back(n);
					}
				}
			}
			if (next.empty()) {
				break;
			}
			std::sort(next.begin(), next.end());
			std::swap(frontier, next);
		}
	}

	//! Length of the shortest walk to dst within the bounds, or -1 if there is none.
	int64_t Length(int64_t dst) const {
		return bfs_dist[dst] == -1 ? -1 : lower + bfs_dist[dst];
	}

	//! Writes the walk to dst as [src, e, v, ..., e, dst]; returns false if dst is not reachable within the bounds.
	bool Reconstruct(int64_t dst, const vector<int64_t> &e, const vector<int64_t> &edge_ids,
	                 vector<int64_t> &output) const {
		D_ASSERT(store_paths);
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
		auto index = index_in_lower[current];
		for (auto level = lower; level > 0; level--) {
			auto entry = level_start[level - 1] + NumericCast<idx_t>(index);
			output.push_back(edge_ids[level_pos[entry]]);
			index = level_parent[entry];
			current = level == 1 ? src : e[level_pos[level_start[level - 2] + NumericCast<idx_t>(index)]];
			output.push_back(current);
		}
		std::reverse(output.begin(), output.end());
		return true;
	}
};

//! Per-thread state of shortestpathbounded and iterativelengthbounded: the buffers and the last single-source search,
//! kept between chunks, and for iterativelengthbounded the bitsets of the multi-source search.
struct BoundedSearchLocalState : public FunctionLocalState {
	explicit BoundedSearchLocalState(bool store_paths) : search(store_paths) {
	}
	BoundedSearch search;
	vector<std::bitset<LANE_LIMIT>> visit;
	vector<std::bitset<LANE_LIMIT>> next;
	vector<std::bitset<LANE_LIMIT>> seen;
};

unique_ptr<FunctionLocalState> InitBoundedPathState(ExpressionState &, const BoundFunctionExpression &,
                                                    FunctionData *) {
	return make_uniq<BoundedSearchLocalState>(true);
}

unique_ptr<FunctionLocalState> InitBoundedLengthState(ExpressionState &, const BoundFunctionExpression &,
                                                      FunctionData *) {
	return make_uniq<BoundedSearchLocalState>(false);
}

struct BoundedRow {
	idx_t row;
	int64_t src;
	int64_t dst;
	int64_t lower;
	int64_t upper;
};

struct BoundedInput {
	ClientContext &context;
	shared_ptr<DuckPGQState> duckpgq_state;
	int32_t csr_id;
	int64_t v_size;
	const int64_t *v;
	const vector<int64_t> &e;
	const vector<int64_t> &edge_ids;
	//! The rows with a valid input; the result of every other row is set to NULL.
	vector<BoundedRow> rows;
};

BoundedInput ReadBoundedInput(DataChunk &args, ExpressionState &state, Vector &result) {
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
	BoundedInput input {info.context, duckpgq_state, info.csr_id, v_size, reinterpret_cast<int64_t *>(csr->v),
	                    csr->e,       csr->edge_ids, {}};

	UnifiedVectorFormat vdata_src, vdata_dst, vdata_lower, vdata_upper;
	args.data[2].ToUnifiedFormat(vdata_src);
	args.data[3].ToUnifiedFormat(vdata_dst);
	args.data[4].ToUnifiedFormat(vdata_lower);
	args.data[5].ToUnifiedFormat(vdata_upper);
	auto src_data = reinterpret_cast<const int64_t *>(vdata_src.data);
	auto dst_data = reinterpret_cast<const int64_t *>(vdata_dst.data);
	auto lower_data = reinterpret_cast<const int64_t *>(vdata_lower.data);
	auto upper_data = reinterpret_cast<const int64_t *>(vdata_upper.data);

	ValidityMask &result_validity = FlatVector::ValidityMutable(result);
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
		input.rows.push_back(BoundedRow {i, src, dst, lower, upper});
	}
	return input;
}

//! Sorts the rows so that rows sharing a source and bounds are adjacent, with the group of the kept search first.
void SortBoundedRows(vector<BoundedRow> &rows, const BoundedSearch &search, int64_t v_size) {
	auto kept = [&](const BoundedRow &r) {
		return search.has_search && search.v_size == v_size && r.src == search.src && r.lower == search.lower &&
		       r.upper == search.upper;
	};
	std::stable_sort(rows.begin(), rows.end(), [&](const BoundedRow &a, const BoundedRow &b) {
		auto a_kept = kept(a);
		auto b_kept = kept(b);
		if (a_kept != b_kept) {
			return a_kept;
		}
		if (a.lower != b.lower) {
			return a.lower < b.lower;
		}
		if (a.upper != b.upper) {
			return a.upper < b.upper;
		}
		return a.src < b.src;
	});
}

//! Multi-source version of BoundedSearch for iterativelengthbounded: up to LANE_LIMIT distinct sources with the same
//! bounds, one bitset lane each. Writes the length of every row in rows[begin, end) or sets it to NULL.
void BoundedLengthLanes(BoundedInput &input, BoundedSearchLocalState &state, idx_t begin, idx_t end,
                        const vector<idx_t> &row_lane, int64_t *result_data, ValidityMask &result_validity) {
	auto &rows = input.rows;
	auto v_size = input.v_size;
	auto *v = input.v;
	auto &e = input.e;
	auto &visit = state.visit;
	auto &next = state.next;
	auto &seen = state.seen;
	if (visit.size() != NumericCast<idx_t>(v_size)) {
		visit.assign(NumericCast<idx_t>(v_size), 0);
		next.assign(NumericCast<idx_t>(v_size), 0);
		seen.assign(NumericCast<idx_t>(v_size), 0);
	}
	auto lower = rows[begin].lower;
	auto upper = rows[begin].upper;

	for (int64_t n = 0; n < v_size; n++) {
		visit[n] = 0;
	}
	for (idx_t i = begin; i < end; i++) {
		visit[rows[i].src][row_lane[i - begin]] = true;
	}

	auto expand = [&]() {
		bool any = false;
		for (int64_t n = 0; n < v_size; n++) {
			next[n] = 0;
		}
		for (int64_t u = 0; u < v_size; u++) {
			if (visit[u].none()) {
				continue;
			}
			any = true;
			for (auto pos = v[u]; pos < v[u + 1]; pos++) {
				next[e[pos]] |= visit[u];
			}
		}
		std::swap(visit, next);
		return any;
	};

	// Exact-length phase: no pruning, visit holds F_level per lane.
	bool reached_lower = true;
	for (int64_t level = 1; level <= lower; level++) {
		input.context.InterruptCheck();
		if (!expand()) {
			reached_lower = false;
			break;
		}
	}

	vector<idx_t> unresolved;
	if (reached_lower) {
		for (int64_t n = 0; n < v_size; n++) {
			seen[n] = visit[n];
		}
		for (idx_t i = begin; i < end; i++) {
			if (seen[rows[i].dst][row_lane[i - begin]]) {
				result_data[rows[i].row] = lower;
			} else {
				unresolved.push_back(i);
			}
		}
		// BFS phase from F_lower.
		for (int64_t extra = 1; extra <= upper - lower && !unresolved.empty(); extra++) {
			input.context.InterruptCheck();
			expand();
			bool change = false;
			for (int64_t n = 0; n < v_size; n++) {
				visit[n] &= ~seen[n];
				seen[n] |= visit[n];
				change |= visit[n].any();
			}
			if (!change) {
				break;
			}
			idx_t kept = 0;
			for (auto i : unresolved) {
				if (visit[rows[i].dst][row_lane[i - begin]]) {
					result_data[rows[i].row] = lower + extra;
				} else {
					unresolved[kept++] = i;
				}
			}
			unresolved.resize(kept);
		}
	} else {
		for (idx_t i = begin; i < end; i++) {
			unresolved.push_back(i);
		}
	}
	for (auto i : unresolved) {
		result_validity.SetInvalid(rows[i].row);
	}
}

} // namespace

static void ShortestPathBoundedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto input = ReadBoundedInput(args, state, result);
	auto &search = ExecuteFunctionState::GetFunctionState(state)->Cast<BoundedSearchLocalState>().search;
	SortBoundedRows(input.rows, search, input.v_size);

	auto result_data = FlatVector::GetDataMutable<list_entry_t>(result);
	ValidityMask &result_validity = FlatVector::ValidityMutable(result);
	vector<int64_t> output_vector;
	uint64_t total_len = 0;
	for (auto &row : input.rows) {
		// Run returns at once while the source and bounds are those of the previous row.
		search.Run(input.context, input.v_size, input.v, input.e, input.edge_ids, row.src, row.lower, row.upper);
		if (!search.Reconstruct(row.dst, input.e, input.edge_ids, output_vector)) {
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
	input.duckpgq_state->csr_to_delete.insert(input.csr_id);
}

static void IterativeLengthBoundedFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto input = ReadBoundedInput(args, state, result);
	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<BoundedSearchLocalState>();
	auto &search = local_state.search;
	SortBoundedRows(input.rows, search, input.v_size);

	auto result_data = FlatVector::GetDataMutable<int64_t>(result);
	ValidityMask &result_validity = FlatVector::ValidityMutable(result);
	auto &rows = input.rows;
	vector<idx_t> row_lane;
	idx_t begin = 0;
	while (begin < rows.size()) {
		// Take the rows with the bounds of rows[begin], up to LANE_LIMIT distinct sources.
		idx_t end = begin;
		idx_t lanes = 0;
		row_lane.clear();
		while (end < rows.size() && rows[end].lower == rows[begin].lower && rows[end].upper == rows[begin].upper) {
			if (lanes == 0 || rows[end].src != rows[end - 1].src) {
				if (lanes == LANE_LIMIT) {
					break;
				}
				lanes++;
			}
			row_lane.push_back(lanes - 1);
			end++;
		}
		if (lanes == 1) {
			// A single source: the single-source search is cheaper, and is kept for the next chunk.
			for (idx_t i = begin; i < end; i++) {
				auto &row = rows[i];
				search.Run(input.context, input.v_size, input.v, input.e, input.edge_ids, row.src, row.lower,
				           row.upper);
				auto length = search.Length(row.dst);
				if (length < 0) {
					result_validity.SetInvalid(row.row);
				} else {
					result_data[row.row] = length;
				}
			}
		} else {
			BoundedLengthLanes(input, local_state, begin, end, row_lane, result_data, result_validity);
		}
		begin = end;
	}
	input.duckpgq_state->csr_to_delete.insert(input.csr_id);
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
	                                       IterativeLengthFunctionData::IterativeLengthBind, nullptr,
	                                       InitBoundedPathState));
	loader.RegisterFunction(ScalarFunction("iterativelengthbounded",
	                                       {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT,
	                                        LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                       LogicalType::BIGINT, IterativeLengthBoundedFunction,
	                                       IterativeLengthFunctionData::IterativeLengthBind, nullptr,
	                                       InitBoundedLengthState));
}

} // namespace duckdb
