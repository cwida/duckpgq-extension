#include "duckpgq/core/operator/bidirectional_pair_search.hpp"

namespace duckdb {

bool SearchGraphSide::HasRowLookup() const {
	if (csrs.empty()) {
		return false;
	}
	for (auto &csr : csrs) {
		if (!csr || !csr->segments.empty()) {
			return false;
		}
	}
	for (auto &delta : delta_csrs) {
		if (delta && !delta->segments.empty()) {
			return false;
		}
	}
	return true;
}

BidirectionalPairSearch::BidirectionalPairSearch(SearchGraphSide forward, SearchGraphSide backward, idx_t v_size_p,
                                                 bool track_parents_p, idx_t max_depth_p,
                                                 idx_t sparse_frontier_divisor_p)
    : v_size(v_size_p), track_parents(track_parents_p), max_depth(max_depth_p),
      sparse_frontier_divisor(sparse_frontier_divisor_p) {
	sides[0].graph = std::move(forward);
	sides[1].graph = std::move(backward);
}

bool BidirectionalPairSearch::Start(int64_t source, int64_t target, idx_t worker_count_p) {
	if (source < 0 || target < 0 || static_cast<idx_t>(source) >= v_size || static_cast<idx_t>(target) >= v_size) {
		return false;
	}
	source_vertex = NumericCast<uint32_t>(source);
	target_vertex = NumericCast<uint32_t>(target);
	if (source_vertex == target_vertex) {
		meet_vertex = source_vertex;
		length = 0;
		return false;
	}
	worker_count = std::max<idx_t>(1, worker_count_p);
	for (auto &side : sides) {
		vector<atomic<uint64_t>>((v_size + 63) / 64).swap(side.seen);
		side.next_lists.assign(worker_count, {});
		if (track_parents) {
			side.discoveries.assign(worker_count, {});
		}
	}
	sides[0].MarkSeen(source_vertex);
	sides[0].frontier.push_back(source_vertex);
	sides[1].MarkSeen(target_vertex);
	sides[1].frontier.push_back(target_vertex);
	// Start() runs in the scheduler: the first level always goes to the workers
	return PrepareLevel(false);
}

bool BidirectionalPairSearch::PrepareLevel(bool run_small_levels) {
	while (true) {
		if (sides[0].frontier.empty() || sides[1].frontier.empty()) {
			return false; // one side has explored everything it reaches
		}
		if (max_depth > 0 && sides[0].depth + sides[1].depth >= max_depth) {
			return false; // the next level could only close a path longer than the bound
		}
		expanding = sides[0].frontier.size() <= sides[1].frontier.size() ? 0 : 1;
		auto &side = sides[expanding];
		frontier_cursor = 0;
		sweep = sparse_frontier_divisor == 0 || side.frontier.size() * sparse_frontier_divisor >= v_size;
		if (sweep) {
			// The frontier as a bitmap (v_size / 8 bytes: it stays in the cache while the rows are scanned)
			side.in_frontier.assign((v_size + 63) / 64, 0);
			for (auto vertex : side.frontier) {
				side.in_frontier[vertex >> 6] |= uint64_t(1) << (vertex & 63);
			}
			auto partitions = std::max<idx_t>(1, side.graph.csrs.size());
			sweep_sub_ranges = std::min<idx_t>(
			    SWEEP_MAX_SUB_RANGES,
			    std::max<idx_t>(1, (worker_count * SWEEP_TASKS_PER_WORKER + partitions - 1) / partitions));
			sweep_cursor = 0;
			sweeps++;
			return true;
		}
		if (!run_small_levels || worker_count == 1 || side.frontier.size() > FRONTIER_SLICE) {
			return true;
		}
		// One slice is one worker's work: cheaper here than as a phase of tasks
		RunLevel(0);
		if (!FinishLevel()) {
			return false;
		}
	}
}

void BidirectionalPairSearch::RunLevel(idx_t worker_id) {
	auto &side = sides[expanding];
	auto &other = sides[1 - expanding];
	auto &frontier = side.frontier;
	auto &next = side.next_lists[worker_id];
	auto log = track_parents ? &side.discoveries[worker_id] : nullptr;
	if (sweep) {
		auto &graph = side.graph;
		auto task_count = graph.csrs.size() * sweep_sub_ranges;
		while (meet_vertex.load(std::memory_order_relaxed) == NO_VERTEX) {
			auto task_idx = sweep_cursor.fetch_add(1);
			if (task_idx >= task_count) {
				break;
			}
			auto partition_idx = task_idx / sweep_sub_ranges;
			auto sub = task_idx % sweep_sub_ranges;
			auto suppressed_bits = partition_idx < graph.suppressed.size() && graph.suppressed[partition_idx]
			                           ? graph.suppressed[partition_idx]->data()
			                           : nullptr;
			if (!ExplorePartition(side, other, *graph.csrs[partition_idx], suppressed_bits, partition_idx, false, sub,
			                      sweep_sub_ranges, next, log)) {
				break;
			}
			if (partition_idx < graph.delta_csrs.size() && graph.delta_csrs[partition_idx] &&
			    !ExplorePartition(side, other, *graph.delta_csrs[partition_idx], nullptr, partition_idx, true, sub,
			                      sweep_sub_ranges, next, log)) {
				break;
			}
		}
		return;
	}
	while (meet_vertex.load(std::memory_order_relaxed) == NO_VERTEX) {
		auto begin = frontier_cursor.fetch_add(FRONTIER_SLICE);
		if (begin >= frontier.size()) {
			break;
		}
		auto end = std::min<idx_t>(frontier.size(), begin + FRONTIER_SLICE);
		for (idx_t i = begin; i < end; i++) {
			if (!ExpandVertex(side, other, frontier[i], next, log)) {
				return;
			}
		}
	}
}

bool BidirectionalPairSearch::AdvanceLevel() {
	return FinishLevel() && PrepareLevel(true);
}

bool BidirectionalPairSearch::FinishLevel() {
	auto &side = sides[expanding];
	side.depth++;
	side.frontier.clear();
	for (auto &next : side.next_lists) {
		discovered += next.size();
		if (side.frontier.empty()) {
			side.frontier.swap(next);
		} else {
			side.frontier.insert(side.frontier.end(), next.begin(), next.end());
		}
		next.clear();
	}
	if (meet_vertex.load(std::memory_order_relaxed) != NO_VERTEX) {
		length = NumericCast<int64_t>(sides[0].depth + sides[1].depth);
		return false;
	}
	return true;
}

bool BidirectionalPairSearch::RelaxRow(Side &side, const Side &other, const LocalCSR &csr, uint32_t start_edges,
                                       uint32_t end_edges, const uint64_t *suppressed_bits, uint32_t from,
                                       idx_t partition_idx, bool is_delta, vector<uint32_t> &next,
                                       vector<Discovery> *log) {
	for (auto offset = start_edges; offset < end_edges; offset++) {
		if (suppressed_bits && ((suppressed_bits[offset >> 6] >> (offset & 63)) & 1)) {
			continue;
		}
		auto target = csr.e[offset] + csr.start_vertex;
		if (!side.MarkSeen(target)) {
			continue;
		}
		if (log) {
			log->push_back({NumericCast<uint32_t>(target), from, PackEdge(partition_idx, offset, is_delta)});
		}
		// The other side is not expanded during this level, so its seen bits are stable
		if (other.Seen(target)) {
			uint32_t none = NO_VERTEX;
			meet_vertex.compare_exchange_strong(none, NumericCast<uint32_t>(target), std::memory_order_relaxed);
			return false;
		}
		next.push_back(NumericCast<uint32_t>(target));
	}
	return true;
}

bool BidirectionalPairSearch::ExplorePartition(Side &side, const Side &other, const LocalCSR &csr,
                                               const uint64_t *suppressed_bits, idx_t partition_idx, bool is_delta,
                                               idx_t sub, idx_t subs, vector<uint32_t> &next,
                                               vector<Discovery> *log) {
	auto in_frontier = side.in_frontier.data();
	if (csr.HasSparseRows()) {
		auto rows = csr.source_vertices.size();
		for (idx_t row_idx = rows * sub / subs; row_idx < rows * (sub + 1) / subs; row_idx++) {
			auto from = csr.source_vertices[row_idx];
			if (!((in_frontier[from >> 6] >> (from & 63)) & 1)) {
				continue;
			}
			if (!RelaxRow(side, other, csr, csr.row_offsets[row_idx], csr.row_offsets[row_idx + 1], suppressed_bits,
			              from, partition_idx, is_delta, next, log)) {
				return false;
			}
		}
		return true;
	}
	auto vertices = csr.GetVertexSize();
	for (idx_t vertex = vertices * sub / subs; vertex < vertices * (sub + 1) / subs; vertex++) {
		if (!((in_frontier[vertex >> 6] >> (vertex & 63)) & 1)) {
			continue;
		}
		if (!RelaxRow(side, other, csr, csr.v[vertex].load(std::memory_order_relaxed),
		              csr.v[vertex + 1].load(std::memory_order_relaxed), suppressed_bits,
		              NumericCast<uint32_t>(vertex), partition_idx, is_delta, next, log)) {
			return false;
		}
	}
	return true;
}

bool BidirectionalPairSearch::ExpandVertex(Side &side, const Side &other, uint32_t vertex, vector<uint32_t> &next,
                                           vector<Discovery> *log) {
	auto &graph = side.graph;
	for (idx_t partition_idx = 0; partition_idx < graph.csrs.size(); partition_idx++) {
		auto &csr = *graph.csrs[partition_idx];
		uint32_t start_edges;
		uint32_t end_edges;
		if (csr.GetRowEdges(vertex, start_edges, end_edges)) {
			auto suppressed_bits = partition_idx < graph.suppressed.size() && graph.suppressed[partition_idx]
			                           ? graph.suppressed[partition_idx]->data()
			                           : nullptr;
			if (!RelaxRow(side, other, csr, start_edges, end_edges, suppressed_bits, vertex, partition_idx, false,
			              next, log)) {
				return false;
			}
		}
		if (partition_idx < graph.delta_csrs.size() && graph.delta_csrs[partition_idx]) {
			auto &delta = *graph.delta_csrs[partition_idx];
			if (delta.GetRowEdges(vertex, start_edges, end_edges) &&
			    !RelaxRow(side, other, delta, start_edges, end_edges, nullptr, vertex, partition_idx, true, next,
			              log)) {
				return false;
			}
		}
	}
	return true;
}

const BidirectionalPairSearch::Discovery &BidirectionalPairSearch::Side::DiscoveryOf(uint32_t vertex) const {
	for (auto &log : discoveries) {
		for (auto &discovery : log) {
			if (discovery.vertex == vertex) {
				return discovery;
			}
		}
	}
	throw InternalException("Bidirectional search: a vertex on the path has no logged discovery");
}

bool BidirectionalPairSearch::EdgeRowId(const SearchGraphSide &graph, uint64_t packed, row_t &row) {
	if (packed == INVALID_EDGE) {
		return false;
	}
	bool is_delta = (packed >> 63) != 0;
	idx_t partition_idx = (packed >> 32) & 0x7FFFFFFF;
	auto offset = static_cast<uint32_t>(packed & 0xFFFFFFFF);
	auto &partitions = is_delta ? graph.delta_csrs : graph.csrs;
	if (partition_idx >= partitions.size() || !partitions[partition_idx] || !partitions[partition_idx]->HasRowIds()) {
		return false;
	}
	row = partitions[partition_idx]->row_ids[offset];
	return true;
}

bool BidirectionalPairSearch::FindEdgeRowId(const SearchGraphSide &graph, uint32_t from, uint32_t to, row_t &row) {
	auto find_in = [&](const LocalCSR &csr, const uint64_t *suppressed_bits) {
		uint32_t start_edges;
		uint32_t end_edges;
		if (!csr.HasRowIds() || to < csr.start_vertex || !csr.GetRowEdges(from, start_edges, end_edges)) {
			return false;
		}
		for (auto offset = start_edges; offset < end_edges; offset++) {
			if (suppressed_bits && ((suppressed_bits[offset >> 6] >> (offset & 63)) & 1)) {
				continue;
			}
			if (csr.e[offset] + csr.start_vertex == to) {
				row = csr.row_ids[offset];
				return true;
			}
		}
		return false;
	};
	for (idx_t partition_idx = 0; partition_idx < graph.csrs.size(); partition_idx++) {
		auto suppressed_bits = partition_idx < graph.suppressed.size() && graph.suppressed[partition_idx]
		                           ? graph.suppressed[partition_idx]->data()
		                           : nullptr;
		if (find_in(*graph.csrs[partition_idx], suppressed_bits)) {
			return true;
		}
		if (partition_idx < graph.delta_csrs.size() && graph.delta_csrs[partition_idx] &&
		    find_in(*graph.delta_csrs[partition_idx], nullptr)) {
			return true;
		}
	}
	return false;
}

void BidirectionalPairSearch::AppendPath(vector<int64_t> &values, vector<bool> &valid) const {
	if (length < 0) {
		return;
	}
	if (length == 0) {
		values.push_back(source_vertex);
		valid.push_back(true);
		return;
	}
	if (!track_parents) {
		throw InternalException("Bidirectional search: a path was requested without parent tracking");
	}
	auto meet = meet_vertex.load(std::memory_order_relaxed);
	// Forward half, walked from the meeting vertex back to the source: [meet, edge, parent, ..., source]
	vector<int64_t> reversed;
	vector<bool> reversed_valid;
	auto current = meet;
	reversed.push_back(current);
	reversed_valid.push_back(true);
	while (current != source_vertex) {
		auto &discovery = sides[0].DiscoveryOf(current);
		row_t row = 0;
		bool has_row = EdgeRowId(sides[0].graph, discovery.edge, row);
		reversed.push_back(has_row ? row : 0);
		reversed_valid.push_back(has_row);
		current = discovery.parent;
		reversed.push_back(current);
		reversed_valid.push_back(true);
	}
	for (idx_t i = reversed.size(); i > 0; i--) {
		values.push_back(reversed[i - 1]);
		valid.push_back(reversed_valid[i - 1]);
	}
	// Backward half, from the meeting vertex on to the destination. The backward side discovered `current` through
	// the reverse edge next -> current, which is the edge current -> next of the graph: its row id comes from the
	// reverse partition when that carries row ids (an undirected index, whose mirror has the edge's row id), and
	// from the forward partitions otherwise (a stored reverse side has none)
	current = meet;
	while (current != target_vertex) {
		auto &discovery = sides[1].DiscoveryOf(current);
		auto next = discovery.parent;
		row_t row = 0;
		bool has_row = EdgeRowId(sides[1].graph, discovery.edge, row) ||
		               FindEdgeRowId(sides[0].graph, current, next, row);
		values.push_back(has_row ? row : 0);
		valid.push_back(has_row);
		values.push_back(next);
		valid.push_back(true);
		current = next;
	}
}

} // namespace duckdb
