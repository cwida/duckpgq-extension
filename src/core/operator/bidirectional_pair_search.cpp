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
                                                 bool track_parents_p, idx_t max_depth_p)
    : v_size(v_size_p), track_parents(track_parents_p), max_depth(max_depth_p) {
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
		vector<atomic<uint8_t>>(v_size).swap(side.seen);
		if (track_parents) {
			side.parent_vertex = unique_ptr<uint32_t[]>(new uint32_t[v_size]);
			side.parent_edge = unique_ptr<uint64_t[]>(new uint64_t[v_size]);
		}
		side.next_lists.assign(worker_count, {});
	}
	sides[0].seen[source_vertex].store(1, std::memory_order_relaxed);
	sides[0].frontier.push_back(source_vertex);
	sides[1].seen[target_vertex].store(1, std::memory_order_relaxed);
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
		frontier_cursor = 0;
		if (!run_small_levels || worker_count == 1 || sides[expanding].frontier.size() > FRONTIER_SLICE) {
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
	while (meet_vertex.load(std::memory_order_relaxed) == NO_VERTEX) {
		auto begin = frontier_cursor.fetch_add(FRONTIER_SLICE);
		if (begin >= frontier.size()) {
			break;
		}
		auto end = std::min<idx_t>(frontier.size(), begin + FRONTIER_SLICE);
		for (idx_t i = begin; i < end; i++) {
			ExpandVertex(side, other, frontier[i], next);
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
                                       idx_t partition_idx, bool is_delta, vector<uint32_t> &next) {
	for (auto offset = start_edges; offset < end_edges; offset++) {
		if (suppressed_bits && ((suppressed_bits[offset >> 6] >> (offset & 63)) & 1)) {
			continue;
		}
		auto target = csr.e[offset] + csr.start_vertex;
		auto &seen = side.seen[target];
		if (seen.load(std::memory_order_relaxed)) {
			continue;
		}
		uint8_t expected = 0;
		if (!seen.compare_exchange_strong(expected, 1, std::memory_order_relaxed)) {
			continue;
		}
		if (track_parents) {
			side.parent_vertex[target] = from;
			side.parent_edge[target] = PackEdge(partition_idx, offset, is_delta);
		}
		// The other side is not expanded during this level, so its seen flags are stable
		if (other.seen[target].load(std::memory_order_relaxed)) {
			uint32_t none = NO_VERTEX;
			meet_vertex.compare_exchange_strong(none, NumericCast<uint32_t>(target), std::memory_order_relaxed);
			return false;
		}
		next.push_back(NumericCast<uint32_t>(target));
	}
	return true;
}

void BidirectionalPairSearch::ExpandVertex(Side &side, const Side &other, uint32_t vertex, vector<uint32_t> &next) {
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
			              next)) {
				return;
			}
		}
		if (partition_idx < graph.delta_csrs.size() && graph.delta_csrs[partition_idx]) {
			auto &delta = *graph.delta_csrs[partition_idx];
			if (delta.GetRowEdges(vertex, start_edges, end_edges) &&
			    !RelaxRow(side, other, delta, start_edges, end_edges, nullptr, vertex, partition_idx, true, next)) {
				return;
			}
		}
	}
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
		row_t row = 0;
		bool has_row = EdgeRowId(sides[0].graph, sides[0].parent_edge[current], row);
		reversed.push_back(has_row ? row : 0);
		reversed_valid.push_back(has_row);
		current = sides[0].parent_vertex[current];
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
		auto next = sides[1].parent_vertex[current];
		row_t row = 0;
		bool has_row = EdgeRowId(sides[1].graph, sides[1].parent_edge[current], row) ||
		               FindEdgeRowId(sides[0].graph, current, next, row);
		values.push_back(has_row ? row : 0);
		valid.push_back(has_row);
		values.push_back(next);
		valid.push_back(true);
		current = next;
	}
}

} // namespace duckdb
