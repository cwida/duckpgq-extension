#include "duckpgq/core/operator/graph_shape_probe.hpp"

namespace duckdb {

namespace {

//! The vertices one probe has discovered: open addressing, sized for the vertex budget, so that a probe costs the
//! same on a graph of a thousand vertices and of a hundred million
class DiscoveredSet {
public:
	DiscoveredSet() : slots(CAPACITY, EMPTY) {
	}
	void Clear() {
		std::fill(slots.begin(), slots.end(), EMPTY);
	}
	//! True when the vertex was not in the set
	bool Insert(uint32_t vertex) {
		idx_t slot = (vertex * 0x9E3779B1u) >> (32 - CAPACITY_BITS);
		while (slots[slot] != EMPTY) {
			if (slots[slot] == vertex) {
				return false;
			}
			slot = (slot + 1) & (CAPACITY - 1);
		}
		slots[slot] = vertex;
		return true;
	}

private:
	// Four slots per vertex of the budget
	static constexpr idx_t CAPACITY_BITS = 14;
	static constexpr idx_t CAPACITY = idx_t(1) << CAPACITY_BITS;
	static constexpr uint32_t EMPTY = NumericLimits<uint32_t>::Maximum();
	vector<uint32_t> slots;
};

enum class ProbeOutcome : uint8_t { DEEP, WIDE, EXHAUSTED };

ProbeOutcome ProbeSource(const SearchGraphSide &graph, uint32_t source, idx_t level_limit, idx_t vertex_budget,
                         DiscoveredSet &discovered, vector<uint32_t> &frontier, vector<uint32_t> &next,
                         idx_t &levels, idx_t &discovered_count) {
	discovered.Clear();
	discovered.Insert(source);
	frontier.clear();
	frontier.push_back(source);
	discovered_count = 1;
	idx_t scanned_edges = 0;
	auto edge_budget = vertex_budget * GraphShapeProbe::EDGES_PER_VERTEX;
	levels = 0;
	while (!frontier.empty()) {
		if (levels >= level_limit) {
			return ProbeOutcome::DEEP;
		}
		next.clear();
		for (auto vertex : frontier) {
			for (idx_t partition_idx = 0; partition_idx < graph.csrs.size(); partition_idx++) {
				// The base partition with its tombstones, then its delta companion
				for (idx_t part = 0; part < 2; part++) {
					const LocalCSR *csr;
					const uint64_t *suppressed_bits = nullptr;
					if (part == 0) {
						csr = graph.csrs[partition_idx].get();
						if (partition_idx < graph.suppressed.size() && graph.suppressed[partition_idx]) {
							suppressed_bits = graph.suppressed[partition_idx]->data();
						}
					} else {
						csr = partition_idx < graph.delta_csrs.size() ? graph.delta_csrs[partition_idx].get() : nullptr;
					}
					uint32_t start_edges;
					uint32_t end_edges;
					if (!csr || !csr->GetRowEdges(vertex, start_edges, end_edges)) {
						continue;
					}
					for (auto offset = start_edges; offset < end_edges; offset++) {
						if (suppressed_bits && ((suppressed_bits[offset >> 6] >> (offset & 63)) & 1)) {
							continue;
						}
						scanned_edges++;
						auto target = NumericCast<uint32_t>(csr->e[offset] + csr->start_vertex);
						if (discovered.Insert(target)) {
							discovered_count++;
							next.push_back(target);
						}
						if (discovered_count >= vertex_budget || scanned_edges >= edge_budget) {
							levels++;
							return ProbeOutcome::WIDE;
						}
					}
				}
			}
		}
		frontier.swap(next);
		levels++;
	}
	return ProbeOutcome::EXHAUSTED;
}

} // namespace

GraphShapeProbeResult GraphShapeProbe::Run(const SearchGraphSide &graph, idx_t v_size, const vector<int64_t> &sources,
                                           idx_t level_limit) {
	GraphShapeProbeResult result;
	if (sources.empty() || level_limit == 0) {
		return result;
	}
	DiscoveredSet discovered;
	vector<uint32_t> frontier;
	vector<uint32_t> next;
	// A small graph cannot fill the budget: a quarter of its vertices within the level limit is not deep either
	auto vertex_budget = std::min<idx_t>(VERTEX_BUDGET, std::max<idx_t>(MIN_VERTEX_BUDGET, v_size / 4));
	auto probe_count = std::min<idx_t>(MAX_PROBES, sources.size());
	result.planned = probe_count;
	for (idx_t probe_idx = 0; probe_idx < probe_count; probe_idx++) {
		// Decided once more than half of the planned probes agree
		if (result.deep * 2 > probe_count || (result.probes - result.deep) * 2 > probe_count) {
			break;
		}
		auto source = sources[probe_idx * sources.size() / probe_count];
		if (source < 0 || static_cast<idx_t>(source) >= v_size) {
			continue;
		}
		idx_t levels = 0;
		idx_t discovered_count = 0;
		auto outcome = ProbeSource(graph, NumericCast<uint32_t>(source), level_limit, vertex_budget, discovered, frontier, next,
		                           levels, discovered_count);
		result.probes++;
		result.max_levels = std::max(result.max_levels, levels);
		switch (outcome) {
		case ProbeOutcome::DEEP:
			result.deep++;
			result.max_deep_discovered = std::max(result.max_deep_discovered, discovered_count);
			break;
		case ProbeOutcome::WIDE:
			result.wide++;
			break;
		default:
			result.exhausted++;
			break;
		}
	}
	return result;
}

} // namespace duckdb
