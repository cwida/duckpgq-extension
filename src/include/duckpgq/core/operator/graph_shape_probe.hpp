#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/bidirectional_pair_search.hpp"

namespace duckdb {

//! What a few bounded searches from a query's sources say about the shape of the graph around them
struct GraphShapeProbeResult {
	//! Searches run (sources inside the graph)
	idx_t probes = 0;
	//! Searches still expanding after the level limit, below the vertex budget
	idx_t deep = 0;
	//! Searches that reached the vertex budget (or the edge budget) within the level limit
	idx_t wide = 0;
	//! Searches that ran out of vertices within the level limit
	idx_t exhausted = 0;
	//! Most levels any search expanded
	idx_t max_levels = 0;
	//! Most vertices a deep search had discovered at the level limit (how far it stayed below the budget)
	idx_t max_deep_discovered = 0;

	//! Searches planned: MAX_PROBES or the number of sources; the run stops once more than half of them agree
	idx_t planned = 0;

	//! More than half of the planned searches were deep
	bool Deep() const {
		return deep * 2 > planned;
	}
};

//! Decides between the two kernels for many sources by the shape of the graph, not only by the source count.
//!
//! An MS-BFS wave of one lane per source pays for every level a pass over all vertices (the check phase and the
//! summary bitmap), and it runs as many levels as its slowest lane. On a small-world graph a search is over in a
//! handful of levels and the lanes share most of their work. On a sparse graph of high diameter (a road network: a
//! path of several hundred edges, frontiers that are thin rings in different places) the wave runs hundreds of levels
//! in which the lanes share nothing, and one single-source search per source is several times faster (OSM Oberbayern,
//! 1000 pairs: 4.26 s on lanes, 1.58 s per source).
//!
//! The probe tells the two apart by how fast a search grows: breadth-first from a source, vertex by vertex through
//! the row lookup of the partitions, until it has discovered VERTEX_BUDGET vertices (a quarter of the graph's
//! vertices when that is less), scanned EDGES_PER_VERTEX times as many edges, run
//! out of vertices, or expanded `level_limit` levels. Only the last outcome is "deep". Growth is exponential in the
//! level on a small-world graph (the budget is reached within a few levels) and about quadratic on a road network (a
//! few hundred vertices after 16 levels). A search that runs out of vertices early says nothing about depth and
//! counts as not deep. The probe starts from MAX_PROBES of the query's own sources, spread evenly over them, and
//! the majority decides (two that agree end the probe); its cost is bounded by the budgets, independent of the size
//! of the graph.
class GraphShapeProbe {
public:
	static constexpr idx_t MAX_PROBES = 3;
	static constexpr idx_t VERTEX_BUDGET = 4096;
	//! The vertex budget is a quarter of the graph's vertices when that is less (at least MIN_VERTEX_BUDGET)
	static constexpr idx_t MIN_VERTEX_BUDGET = 16;
	//! Edge budget: this many scanned edges per vertex of the vertex budget
	static constexpr idx_t EDGES_PER_VERTEX = 16;

	//! Serial. `sources` are dense vertex ids (negative or beyond v_size: outside the graph, skipped); `graph` needs
	//! the row lookup (SearchGraphSide::HasRowLookup)
	static GraphShapeProbeResult Run(const SearchGraphSide &graph, idx_t v_size, const vector<int64_t> &sources,
	                                 idx_t level_limit);
};

} // namespace duckdb
