#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

namespace duckdb {

//! The partitions of one direction of the graph with the index snapshot's overlay (empty for a plain CSR)
struct SearchGraphSide {
	std::vector<shared_ptr<LocalCSR>> csrs;
	std::vector<shared_ptr<LocalCSR>> delta_csrs;
	std::vector<shared_ptr<vector<uint64_t>>> suppressed;

	//! Whether a vertex's edges can be looked up in every partition (not on streaming segments)
	bool HasRowLookup() const;
};

//! Shortest path between one source and one destination by a search from both ends (meet in the middle).
//!
//! The forward side explores the edges from the source, the backward side the reverse edges from the destination.
//! Each level expands the side with the smaller frontier: vertex by vertex through the row lookup of the partitions
//! while the frontier is small, by a sweep over the partitions' rows once it holds more than 1 / sparse_frontier_divisor
//! of the vertices (both sides only grow that large when the destination is far or unreachable).
//! Both sides are complete to their depth and share no vertex until the search ends, so no path of length <=
//! depth(forward) + depth(backward) exists; the first vertex an expansion discovers that the other side has seen
//! therefore closes a path of exactly depth(forward) + depth(backward) + 1 edges, the shortest, and the search stops
//! there without finishing the level. A side whose frontier empties has explored everything it can reach: no path.
//!
//! Per vertex and side the search keeps one bit (seen) and, from its first sweep on, one bit for the membership of
//! the frontier being swept. For paths every discovery is logged with the vertex and the edge it came through (16
//! bytes per discovered vertex, in discovery order per worker); the few vertices of the path are looked up by a scan
//! of the log, so the search touches no per-vertex parent arrays (two arrays of 12 bytes per vertex cost a page
//! fault per discovery on a graph of a million vertices). Discovery is an atomic test-and-set of the seen bit, so the
//! workers of a level expand slices of the frontier concurrently; a level whose frontier fits one slice runs in the
//! serial step between two phases instead of as tasks.
class BidirectionalPairSearch {
public:
	static constexpr uint64_t INVALID_EDGE = NumericLimits<uint64_t>::Maximum();

	//! max_depth: longest path to report in edges (0 = unbounded); sparse_frontier_divisor: 0 always sweeps
	BidirectionalPairSearch(SearchGraphSide forward, SearchGraphSide backward, idx_t v_size, bool track_parents,
	                        idx_t max_depth, idx_t sparse_frontier_divisor);

	//! Serial; false when the search is over at once (same vertex, or a vertex outside the graph)
	bool Start(int64_t source, int64_t target, idx_t worker_count);
	//! The parallel part of the current level on one worker
	void RunLevel(idx_t worker_id);
	//! Serial, after every worker has finished the level; true when another level needs the workers
	bool AdvanceLevel();

	//! Edges on the shortest path, -1 when the destination is unreachable (within max_depth)
	int64_t Length() const {
		return length;
	}
	//! Appends the path as [v0, e0, v1, ..., vk]: dense vertex ids and edge-table row ids (invalid when unknown)
	void AppendPath(vector<int64_t> &values, vector<bool> &valid) const;

	//! Levels expanded and vertices discovered, both sides together
	idx_t Levels() const {
		return sides[0].depth + sides[1].depth;
	}
	idx_t Discovered() const {
		return discovered;
	}
	//! Levels expanded by a sweep over the partitions
	idx_t Sweeps() const {
		return sweeps;
	}

private:
	//! A vertex with the vertex and the packed edge it was discovered through
	struct Discovery {
		uint32_t vertex;
		uint32_t parent;
		uint64_t edge;
	};

	struct Side {
		SearchGraphSide graph;
		//! One bit per vertex
		vector<atomic<uint64_t>> seen;
		vector<uint32_t> frontier;
		vector<vector<uint32_t>> next_lists;
		//! Paths: the discoveries of each worker
		vector<vector<Discovery>> discoveries;

		bool Seen(idx_t vertex) const {
			return (seen[vertex >> 6].load(std::memory_order_relaxed) >> (vertex & 63)) & 1;
		}
		//! True when this call set the bit
		bool MarkSeen(idx_t vertex) {
			auto mask = uint64_t(1) << (vertex & 63);
			auto &word = seen[vertex >> 6];
			if (word.load(std::memory_order_relaxed) & mask) {
				return false;
			}
			return !(word.fetch_or(mask, std::memory_order_relaxed) & mask);
		}
		//! The discovery of `vertex`; throws when it was never logged
		const Discovery &DiscoveryOf(uint32_t vertex) const;
		//! One bit per vertex: the frontier of the level being swept; allocated by the first sweep
		vector<uint64_t> in_frontier;
		idx_t depth = 0;
	};

	static constexpr idx_t FRONTIER_SLICE = 64;
	static constexpr uint32_t NO_VERTEX = NumericLimits<uint32_t>::Maximum();

	static uint64_t PackEdge(idx_t partition_idx, uint32_t offset, bool is_delta) {
		return (is_delta ? (uint64_t(1) << 63) : 0) | (static_cast<uint64_t>(partition_idx) << 32) | offset;
	}

	//! Chooses the side of the next level and runs levels of one slice in place; false when the search is over
	bool PrepareLevel(bool run_small_levels);
	//! Merges the discoveries of the level into the side's frontier; false when the sides met
	bool FinishLevel();
	//! False when the sides met at one of the vertex's edges
	bool ExpandVertex(Side &side, const Side &other, uint32_t vertex, vector<uint32_t> &next,
	                  vector<Discovery> *log);
	//! Sweep: the rows [rows x sub / subs, rows x (sub + 1) / subs) of one partition whose source is in the frontier;
	//! false when the sides met
	bool ExplorePartition(Side &side, const Side &other, const LocalCSR &csr, const uint64_t *suppressed_bits,
	                      idx_t partition_idx, bool is_delta, idx_t sub, idx_t subs, vector<uint32_t> &next,
	                      vector<Discovery> *log);
	//! False when the sides met in this row
	bool RelaxRow(Side &side, const Side &other, const LocalCSR &csr, uint32_t start_edges, uint32_t end_edges,
	              const uint64_t *suppressed_bits, uint32_t from, idx_t partition_idx, bool is_delta,
	              vector<uint32_t> &next, vector<Discovery> *log);
	//! Row id of a packed edge of `graph`; false when its partition carries no row ids
	static bool EdgeRowId(const SearchGraphSide &graph, uint64_t packed, row_t &row);
	//! Row id of an edge from -> to of `graph`; false when there is none with a row id
	static bool FindEdgeRowId(const SearchGraphSide &graph, uint32_t from, uint32_t to, row_t &row);

	Side sides[2];
	idx_t v_size;
	bool track_parents;
	idx_t max_depth;
	idx_t sparse_frontier_divisor;
	idx_t worker_count = 1;
	//! The current level sweeps the partitions; its tasks are (partition, row range), claimed through the cursor.
	//! A partition is cut into enough row ranges for SWEEP_TASKS_PER_WORKER tasks per worker: the frontier's rows
	//! are spread unevenly over the partitions, and whole partitions as tasks left most workers of a search that
	//! has every thread idle (SF300, 32 workers: an unreachable pair took 10.6 ms against 7.0 ms by lookups)
	static constexpr idx_t SWEEP_TASKS_PER_WORKER = 8;
	static constexpr idx_t SWEEP_MAX_SUB_RANGES = 256;
	bool sweep = false;
	idx_t sweep_sub_ranges = 1;
	atomic<idx_t> sweep_cursor {0};
	idx_t sweeps = 0;
	//! The side the current level expands (0 forward, 1 backward)
	idx_t expanding = 0;
	atomic<idx_t> frontier_cursor {0};
	//! The vertex both sides have seen, set once
	atomic<uint32_t> meet_vertex {NO_VERTEX};
	uint32_t source_vertex = NO_VERTEX;
	uint32_t target_vertex = NO_VERTEX;
	int64_t length = -1;
	idx_t discovered = 0;
};

} // namespace duckdb
