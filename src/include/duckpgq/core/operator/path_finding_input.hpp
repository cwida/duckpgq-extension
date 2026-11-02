#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

//! How the edges reach the path-finding operator; the optimizer rule reads it off the marker's arguments
enum class PathFindingInputRoute {
	//! marker(src, dst, csr_id): the second child builds a global CSR (create_csr_edge) and yields its id
	GLOBAL_CSR,
	//! marker(src, dst, {src, dst[, id]}, vertex_count, edge_count, key[, max_depth]): the second child streams the
	//! edge endpoints, radix-partitioned into the CSR partitions during the sink
	BUFFERED_ENDPOINTS,
	//! marker(src, dst, {src, dst}, {src, dst}, vertex_count, edge_count, key): the endpoints are scanned twice, once
	//! to count per source and once to fill the exactly sized partitions
	PRECOUNTED_ENDPOINTS,
	//! marker(src, dst, vertex_count, edge_count, 'csr-index|...'[, max_depth]): the pairs are the only child; the
	//! partitions come from the CSR index snapshot of the query's transaction
	CSR_INDEX
};

//! What the optimizer rule learnt from the marker: the route and the constants that go with it
struct PathFindingInput {
	PathFindingInputRoute route = PathFindingInputRoute::GLOBAL_CSR;
	//! Vertex and edge counts the endpoint routes declare (an index snapshot overrides them; unused on GLOBAL_CSR)
	idx_t vertex_count = 0;
	idx_t edge_count = 0;
	//! Longest path length the query needs; 0 means unbounded. BFS stops after this many iterations.
	idx_t max_depth = 0;
	//! The marker's key: names the index on CSR_INDEX, describes the graph (shown by EXPLAIN) on the other routes
	string cache_key;
	//! The endpoint input holds both directions of every edge (an undirected MATCH pattern), so the forward
	//! partitions serve a reverse search too
	bool undirected = false;
};

} // namespace duckdb
