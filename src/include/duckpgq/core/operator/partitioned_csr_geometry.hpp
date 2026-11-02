//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/operator/partitioned_csr_geometry.hpp
//
// Geometry of the partitioned CSR the path-finding operator builds on the fly and the CSR index freezes at CREATE
// INDEX: the vertex and edge counts of an edge table, the endpoint-join SQL, and the partition width / count /
// radix bits for a vertex count and thread count.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

class PropertyGraphTable;

//! The CSR range (next row id of the vertex table) and the transaction-visible edge count; false when the edge
//! table's endpoints are not one DuckDB table
bool GetDirectedPathFindingStorageCounts(ClientContext &context, const PropertyGraphTable &edge_table,
                                         idx_t &vertex_count, idx_t &edge_count);

//! Row ids of the endpoints of every edge as pathfinding_edge_src, pathfinding_edge_dst and, with_edge_ids, the
//! edge row id as pathfinding_edge_id (paths report edges by row id)
string GetDirectedPathFindingEndpointsSQL(const PropertyGraphTable &edge_table, bool with_edge_ids = false);

idx_t GetBufferedPartitionedCSRWidth(idx_t vertex_count, idx_t thread_count, ClientContext &context);

idx_t GetBufferedPartitionedCSRLogicalPartitionCount(idx_t vertex_count, idx_t thread_count, ClientContext &context);

idx_t GetBufferedPartitionedCSRRadixBits(idx_t vertex_count, idx_t thread_count, ClientContext &context);

} // namespace duckdb
