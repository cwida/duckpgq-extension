#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

class PropertyGraphTable;

bool GetDirectedPathFindingStorageCounts(ClientContext &context, const PropertyGraphTable &edge_table,
                                         idx_t &vertex_count, idx_t &edge_count);

//! Row ids of the endpoints of every edge as pathfinding_edge_src, pathfinding_edge_dst and, with_edge_ids, the
//! edge row id as pathfinding_edge_id (paths report edges by row id)
string GetDirectedPathFindingEndpointsSQL(const PropertyGraphTable &edge_table, bool with_edge_ids = false);

idx_t GetBufferedPartitionedCSRWidth(idx_t vertex_count, idx_t thread_count, ClientContext &context);

idx_t GetBufferedPartitionedCSRLogicalPartitionCount(idx_t vertex_count, idx_t thread_count, ClientContext &context);

idx_t GetBufferedPartitionedCSRRadixBits(idx_t vertex_count, idx_t thread_count, ClientContext &context);

} // namespace duckdb
