#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

class PropertyGraphTable;

bool GetDirectedPathFindingStorageCounts(ClientContext &context, const PropertyGraphTable &edge_table,
                                         idx_t &vertex_count, idx_t &edge_count);

string GetDirectedPathFindingEndpointsSQL(const PropertyGraphTable &edge_table);

idx_t GetBufferedPartitionedCSRWidth(idx_t vertex_count, idx_t thread_count, ClientContext &context);

idx_t GetBufferedPartitionedCSRLogicalPartitionCount(idx_t vertex_count, idx_t thread_count, ClientContext &context);

idx_t GetBufferedPartitionedCSRRadixBits(idx_t vertex_count, idx_t thread_count, ClientContext &context);

} // namespace duckdb
