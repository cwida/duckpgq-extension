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

//! The logical identity deliberately excludes thread count, partition geometry, and
//! tuning settings. Those are physical properties of an immutable completed index.
string GetBufferedPartitionedCSRLogicalKey(const string &base_cache_key, idx_t vertex_count, idx_t edge_count);

} // namespace duckdb
