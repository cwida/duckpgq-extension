#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

idx_t GetBufferedPartitionedCSRWidth(idx_t vertex_count, idx_t thread_count, ClientContext &context);

idx_t GetBufferedPartitionedCSRRadixBits(idx_t vertex_count, idx_t thread_count, ClientContext &context);

string GetBufferedPartitionedCSRCacheKey(ClientContext &context, const string &base_cache_key, idx_t vertex_count,
                                         idx_t edge_count, const string &mode, bool radix_partitioned_input = true);

} // namespace duckdb
