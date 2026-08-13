#include "duckpgq/core/operator/partitioned_csr_cache.hpp"

#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckpgq/core/option/duckpgq_option.hpp"

#include <iomanip>
#include <limits>
#include <sstream>

namespace duckdb {

static idx_t GetBufferedPartitionedCSRRequiredPartitionCount(idx_t vertex_count, idx_t thread_count,
                                                             ClientContext &context) {
	auto target_partition_count =
	    std::max<idx_t>(1, thread_count) * (1 + std::max<int32_t>(1, GetLightPartitionMultiplier(context)));
	auto minimum_partition_count = std::max<idx_t>(1, (vertex_count + 2 + UINT16_MAX - 1) / UINT16_MAX);
	auto maximum_partition_count = RadixPartitioning::NumberOfPartitions(RadixPartitioning::MAX_RADIX_BITS);
	if (minimum_partition_count > maximum_partition_count) {
		throw OutOfRangeException(
		    "Radix-partitioned endpoint construction supports at most %llu vertices with uint16 destinations",
		    maximum_partition_count * UINT16_MAX);
	}
	return std::min(std::max(target_partition_count, minimum_partition_count), maximum_partition_count);
}

idx_t GetBufferedPartitionedCSRRadixBits(idx_t vertex_count, idx_t thread_count, ClientContext &context) {
	auto required_partition_count =
	    GetBufferedPartitionedCSRRequiredPartitionCount(vertex_count, thread_count, context);
	idx_t radix_bits = 0;
	idx_t partition_count = 1;
	while (partition_count < required_partition_count && radix_bits < RadixPartitioning::MAX_RADIX_BITS) {
		partition_count <<= 1;
		radix_bits++;
	}
	return radix_bits;
}

idx_t GetBufferedPartitionedCSRWidth(idx_t vertex_count, idx_t thread_count, ClientContext &context) {
	auto partition_count = GetBufferedPartitionedCSRRequiredPartitionCount(vertex_count, thread_count, context);
	return std::max<idx_t>(1, (vertex_count + 2 + partition_count - 1) / partition_count);
}

idx_t GetBufferedPartitionedCSRLogicalPartitionCount(idx_t vertex_count, idx_t thread_count, ClientContext &context) {
	auto partition_width = GetBufferedPartitionedCSRWidth(vertex_count, thread_count, context);
	return std::max<idx_t>(1, (vertex_count + 2 + partition_width - 1) / partition_width);
}

string GetBufferedPartitionedCSRCacheKey(ClientContext &context, const string &base_cache_key, idx_t vertex_count,
                                         idx_t edge_count, const string &mode, bool radix_partitioned_input) {
	if (base_cache_key.empty()) {
		return string();
	}

	bool build_forward = true;
	bool build_reverse = false;
	bool build_pull = false;
	bool finalize_sparse_rows = true;
	if (mode == "bidirectionaliterativelength") {
		build_reverse = true;
		finalize_sparse_rows = false;
	} else if (mode == "pushpulliterativelength") {
		build_pull = true;
	} else if (mode != "iterativelength" && mode != "shortestpath") {
		throw InvalidInputException("Unknown path-finding mode %s", mode);
	}

	auto thread_count = std::max<idx_t>(1, TaskScheduler::GetScheduler(context).NumberOfThreads());
	auto partition_width = GetBufferedPartitionedCSRWidth(vertex_count, thread_count, context);
	std::ostringstream key;
	key << std::setprecision(std::numeric_limits<double>::max_digits10);
	key << "partitioned-csr-v4|graph=" << base_cache_key.size() << ":" << base_cache_key;
	key << "|vertices=" << vertex_count + 2 << "|edges=" << edge_count;
	key << "|input=" << (radix_partitioned_input ? "radix-partitioned-endpoints" : "precounted-endpoints");
	key << "|partition_width=" << partition_width;
	key << "|partition_count="
	    << GetBufferedPartitionedCSRLogicalPartitionCount(vertex_count, thread_count, context);
	key << "|threads=" << thread_count;
	key << "|forward=" << build_forward;
	key << "|reverse=" << build_reverse;
	key << "|pull=" << build_pull;
	key << "|sparse=" << finalize_sparse_rows;
	key << "|heavy_fraction=" << GetHeavyPartitionFraction(context);
	key << "|light_multiplier=" << GetLightPartitionMultiplier(context);
	return key.str();
}

} // namespace duckdb
