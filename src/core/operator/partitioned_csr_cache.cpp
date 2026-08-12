#include "duckpgq/core/operator/partitioned_csr_cache.hpp"

#include "duckdb/parallel/task_scheduler.hpp"
#include "duckpgq/core/option/duckpgq_option.hpp"

#include <iomanip>
#include <limits>
#include <sstream>

namespace duckdb {

idx_t GetBufferedPartitionedCSRWidth(idx_t vertex_count, idx_t thread_count, ClientContext &context) {
	auto target_partition_count =
	    std::max<idx_t>(1, thread_count) * (1 + std::max<int32_t>(1, GetLightPartitionMultiplier(context)));
	auto partition_width = std::max<idx_t>(1, (vertex_count + 2 + target_partition_count - 1) /
	                                               target_partition_count);
	return std::min<idx_t>(partition_width, UINT16_MAX);
}

string GetBufferedPartitionedCSRCacheKey(ClientContext &context, const string &base_cache_key,
                                         idx_t vertex_count, idx_t edge_count, const string &mode) {
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
	key << "partitioned-csr-v2|graph=" << base_cache_key.size() << ":" << base_cache_key;
	key << "|vertices=" << vertex_count + 2 << "|edges=" << edge_count;
	key << "|input=precounted-endpoints";
	key << "|partition_width=" << partition_width;
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
