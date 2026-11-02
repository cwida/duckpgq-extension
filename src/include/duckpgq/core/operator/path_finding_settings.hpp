#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/option/duckpgq_option.hpp"

namespace duckdb {

//! The settings the kernels read, looked up once per query (a random-pair query has one source group per pair and
//! one MS-BFS batch per 2048 pairs; 65536 groups would otherwise cost about 300,000 setting lookups)
struct PathFindingSettings {
	explicit PathFindingSettings(ClientContext &context)
	    : benchmark_enabled(GetPathFindingBenchmarkOption(context)),
	      benchmark_lane_activity_enabled(benchmark_enabled && GetPathFindingBenchmarkLaneActivityOption(context)),
	      benchmark_output_prefix(GetPathFindingBenchmarkPrefix(context)),
	      target_early_termination(GetPathFindingTargetEarlyTermination(context)),
	      threads_per_batch(GetPathFindingThreadsPerBatch(context)),
	      sparse_frontier_divisor(NonNegative(GetPathFindingSparseFrontierDivisor(context))),
	      frontier_bitmap(GetPathFindingFrontierBitmap(context)),
	      frontier_bitmap_filter(GetPathFindingFrontierBitmapFilter(context)),
	      grouped_batches_budget(GetPathFindingGroupedBatchesBudget(context)),
	      visit_prefetch(NonNegative(GetPathFindingVisitPrefetch(context))),
	      sub_range_tasks(GetPathFindingSubRangeTasks(context)),
	      sparse_level_divisor(NonNegative(GetPathFindingSparseLevelDivisor(context))) {
	}

	//! experimental_path_finding_operator_benchmark, _benchmark_lane_activity, _benchmark_prefix
	bool benchmark_enabled;
	bool benchmark_lane_activity_enabled;
	string benchmark_output_prefix;
	//! Source-grouped kernel: _target_early_termination, _threads_per_batch, _sparse_frontier_divisor (0 sweeps)
	bool target_early_termination;
	int64_t threads_per_batch;
	idx_t sparse_frontier_divisor;
	//! MS-BFS lane kernel: _frontier_bitmap, _frontier_bitmap_filter (-1 decides from the working set size),
	//! _grouped_batches_budget, _visit_prefetch (0 off), _sub_range_tasks, _sparse_level_divisor (0 sweeps)
	bool frontier_bitmap;
	int32_t frontier_bitmap_filter;
	int64_t grouped_batches_budget;
	idx_t visit_prefetch;
	bool sub_range_tasks;
	idx_t sparse_level_divisor;

private:
	static idx_t NonNegative(int32_t value) {
		return value > 0 ? NumericCast<idx_t>(value) : 0;
	}
};

} // namespace duckdb
