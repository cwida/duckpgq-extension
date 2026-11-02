#include "duckpgq/core/option/duckpgq_option.hpp"
#include "duckpgq/common.hpp"

namespace duckdb {

namespace {

struct DuckPGQSetting {
	const char *name;
	const char *description;
	LogicalType type;
	Value default_value;
};

//! Every setting of the extension: name, description, type and default. The defaults are the behaviour the paper
//! measures; the microbenchmark switches (snapshot cache onwards) each turn one mechanism off or move a threshold.
const vector<DuckPGQSetting> &Settings() {
	static const vector<DuckPGQSetting> settings = {
	    {"experimental_path_finding_operator", "Enables the experimental path finding operator to be triggered",
	     LogicalType::BOOLEAN, Value(false)},
	    {"experimental_path_finding_operator_light_partition_multiplier",
	     "Multiplier used for the light partitions of the local CSR partitioning", LogicalType::INTEGER, Value(1)},
	    {"experimental_path_finding_operator_heavy_partition_fraction",
	     "Fraction of edges part of the heavy partitions for the local CSR partitioning", LogicalType::DOUBLE,
	     Value(0.75)},
	    {"experimental_path_finding_operator_benchmark",
	     "Write experimental path-finding operator benchmark CSV files", LogicalType::BOOLEAN, Value(false)},
	    {"experimental_path_finding_operator_benchmark_lane_activity",
	     "Include per-iteration lane activity CSV output for the experimental path-finding operator",
	     LogicalType::BOOLEAN, Value(false)},
	    {"experimental_path_finding_operator_benchmark_prefix",
	     "Prefix for experimental path-finding operator benchmark CSV files", LogicalType::VARCHAR,
	     Value("path_finding_operator")},
	    {"experimental_path_finding_operator_build_reverse_csr",
	     "Build reverse local CSR partitions for path-finding operator experiments", LogicalType::BOOLEAN,
	     Value(false)},
	    {"experimental_path_finding_operator_push_pull_frontier_gate",
	     "Use pull in push/pull MS-BFS when frontier_vertices * gate is at least the vertex count",
	     LogicalType::INTEGER, Value(2)},
	    {"experimental_path_finding_operator_deduplicate_pairs",
	     "Deduplicate exact source/destination pairs inside each experimental path-finding operator batch",
	     LogicalType::BOOLEAN, Value(false)},
	    {"experimental_path_finding_operator_grouped_batches",
	     "Run regular MS-BFS batches with bounded concurrent worker groups instead of one event per batch",
	     LogicalType::BOOLEAN, Value(true)},
	    {"experimental_path_finding_operator_reach",
	     "Plan a path pattern whose destination is unbound or only filtered as a reach query: "
	     "the operator emits every vertex each source reaches instead of matching a pair list",
	     LogicalType::BOOLEAN, Value(true)},
	    {"experimental_path_finding_operator_reach_filtered",
	     "Also plan a destination with a non-key predicate as a reach query (the predicate is "
	     "applied after the reach); off, because a selective predicate lets the pair mode stop "
	     "each search early (LDBC BI19 at SF100: 5.9 s pair mode vs 7.1 s reach)",
	     LogicalType::BOOLEAN, Value(false)},
	    {"experimental_path_finding_operator_threads_per_batch",
	     "Maximum number of DuckDB tasks assigned to each grouped regular MS-BFS batch; values <= 0 use all threads",
	     LogicalType::INTEGER, Value(0)},
	    {"experimental_path_finding_operator_max_concurrent_batches",
	     "Maximum number of grouped regular MS-BFS batches admitted concurrently; values <= 0 derive from thread budget",
	     LogicalType::INTEGER, Value(0)},
	    {"experimental_path_finding_operator_reverse_orientation_ratio",
	     "Use reverse MS-BFS for regular path-finding when estimated distinct_src >= ratio * estimated distinct_dst; "
	     "values <= 0 disable adaptive reverse orientation",
	     LogicalType::INTEGER, Value(4)},
	    {"experimental_path_finding_operator_source_group_ratio",
	     "Group length queries by source (a few sources run the per-source frontier kernel, more run as MS-BFS waves of "
	     "one lane per source); values <= 0 switch grouping off and run the former MS-BFS batches of one lane per pair",
	     LogicalType::INTEGER, Value(4)},
	    {"duckpgq_csr_index_fold_ratio",
	     "A CSR index folds its change log into a new epoch when the log exceeds this fraction of "
	     "the epoch's edges; 0 disables automatic folding",
	     LogicalType::DOUBLE, Value(0.1)},
	    {"experimental_path_finding_operator_snapshot_cache",
	     "Reuse a CSR index snapshot across queries while the epoch, its change log and the "
	     "visible change groups are unchanged; false rebuilds the overlay for every query",
	     LogicalType::BOOLEAN, Value(true)},
	    {"experimental_path_finding_operator_target_early_termination",
	     "Stop a source-grouped search once every destination of the group is reached; false "
	     "searches to the depth bound or exhaustion",
	     LogicalType::BOOLEAN, Value(true)},
	    {"experimental_path_finding_operator_sparse_frontier_divisor",
	     "A source-grouped frontier is expanded vertex by vertex when frontier_size * divisor < "
	     "vertex_count, and by a sweep over the partitions otherwise; 0 always sweeps",
	     LogicalType::INTEGER, Value(64)},
	    {"experimental_path_finding_operator_few_sources_limit",
	     "Pair queries with at most this many distinct sources always use the per-source frontier "
	     "kernel instead of MS-BFS lanes; 0 leaves the choice to the source group ratio",
	     LogicalType::INTEGER, Value(16)},
	    {"experimental_path_finding_operator_grouped_batches_budget",
	     "The MS-BFS batch scheduler starts wide (one worker group per batch, up to the threads, "
	     "each with its own threads) while that many working sets (192 B per vertex each) fit "
	     "this many bytes; above it the groups pool their level tasks and the window of "
	     "concurrent groups starts at four and grows while the measured throughput improves "
	     "(W31: 512 MB sits between SF10's 32 x 14 MB and SF100's 32 x 86 MB on a 128 MB L3 "
	     "machine); 0 always ramps",
	     LogicalType::BIGINT, Value::BIGINT(512LL * 1024 * 1024)},
	    {"experimental_path_finding_operator_batch_oversubscription",
	     "Tasks per level of one MS-BFS batch relative to its share of the threads (threads / "
	     "concurrent groups): 1 gives every group its own threads, higher values pool the "
	     "level tasks of all groups in the executor so idle time at a group's level barrier is "
	     "filled with another group's work; 0 (default) chooses 1 while the working sets fit "
	     "the grouped batches budget and the thread count above it",
	     LogicalType::INTEGER, Value(0)},
	    {"experimental_path_finding_operator_frontier_bitmap",
	     "MS-BFS keeps a one-bit-per-vertex summary of the frontier; a level with few active "
	     "vertices (count x sparse_frontier_divisor < V) expands them by row lookup from that "
	     "list instead of sweeping every partition row; false sweeps every level",
	     LogicalType::BOOLEAN, Value(true)},
	    {"experimental_path_finding_operator_frontier_bitmap_filter",
	     "On dense MS-BFS levels test the frontier bitmap per source before reading its visit "
	     "line: 1 on, 0 off, -1 (default) on when the working sets exceed the grouped batches "
	     "budget (SF100: 1.2-1.6x faster) and off when they are cache resident (SF10: 10% "
	     "overhead)",
	     LogicalType::INTEGER, Value(-1)},
	    {"experimental_path_finding_operator_sparse_level_divisor",
	     "An MS-BFS level whose active vertices x divisor < V expands the active vertices by row "
	     "lookup in each partition instead of sweeping the partition's rows (the first two and "
	     "the tail levels of a wave); 0 sweeps every level. 16 measured best (27 Oct)",
	     LogicalType::INTEGER, Value(16)},
	    {"experimental_path_finding_operator_partition_vertices",
	     "Vertices per CSR partition (destination range) for the per-query build and for CREATE "
	     "INDEX ... USING CSR; 65535 (default, the widest a uint16 destination allows) gives the "
	     "fewest partitions: every frontier vertex costs one row lookup per partition and every "
	     "source's visit line is read once per partition, so coarse partitions are 1.5x faster "
	     "on many-batch pair lists and 2x on sources x all (W32, 27 Oct); a single batch keeps "
	     "its parallelism through destination sub-range tasks and pays 1.3-1.5x. <= 0 derives "
	     "the count from the thread count (threads x (1 + light_partition_multiplier))",
	     LogicalType::INTEGER, Value(65535)},
	    {"experimental_path_finding_operator_row_bucket_rows",
	     "Target sparse rows per bucket of a partition's row index (a bucket covers 2^k source ids, "
	     "k chosen so a bucket holds about this many rows; a lookup reads one bucket entry and scans "
	     "its rows). 2 since 20 Oct, a default rather than a tuned value; 0 builds no index and "
	     "looks rows up by binary search. Read when a CSR is built (per-query build, CREATE INDEX, "
	     "fold); a persisted index loads with the global value (microbenchmark row_buckets)",
	     LogicalType::INTEGER, Value(2)},
	    {"experimental_path_finding_operator_sub_range_tasks",
	     "When an MS-BFS batch has more workers than partitions, split each partition's "
	     "destination range into sub-range tasks so the batch keeps its parallelism on coarse "
	     "partitions (W32); false gives one task per partition",
	     LogicalType::BOOLEAN, Value(true)},
	    {"duckpgq_csr_index_change_groups",
	     "A CSR index snapshot decides the visibility of a change group (one commit chunk of one "
	     "transaction) with one row-version check and applies it to every row of the group; false "
	     "checks every logged row individually (the microbenchmark of the per-row cost)",
	     LogicalType::BOOLEAN, Value(true)},
	    {"experimental_path_finding_operator_lane_waves",
	     "Source groups beyond the few-sources limit run as MS-BFS waves of one lane per source "
	     "with a destination set per lane (Then et al.) instead of one single-source search per "
	     "group; false keeps the per-source searches",
	     LogicalType::BOOLEAN, Value(true)},
	    {"experimental_path_finding_operator_reach_lanes",
	     "Reach queries (sources x all vertices) with more distinct sources than the few-sources "
	     "limit run as MS-BFS waves of one lane per source, each lane recording every vertex it "
	     "reaches in the check phase; false runs one single-source search per source",
	     LogicalType::BOOLEAN, Value(true)},
	    {"experimental_path_finding_operator_source_search_rounds",
	     "Per-source searches (paths, reach, few sources) run in rounds whose per-vertex arrays "
	     "fit the grouped-batches budget, at most one per thread, each search allocating its "
	     "arrays when it starts and freeing them when it ends; false starts every search at "
	     "once (the former behaviour, for the microbenchmark)",
	     LogicalType::BOOLEAN, Value(true)},
	    {"experimental_path_finding_operator_visit_prefetch",
	     "MS-BFS explore prefetches the visit line of the source this many rows ahead in the "
	     "partition's source list; 0 (default) disables",
	     LogicalType::INTEGER, Value(0)},
	};
	return settings;
}

//! The current value of a registered setting
template <class T>
T GetSetting(ClientContext &context, const char *name) {
	Value value;
	if (!context.TryGetCurrentSetting(name, value)) {
		throw InternalException("DuckPGQ setting %s is not registered", name);
	}
	return value.GetValue<T>();
}

} // namespace

void CorePGQOptions::Register(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	for (auto &setting : Settings()) {
		config.AddExtensionOption(setting.name, setting.description, setting.type, setting.default_value);
	}
}

bool GetPathFindingOption(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator");
}

int32_t GetLightPartitionMultiplier(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_light_partition_multiplier");
}

double_t GetHeavyPartitionFraction(ClientContext &context) {
	return GetSetting<double_t>(context, "experimental_path_finding_operator_heavy_partition_fraction");
}

bool GetPathFindingBenchmarkOption(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_benchmark");
}

bool GetPathFindingBenchmarkLaneActivityOption(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_benchmark_lane_activity");
}

string GetPathFindingBenchmarkPrefix(ClientContext &context) {
	return GetSetting<string>(context, "experimental_path_finding_operator_benchmark_prefix");
}

bool GetPathFindingBuildReverseCSR(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_build_reverse_csr");
}

int32_t GetPathFindingPushPullFrontierGate(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_push_pull_frontier_gate");
}

bool GetPathFindingDeduplicatePairs(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_deduplicate_pairs");
}

bool GetPathFindingGroupedBatches(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_grouped_batches");
}

bool GetPathFindingReachOption(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_reach");
}

bool GetPathFindingReachFilteredOption(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_reach_filtered");
}

int32_t GetPathFindingThreadsPerBatch(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_threads_per_batch");
}

int32_t GetPathFindingMaxConcurrentBatches(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_max_concurrent_batches");
}

int32_t GetPathFindingReverseOrientationRatio(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_reverse_orientation_ratio");
}

int32_t GetPathFindingSourceGroupRatio(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_source_group_ratio");
}

double_t GetCSRIndexFoldRatio(ClientContext &context) {
	return GetSetting<double_t>(context, "duckpgq_csr_index_fold_ratio");
}

bool GetPathFindingSnapshotCache(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_snapshot_cache");
}

bool GetPathFindingTargetEarlyTermination(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_target_early_termination");
}

int32_t GetPathFindingSparseFrontierDivisor(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_sparse_frontier_divisor");
}

int32_t GetPathFindingFewSourcesLimit(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_few_sources_limit");
}

int64_t GetPathFindingGroupedBatchesBudget(ClientContext &context) {
	return GetSetting<int64_t>(context, "experimental_path_finding_operator_grouped_batches_budget");
}

int32_t GetPathFindingBatchOversubscription(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_batch_oversubscription");
}

bool GetPathFindingFrontierBitmap(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_frontier_bitmap");
}

int32_t GetPathFindingFrontierBitmapFilter(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_frontier_bitmap_filter");
}

int32_t GetPathFindingSparseLevelDivisor(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_sparse_level_divisor");
}

int32_t GetPathFindingPartitionVertices(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_partition_vertices");
}

int32_t GetPathFindingRowBucketRows(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_row_bucket_rows");
}

bool GetPathFindingSubRangeTasks(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_sub_range_tasks");
}

bool GetCSRIndexChangeGroups(ClientContext &context) {
	return GetSetting<bool>(context, "duckpgq_csr_index_change_groups");
}

bool GetPathFindingLaneWaves(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_lane_waves");
}

bool GetPathFindingReachLanes(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_reach_lanes");
}

bool GetPathFindingSourceSearchRounds(ClientContext &context) {
	return GetSetting<bool>(context, "experimental_path_finding_operator_source_search_rounds");
}

int32_t GetPathFindingVisitPrefetch(ClientContext &context) {
	return GetSetting<int32_t>(context, "experimental_path_finding_operator_visit_prefetch");
}

} // namespace duckdb
