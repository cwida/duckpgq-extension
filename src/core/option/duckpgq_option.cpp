#include "duckpgq/core/option/duckpgq_option.hpp"
#include "duckpgq/common.hpp"

namespace duckdb {

bool GetPathFindingOption(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator", value);
	return value.GetValue<bool>();
}

int32_t GetPathFindingTaskSize(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_task_size", value);
	return value.GetValue<int32_t>();
}

int32_t GetLightPartitionMultiplier(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_light_partition_multiplier", value);
	return value.GetValue<int32_t>();
}

double_t GetCSRIndexFoldRatio(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("duckpgq_csr_index_fold_ratio", value);
	return value.GetValue<double_t>();
}

double_t GetHeavyPartitionFraction(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_heavy_partition_fraction", value);
	return value.GetValue<double_t>();
}

bool GetPathFindingBenchmarkOption(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_benchmark", value);
	return value.GetValue<bool>();
}

bool GetPathFindingBenchmarkLaneActivityOption(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_benchmark_lane_activity", value);
	return value.GetValue<bool>();
}

string GetPathFindingBenchmarkPrefix(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_benchmark_prefix", value);
	return value.GetValue<string>();
}

bool GetPathFindingBuildReverseCSR(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_build_reverse_csr", value);
	return value.GetValue<bool>();
}

int32_t GetPathFindingPushPullFrontierGate(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_push_pull_frontier_gate", value);
	return value.GetValue<int32_t>();
}

bool GetPathFindingDeduplicatePairs(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_deduplicate_pairs", value);
	return value.GetValue<bool>();
}

bool GetPathFindingGroupedBatches(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_grouped_batches", value);
	return value.GetValue<bool>();
}

bool GetPathFindingReachOption(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_reach", value);
	return value.GetValue<bool>();
}

bool GetPathFindingReachFilteredOption(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_reach_filtered", value);
	return value.GetValue<bool>();
}

int32_t GetPathFindingThreadsPerBatch(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_threads_per_batch", value);
	return value.GetValue<int32_t>();
}

int32_t GetPathFindingMaxConcurrentBatches(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_max_concurrent_batches", value);
	return value.GetValue<int32_t>();
}

int32_t GetPathFindingReverseOrientationRatio(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_reverse_orientation_ratio", value);
	return value.GetValue<int32_t>();
}

int32_t GetPathFindingSourceGroupRatio(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_source_group_ratio", value);
	return value.GetValue<int32_t>();
}

bool GetPathFindingSnapshotCache(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_snapshot_cache", value);
	return value.GetValue<bool>();
}

bool GetPathFindingTargetEarlyTermination(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_target_early_termination", value);
	return value.GetValue<bool>();
}

int32_t GetPathFindingSparseFrontierDivisor(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_sparse_frontier_divisor", value);
	return value.GetValue<int32_t>();
}

int32_t GetPathFindingFewSourcesLimit(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_few_sources_limit", value);
	return value.GetValue<int32_t>();
}

int64_t GetPathFindingGroupedBatchesBudget(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_grouped_batches_budget", value);
	return value.GetValue<int64_t>();
}

int32_t GetPathFindingBatchOversubscription(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_batch_oversubscription", value);
	return value.GetValue<int32_t>();
}

bool GetPathFindingFrontierBitmap(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_frontier_bitmap", value);
	return value.GetValue<bool>();
}

int32_t GetPathFindingVisitPrefetch(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_visit_prefetch", value);
	return value.GetValue<int32_t>();
}

int32_t GetPathFindingFrontierBitmapFilter(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_frontier_bitmap_filter", value);
	return value.GetValue<int32_t>();
}

int32_t GetPathFindingSparseLevelDivisor(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_sparse_level_divisor", value);
	return value.GetValue<int32_t>();
}

int32_t GetPathFindingPartitionVertices(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_partition_vertices", value);
	return value.GetValue<int32_t>();
}

bool GetPathFindingSubRangeTasks(ClientContext &context) {
	Value value;
	context.TryGetCurrentSetting("experimental_path_finding_operator_sub_range_tasks", value);
	return value.GetValue<bool>();
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterExperimentalPathFindingOperator(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator",
	                          "Enables the experimental path finding operator to be triggered", LogicalType::BOOLEAN,
	                          Value(false));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingTaskSize(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("experimental_path_finding_operator_task_size",
	                          "Number of vertices processed per thread at a time", LogicalType::INTEGER, Value(256));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingLightPartitionMultiplier(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("experimental_path_finding_operator_light_partition_multiplier",
	                          "Multiplier used for the light partitions of the local CSR partitioning",
	                          LogicalType::INTEGER, Value(1));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingHeavyPartitionFraction(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("experimental_path_finding_operator_heavy_partition_fraction",
	                          "Fraction of edges part of the heavy partitions for the local CSR partitioning",
	                          LogicalType::DOUBLE, Value(0.75));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingBenchmark(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("experimental_path_finding_operator_benchmark",
	                          "Write experimental path-finding operator benchmark CSV files", LogicalType::BOOLEAN,
	                          Value(false));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingBenchmarkLaneActivity(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption(
	    "experimental_path_finding_operator_benchmark_lane_activity",
	    "Include per-iteration lane activity CSV output for the experimental path-finding operator",
	    LogicalType::BOOLEAN, Value(false));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingBenchmarkPrefix(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("experimental_path_finding_operator_benchmark_prefix",
	                          "Prefix for experimental path-finding operator benchmark CSV files", LogicalType::VARCHAR,
	                          Value("path_finding_operator"));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingBuildReverseCSR(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("experimental_path_finding_operator_build_reverse_csr",
	                          "Build reverse local CSR partitions for path-finding operator experiments",
	                          LogicalType::BOOLEAN, Value(false));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingPushPullFrontierGate(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption(
	    "experimental_path_finding_operator_push_pull_frontier_gate",
	    "Use pull in push/pull MS-BFS when frontier_vertices * gate is at least the vertex count",
	    LogicalType::INTEGER, Value(2));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingDeduplicatePairs(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption(
	    "experimental_path_finding_operator_deduplicate_pairs",
	    "Deduplicate exact source/destination pairs inside each experimental path-finding operator batch",
	    LogicalType::BOOLEAN, Value(false));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingReach(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("experimental_path_finding_operator_reach",
	                          "Plan a path pattern whose destination is unbound or only filtered as a reach query: "
	                          "the operator emits every vertex each source reaches instead of matching a pair list",
	                          LogicalType::BOOLEAN, Value(true));
}

void CorePGQOptions::RegisterPathFindingReachFiltered(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("experimental_path_finding_operator_reach_filtered",
	                          "Also plan a destination with a non-key predicate as a reach query (the predicate is "
	                          "applied after the reach); off, because a selective predicate lets the pair mode stop "
	                          "each search early (LDBC BI19 at SF100: 5.9 s pair mode vs 7.1 s reach)",
	                          LogicalType::BOOLEAN, Value(false));
}

void CorePGQOptions::RegisterPathFindingGroupedBatches(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption(
	    "experimental_path_finding_operator_grouped_batches",
	    "Run regular MS-BFS batches with bounded concurrent worker groups instead of one event per batch",
	    LogicalType::BOOLEAN, Value(true));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingThreadsPerBatch(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption(
	    "experimental_path_finding_operator_threads_per_batch",
	    "Maximum number of DuckDB tasks assigned to each grouped regular MS-BFS batch; values <= 0 use all threads",
	    LogicalType::INTEGER, Value(0));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingMaxConcurrentBatches(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption(
	    "experimental_path_finding_operator_max_concurrent_batches",
	    "Maximum number of grouped regular MS-BFS batches admitted concurrently; values <= 0 derive from thread budget",
	    LogicalType::INTEGER, Value(0));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingReverseOrientationRatio(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption(
	    "experimental_path_finding_operator_reverse_orientation_ratio",
	    "Use reverse MS-BFS for regular path-finding when estimated distinct_src >= ratio * estimated distinct_dst; "
	    "values <= 0 disable adaptive reverse orientation",
	    LogicalType::INTEGER, Value(4));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingSourceGroupRatio(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption(
	    "experimental_path_finding_operator_source_group_ratio",
	    "Use source-grouped regular path-finding when pair_count >= ratio * oriented_distinct_source_count; "
	    "values <= 0 disable adaptive source grouping",
	    LogicalType::INTEGER, Value(4));
}

//------------------------------------------------------------------------------
// Register option
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterCSRIndexFoldRatio(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption("duckpgq_csr_index_fold_ratio",
	                          "A CSR index folds its change log into a new epoch when the log exceeds this fraction of "
	                          "the epoch's edges; 0 disables automatic folding",
	                          LogicalType::DOUBLE, Value(0.1));
}

//------------------------------------------------------------------------------
// Microbenchmark switches (default = the behaviour the paper measures; each one turns a mechanism off or moves a
// hard-coded threshold so a microbenchmark can isolate it)
//------------------------------------------------------------------------------
void CorePGQOptions::RegisterPathFindingSnapshotCache(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_snapshot_cache",
	                          "Reuse a CSR index snapshot across queries while the epoch, its change log and the "
	                          "visible change groups are unchanged; false rebuilds the overlay for every query",
	                          LogicalType::BOOLEAN, Value(true));
}

void CorePGQOptions::RegisterPathFindingTargetEarlyTermination(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_target_early_termination",
	                          "Stop a source-grouped search once every destination of the group is reached; false "
	                          "searches to the depth bound or exhaustion",
	                          LogicalType::BOOLEAN, Value(true));
}

void CorePGQOptions::RegisterPathFindingSparseFrontierDivisor(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_sparse_frontier_divisor",
	                          "A source-grouped frontier is expanded vertex by vertex when frontier_size * divisor < "
	                          "vertex_count, and by a sweep over the partitions otherwise; 0 always sweeps",
	                          LogicalType::INTEGER, Value(64));
}

void CorePGQOptions::RegisterPathFindingFewSourcesLimit(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_few_sources_limit",
	                          "Pair queries with at most this many distinct sources always use the per-source frontier "
	                          "kernel instead of MS-BFS lanes; 0 leaves the choice to the source group ratio",
	                          LogicalType::INTEGER, Value(16));
}

void CorePGQOptions::RegisterPathFindingGroupedBatchesBudget(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_grouped_batches_budget",
	                          "The MS-BFS batch scheduler starts wide (one worker group per batch, up to the threads, "
	                          "each with its own threads) while that many working sets (192 B per vertex each) fit "
	                          "this many bytes; above it the groups pool their level tasks and the window of "
	                          "concurrent groups starts at four and grows while the measured throughput improves "
	                          "(W31: 512 MB sits between SF10's 32 x 14 MB and SF100's 32 x 86 MB on a 128 MB L3 "
	                          "machine); 0 always ramps",
	                          LogicalType::BIGINT, Value::BIGINT(512LL * 1024 * 1024));
}

void CorePGQOptions::RegisterPathFindingBatchOversubscription(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_batch_oversubscription",
	                          "Tasks per level of one MS-BFS batch relative to its share of the threads (threads / "
	                          "concurrent groups): 1 gives every group its own threads, higher values pool the "
	                          "level tasks of all groups in the executor so idle time at a group's level barrier is "
	                          "filled with another group's work; 0 (default) chooses 1 while the working sets fit "
	                          "the grouped batches budget and the thread count above it",
	                          LogicalType::INTEGER, Value(0));
}

void CorePGQOptions::RegisterPathFindingFrontierBitmap(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_frontier_bitmap",
	                          "MS-BFS keeps a one-bit-per-vertex summary of the frontier; a level with few active "
	                          "vertices (count x sparse_frontier_divisor < V) expands them by row lookup from that "
	                          "list instead of sweeping every partition row; false sweeps every level",
	                          LogicalType::BOOLEAN, Value(true));
}

void CorePGQOptions::RegisterPathFindingVisitPrefetch(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_visit_prefetch",
	                          "MS-BFS explore prefetches the visit line of the source this many rows ahead in the "
	                          "partition's source list; 0 (default) disables",
	                          LogicalType::INTEGER, Value(0));
}

void CorePGQOptions::RegisterPathFindingFrontierBitmapFilter(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_frontier_bitmap_filter",
	                          "On dense MS-BFS levels test the frontier bitmap per source before reading its visit "
	                          "line: 1 on, 0 off, -1 (default) on when the working sets exceed the grouped batches "
	                          "budget (SF100: 1.2-1.6x faster) and off when they are cache resident (SF10: 10% "
	                          "overhead)",
	                          LogicalType::INTEGER, Value(-1));
}

void CorePGQOptions::RegisterPathFindingSparseLevelDivisor(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_sparse_level_divisor",
	                          "An MS-BFS level whose active vertices x divisor < V expands the active vertices by row "
	                          "lookup in each partition instead of sweeping the partition's rows (the first two and "
	                          "the tail levels of a wave); 0 sweeps every level. 16 measured best (27 Oct)",
	                          LogicalType::INTEGER, Value(16));
}

void CorePGQOptions::RegisterPathFindingPartitionVertices(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_partition_vertices",
	                          "Vertices per CSR partition (destination range) for the per-query build and for CREATE "
	                          "INDEX ... USING CSR; 65535 (default, the widest a uint16 destination allows) gives the "
	                          "fewest partitions: every frontier vertex costs one row lookup per partition and every "
	                          "source's visit line is read once per partition, so coarse partitions are 1.5x faster "
	                          "on many-batch pair lists and 2x on sources x all (W32, 27 Oct); a single batch keeps "
	                          "its parallelism through destination sub-range tasks and pays 1.3-1.5x. <= 0 derives "
	                          "the count from the thread count (threads x (1 + light_partition_multiplier))",
	                          LogicalType::INTEGER, Value(65535));
}

void CorePGQOptions::RegisterPathFindingSubRangeTasks(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("experimental_path_finding_operator_sub_range_tasks",
	                          "When an MS-BFS batch has more workers than partitions, split each partition's "
	                          "destination range into sub-range tasks so the batch keeps its parallelism on coarse "
	                          "partitions (W32); false gives one task per partition",
	                          LogicalType::BOOLEAN, Value(true));
}

} // namespace duckdb
