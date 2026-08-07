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
void CorePGQOptions::RegisterPathFindingGroupedBatches(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	config.AddExtensionOption(
	    "experimental_path_finding_operator_grouped_batches",
	    "Run regular MS-BFS batches with bounded concurrent worker groups instead of one event per batch",
	    LogicalType::BOOLEAN, Value(false));
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

} // namespace duckdb
