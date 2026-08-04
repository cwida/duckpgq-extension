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

} // namespace duckdb
