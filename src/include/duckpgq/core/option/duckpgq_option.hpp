#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

bool GetPathFindingOption(ClientContext &context);
int32_t GetPathFindingTaskSize(ClientContext &context);
int32_t GetLightPartitionMultiplier(ClientContext &context);
double_t GetHeavyPartitionFraction(ClientContext &context);
bool GetPathFindingBenchmarkOption(ClientContext &context);
bool GetPathFindingBenchmarkLaneActivityOption(ClientContext &context);
string GetPathFindingBenchmarkPrefix(ClientContext &context);
bool GetPathFindingBuildReverseCSR(ClientContext &context);
int32_t GetPathFindingPushPullFrontierGate(ClientContext &context);
bool GetPathFindingDeduplicatePairs(ClientContext &context);

struct CorePGQOptions {
	static void Register(ExtensionLoader &loader) {
		RegisterExperimentalPathFindingOperator(loader);
		RegisterPathFindingTaskSize(loader);
		RegisterPathFindingLightPartitionMultiplier(loader);
		RegisterPathFindingHeavyPartitionFraction(loader);
		RegisterPathFindingBenchmark(loader);
		RegisterPathFindingBenchmarkLaneActivity(loader);
		RegisterPathFindingBenchmarkPrefix(loader);
		RegisterPathFindingBuildReverseCSR(loader);
		RegisterPathFindingPushPullFrontierGate(loader);
		RegisterPathFindingDeduplicatePairs(loader);
	}

private:
	static void RegisterExperimentalPathFindingOperator(ExtensionLoader &loader);
	static void RegisterPathFindingTaskSize(ExtensionLoader &loader);
	static void RegisterPathFindingLightPartitionMultiplier(ExtensionLoader &loader);
	static void RegisterPathFindingHeavyPartitionFraction(ExtensionLoader &loader);
	static void RegisterPathFindingBenchmark(ExtensionLoader &loader);
	static void RegisterPathFindingBenchmarkLaneActivity(ExtensionLoader &loader);
	static void RegisterPathFindingBenchmarkPrefix(ExtensionLoader &loader);
	static void RegisterPathFindingBuildReverseCSR(ExtensionLoader &loader);
	static void RegisterPathFindingPushPullFrontierGate(ExtensionLoader &loader);
	static void RegisterPathFindingDeduplicatePairs(ExtensionLoader &loader);
};

} // namespace duckdb
