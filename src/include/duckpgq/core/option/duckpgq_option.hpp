#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

//! The extension's settings, registered from one table (duckpgq_option.cpp) and read through the typed getters
struct CorePGQOptions {
	static void Register(ExtensionLoader &loader);
};

bool GetPathFindingOption(ClientContext &context);
int32_t GetLightPartitionMultiplier(ClientContext &context);
double_t GetHeavyPartitionFraction(ClientContext &context);
bool GetPathFindingBenchmarkOption(ClientContext &context);
bool GetPathFindingBenchmarkLaneActivityOption(ClientContext &context);
string GetPathFindingBenchmarkPrefix(ClientContext &context);
bool GetPathFindingBuildReverseCSR(ClientContext &context);
int32_t GetPathFindingPushPullFrontierGate(ClientContext &context);
bool GetPathFindingDeduplicatePairs(ClientContext &context);
bool GetPathFindingGroupedBatches(ClientContext &context);
bool GetPathFindingReachOption(ClientContext &context);
bool GetPathFindingReachFilteredOption(ClientContext &context);
int32_t GetPathFindingThreadsPerBatch(ClientContext &context);
int32_t GetPathFindingMaxConcurrentBatches(ClientContext &context);
int32_t GetPathFindingReverseOrientationRatio(ClientContext &context);
int32_t GetPathFindingSourceGroupRatio(ClientContext &context);
double_t GetCSRIndexFoldRatio(ClientContext &context);
bool GetPathFindingSnapshotCache(ClientContext &context);
bool GetPathFindingTargetEarlyTermination(ClientContext &context);
int32_t GetPathFindingSparseFrontierDivisor(ClientContext &context);
int32_t GetPathFindingFewSourcesLimit(ClientContext &context);
int64_t GetPathFindingGroupedBatchesBudget(ClientContext &context);
int32_t GetPathFindingBatchOversubscription(ClientContext &context);
bool GetPathFindingFrontierBitmap(ClientContext &context);
int32_t GetPathFindingFrontierBitmapFilter(ClientContext &context);
int32_t GetPathFindingSparseLevelDivisor(ClientContext &context);
int32_t GetPathFindingPartitionVertices(ClientContext &context);
int32_t GetPathFindingRowBucketRows(ClientContext &context);
bool GetPathFindingSubRangeTasks(ClientContext &context);
bool GetCSRIndexChangeGroups(ClientContext &context);
bool GetPathFindingLaneWaves(ClientContext &context);
bool GetPathFindingSourceSearchRounds(ClientContext &context);
int32_t GetPathFindingVisitPrefetch(ClientContext &context);

} // namespace duckdb
