//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckpgq/core/operator/physical_path_finding_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/bfs_state.hpp"
#include "local_csr/local_csr_state.hpp"

#include <duckpgq/core/utils/compressed_sparse_row.hpp>
#include <chrono>

namespace duckdb {
class BFSState;      // Forward declaration
class LocalCSRState; // Forward declaration
class SourceGroupedIterativeLengthState;

enum class PathFindingOperatorMode {
	ITERATIVE_LENGTH,
	PUSH_PULL_ITERATIVE_LENGTH,
	BIDIRECTIONAL_ITERATIVE_LENGTH,
	SHORTEST_PATH
};

enum class PathFindingSearchOrientation { FORWARD, REVERSE };

struct PathFindingPairStats {
	idx_t pair_count = 0;
	idx_t distinct_src_count = 0;
	idx_t distinct_dst_count = 0;
	idx_t null_pair_count = 0;
	idx_t self_pair_count = 0;
	bool distinct_counts_are_exact = false;
};

class PhysicalPathFinding : public PhysicalComparisonJoin {
public:
	PhysicalPathFinding(PhysicalPlan &physical_plan, LogicalExtensionOperator &op, PhysicalOperator &pairs,
	                    PhysicalOperator &csr, PhysicalOperator *counts = nullptr);

	static constexpr PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;
	vector<unique_ptr<Expression>> expressions;
	string mode; // "iterativelength" or "shortestpath"
	string cache_key;
	bool edge_input;
	bool precounted_edge_input;
	idx_t precounted_vertex_count;
	idx_t precounted_edge_count;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	// CachingOperator Interface
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	// Source interface
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PathFindingLocalSinkState : public LocalSinkState {
public:
	PathFindingLocalSinkState(ClientContext &context, const PhysicalPathFinding &op);

	void SinkPairs(DataChunk &input);
	void SinkEndpoints(DataChunk &input);

	ColumnDataCollection local_pairs;
	ClientContext &context;
	std::vector<LocalCSRBuildPartition> local_endpoint_partitions;
	idx_t local_counted_endpoint_count = 0;
	idx_t local_filled_endpoint_count = 0;
	idx_t vertex_count = 0;
	idx_t expected_edge_count = 0;
	idx_t endpoint_partition_width = 0;
	bool endpoint_metadata_initialized = false;
};

class PathFindingGlobalSinkState : public GlobalSinkState {
public:
	PathFindingGlobalSinkState(ClientContext &context, const PhysicalPathFinding &op);

	void Sink(DataChunk &input, PathFindingLocalSinkState &lstate);
	// pairs is a 2-column table with src and dst
	unique_ptr<ColumnDataCollection> global_pairs;
	mutex global_pairs_lock;
	mutex global_endpoints_lock;
	mutex endpoint_init_lock;
	ColumnDataScanState global_scan_state;
	idx_t result_scan_idx;
	idx_t next_batch_index;
	vector<shared_ptr<BFSState>> bfs_states;
	vector<shared_ptr<SourceGroupedIterativeLengthState>> source_group_states;
	vector<std::pair<idx_t, idx_t>> source_group_output_refs;
	vector<int64_t> source_group_sources;
	vector<vector<shared_ptr<DataChunk>>> source_group_output_chunks;
	vector<shared_ptr<DataChunk>> global_output_batches;
	vector<vector<idx_t>> global_output_to_search;
	bool use_global_deduplication;
	bool use_source_grouping;
	bool global_dedupe_results_initialized;
	bool edge_input;
	bool precounted_edge_input;
	std::vector<std::vector<LocalCSRBuildPartition>> endpoint_build_runs;
	std::vector<shared_ptr<LocalCSR>> endpoint_partition_csrs;
	idx_t counted_endpoint_count = 0;
	idx_t endpoint_count = 0;
	idx_t vertex_count = 0;
	idx_t expected_edge_count = 0;
	idx_t endpoint_partition_width = 0;
	bool endpoint_counts_initialized = false;
	bool endpoint_counts_finalized = false;
	bool endpoint_build_started = false;
	std::chrono::steady_clock::time_point endpoint_build_start;
	std::chrono::steady_clock::time_point endpoint_fill_start;
	CSR *csr;
	int32_t csr_id;
	size_t child;
	string mode;
	PathFindingOperatorMode path_finding_mode;
	PathFindingSearchOrientation search_orientation;
	ClientContext &context_;
	idx_t num_threads;
	shared_ptr<LocalCSRState> local_csr_state;
	PathFindingPairStats pair_stats;
};

} // namespace duckdb
