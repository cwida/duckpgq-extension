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

namespace duckdb {
class BFSState;      // Forward declaration
class LocalCSRState; // Forward declaration

enum class PathFindingOperatorMode {
	ITERATIVE_LENGTH,
	PUSH_PULL_ITERATIVE_LENGTH,
	BIDIRECTIONAL_ITERATIVE_LENGTH,
	SHORTEST_PATH
};

class PhysicalPathFinding : public PhysicalComparisonJoin {
public:
	PhysicalPathFinding(PhysicalPlan &physical_plan, LogicalExtensionOperator &op, PhysicalOperator &pairs,
	                    PhysicalOperator &csr);

	static constexpr PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;
	vector<unique_ptr<Expression>> expressions;
	string mode; // "iterativelength" or "shortestpath"

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

	void Sink(DataChunk &input, idx_t child);

	ColumnDataCollection local_pairs;
};

class PathFindingGlobalSinkState : public GlobalSinkState {
public:
	PathFindingGlobalSinkState(ClientContext &context, const PhysicalPathFinding &op);

	void Sink(DataChunk &input, PathFindingLocalSinkState &lstate);
	// pairs is a 2-column table with src and dst
	unique_ptr<ColumnDataCollection> global_pairs;
	ColumnDataScanState global_scan_state;
	idx_t result_scan_idx;
	idx_t next_batch_index;
	vector<shared_ptr<BFSState>> bfs_states;
	CSR *csr;
	int32_t csr_id;
	size_t child;
	string mode;
	PathFindingOperatorMode path_finding_mode;
	ClientContext &context_;
	idx_t num_threads;
	shared_ptr<LocalCSRState> local_csr_state;
};

} // namespace duckdb
