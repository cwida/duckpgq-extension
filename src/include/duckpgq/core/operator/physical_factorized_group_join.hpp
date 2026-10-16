//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/operator/physical_factorized_group_join.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/execution/operator/aggregate/grouped_aggregate_data.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/radix_partitioned_hashtable.hpp"
#include "duckdb/parser/group_by_node.hpp"

namespace duckdb {

class PhysicalFactorizedGroupJoinSink;

//! Binds a built-in aggregate by name for the given arguments
unique_ptr<BoundAggregateExpression> BindFactorizedAggregate(ClientContext &context, const string &name,
                                                             vector<unique_ptr<Expression>> children);

//! State of the shared hash table or directory, owned by the operator for the duration of one execution
struct FactorizedGroupJoinBuildState {
	mutex lock;
	unique_ptr<GlobalSinkState> radix_state;
	atomic<idx_t> finalized_sides {0};
	//! Directory mode: one counter per key value and side, indexed by key - directory_min
	unique_ptr<atomic<int64_t>[]> directory[2];
	//! Set once the scan has started; a sink state created after that belongs to a new execution
	bool consumed = false;
};

//! Aggregates two inputs per join key into one hash table and emits every key found on both sides once, together
//! with the number of rows (or the summed weight) each side contributed for that key.
class PhysicalFactorizedGroupJoin : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;
	static constexpr idx_t SIDES = 2;

	PhysicalFactorizedGroupJoin(PhysicalPlan &physical_plan, ClientContext &context, LogicalOperator &op,
	                            PhysicalOperator &left, PhysicalOperator &right,
	                            vector<vector<unique_ptr<Expression>>> keys, vector<unique_ptr<Expression>> weights,
	                            Value directory_min, idx_t directory_range, idx_t estimated_cardinality);

	//! Join keys per side, resolved against that side's input
	vector<vector<unique_ptr<Expression>>> keys;
	//! Optional weight per side (null: every row counts once)
	vector<unique_ptr<Expression>> weights;
	vector<LogicalType> key_types;
	//! Groups and the two per-side aggregates of the shared hash table
	GroupedAggregateData grouped_aggregate_data;
	GroupingSet grouping_set;
	unique_ptr<RadixPartitionedHashTable> radix_table;
	//! Which aggregate each side updates
	unsafe_vector<idx_t> aggregate_filter[SIDES];
	//! Payload column of each side's weight, if any
	optional_idx weight_payload_index[SIDES];
	vector<LogicalType> payload_types;
	//! Layout of a hash table scan: keys followed by the two aggregates
	vector<LogicalType> scan_types;
	//! Directory mode: the single key lies in [directory_min, directory_min + directory_range)
	int64_t directory_min = 0;
	idx_t directory_range = 0;

	//! Helper operators that sink each input into the shared table; they are also this operator's children
	optional_ptr<PhysicalOperator> sinks[SIDES];
	//! Shared between both sinks and the scan of one execution. Recursive CTEs re-run the pipelines by recreating
	//! the sink and source states only, so the build state is renewed from those rather than from BuildPipelines.
	mutable mutex build_lock;
	mutable shared_ptr<FactorizedGroupJoinBuildState> build_state;

public:
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}
	OrderPreservationType SourceOrder() const override {
		return OrderPreservationType::NO_ORDER;
	}

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;

	//! The build state a sink joins: the current one, or a fresh one when the current one was already scanned
	shared_ptr<FactorizedGroupJoinBuildState> AcquireBuildState() const;
	//! The build state the scan consumes; marks it so that later sinks start a new one
	shared_ptr<FactorizedGroupJoinBuildState> ConsumeBuildState() const;
	//! Global sink state of the shared hash table, created on first use
	GlobalSinkState &GetRadixSinkState(ClientContext &context, FactorizedGroupJoinBuildState &build) const;
	bool UsesDirectory() const {
		return directory_range > 0;
	}
	//! Counters of one side of the directory, allocated on first use
	atomic<int64_t> *GetDirectory(idx_t side, FactorizedGroupJoinBuildState &build) const;

protected:
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
};

} // namespace duckdb
