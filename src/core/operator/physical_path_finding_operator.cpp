#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckpgq/core/operator/bfs_state.hpp"
#include <duckpgq/core/operator/iterative_length/bidirectional_iterative_length_state.hpp>
#include <duckpgq/core/operator/iterative_length/iterative_length_state.hpp>
#include <duckpgq/core/operator/iterative_length/push_pull_iterative_length_state.hpp>
#include <duckpgq/core/operator/local_csr/local_csr_event.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <duckpgq_state.hpp>
#include <fstream>
#include <thread>

namespace duckdb {

namespace {

PathFindingOperatorMode ParsePathFindingOperatorMode(const string &mode) {
	if (mode == "iterativelength") {
		return PathFindingOperatorMode::ITERATIVE_LENGTH;
	}
	if (mode == "pushpulliterativelength") {
		return PathFindingOperatorMode::PUSH_PULL_ITERATIVE_LENGTH;
	}
	if (mode == "bidirectionaliterativelength") {
		return PathFindingOperatorMode::BIDIRECTIONAL_ITERATIVE_LENGTH;
	}
	if (mode == "shortestpath") {
		return PathFindingOperatorMode::SHORTEST_PATH;
	}
	throw InvalidInputException("Unknown mode specified %s", mode);
}

void ConfigureLocalCSRStateForMode(LocalCSRState &local_csr_state, PathFindingOperatorMode mode) {
	switch (mode) {
	case PathFindingOperatorMode::BIDIRECTIONAL_ITERATIVE_LENGTH:
		local_csr_state.build_reverse_csr = true;
		local_csr_state.finalize_sparse_rows = false;
		break;
	case PathFindingOperatorMode::PUSH_PULL_ITERATIVE_LENGTH:
		local_csr_state.build_pull_csr = true;
		break;
	default:
		break;
	}
}

shared_ptr<BFSState> CreateBFSStateForMode(PathFindingOperatorMode mode, const shared_ptr<PathFindingBatch> &batch,
                                           LocalCSRState &local_csr_state, idx_t num_threads, ClientContext &context,
                                           int64_t vsize) {
	switch (mode) {
	case PathFindingOperatorMode::ITERATIVE_LENGTH:
		return make_shared_ptr<IterativeLengthState>(batch, local_csr_state.partition_csrs, num_threads, context, vsize);
	case PathFindingOperatorMode::PUSH_PULL_ITERATIVE_LENGTH:
		return make_shared_ptr<PushPullIterativeLengthState>(batch, local_csr_state.partition_csrs,
		                                                     local_csr_state.pull_partition_csrs, num_threads, context,
		                                                     vsize);
	case PathFindingOperatorMode::BIDIRECTIONAL_ITERATIVE_LENGTH:
		return make_shared_ptr<BidirectionalIterativeLengthState>(
		    batch, local_csr_state.partition_csrs, local_csr_state.reverse_partition_csrs, num_threads, context, vsize);
	case PathFindingOperatorMode::SHORTEST_PATH:
		// TODO(dtenwolde) implement also for shortest path
		throw NotImplementedException("Shortest path operator has not been implemented yet.");
	}
	throw InternalException("Unhandled path-finding operator mode");
}

void ScheduleBFSBatchForMode(PathFindingGlobalSinkState &gstate, const shared_ptr<PathFindingBatch> &batch,
                             Pipeline &pipeline, Event &event, const PhysicalPathFinding *op, ClientContext &context) {
	auto bfs_state = CreateBFSStateForMode(gstate.path_finding_mode, batch, *gstate.local_csr_state, gstate.num_threads,
	                                       context, gstate.csr->vsize);
	bfs_state->ScheduleBFSBatch(pipeline, event, op);
	gstate.bfs_states.push_back(std::move(bfs_state));
}

SinkFinalizeType FinalizeCSRBuildPhase(PathFindingGlobalSinkState &gstate, Pipeline &pipeline, Event &event,
                                        const PhysicalPathFinding &op, ClientContext &context) {
	++gstate.child;
	auto local_csr_state = make_shared_ptr<LocalCSRState>(context, gstate.csr, gstate.num_threads);
	ConfigureLocalCSRStateForMode(*local_csr_state, gstate.path_finding_mode);
	gstate.local_csr_state = local_csr_state;
	event.InsertEvent(make_shared_ptr<LocalCSREvent>(local_csr_state, pipeline, op, context));
	return SinkFinalizeType::READY;
}

SinkFinalizeType FinalizePathFindingPhase(PathFindingGlobalSinkState &gstate, Pipeline &pipeline, Event &event,
                                          const PhysicalPathFinding &op, ClientContext &context) {
	if (gstate.global_pairs->Count() == 0) {
		return SinkFinalizeType::READY;
	}

	while (gstate.global_scan_state.next_row_index < gstate.global_pairs->Count()) {
		auto current_chunk = make_shared_ptr<DataChunk>();
		current_chunk->Initialize(context, gstate.global_pairs->Types());
		gstate.global_pairs->Scan(gstate.global_scan_state, *current_chunk);
		auto batch = make_shared_ptr<PathFindingBatch>(current_chunk, gstate.next_batch_index++);
		ScheduleBFSBatchForMode(gstate, batch, pipeline, event, &op, context);
	}

	++gstate.child;
	auto duckpgq_state = GetDuckPGQState(context);
	duckpgq_state->csr_to_delete.insert(gstate.csr_id);
	return SinkFinalizeType::READY;
}

} // namespace

PhysicalPathFinding::PhysicalPathFinding(PhysicalPlan &physical_plan, LogicalExtensionOperator &op,
                                         PhysicalOperator &pairs, PhysicalOperator &csr)
    : PhysicalComparisonJoin(physical_plan, op, TYPE, {}, JoinType::INNER, op.estimated_cardinality) {
	children.push_back(pairs);
	children.push_back(csr);
	expressions = std::move(op.expressions);
	estimated_cardinality = op.estimated_cardinality;
	auto &path_finding_op = op.Cast<LogicalPathFindingOperator>();
	mode = path_finding_op.mode;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
PathFindingLocalSinkState::PathFindingLocalSinkState(ClientContext &context, const PhysicalPathFinding &op)
    : local_pairs(context, op.children[0].get().GetTypes()) {
}

void PathFindingLocalSinkState::Sink(DataChunk &input, idx_t child) {
	if (child == 1) {
		local_pairs.Append(input);
	}
}

PathFindingGlobalSinkState::PathFindingGlobalSinkState(ClientContext &context, const PhysicalPathFinding &op)
    : context_(context) {
	global_pairs = make_uniq<ColumnDataCollection>(context, op.children[0].get().GetTypes());

	global_pairs->InitializeScan(global_scan_state);
	result_scan_idx = 0;
	next_batch_index = 0;

	child = 0;
	mode = op.mode;
	path_finding_mode = ParsePathFindingOperatorMode(mode);
	auto &scheduler = TaskScheduler::GetScheduler(context);
	num_threads = scheduler.NumberOfThreads();
}

void PathFindingGlobalSinkState::Sink(DataChunk &input, PathFindingLocalSinkState &lstate) {
	if (child == 0) {
		// CSR phase
		auto duckpgq_state = GetDuckPGQState(context_);
		csr_id = input.GetValue(0, 0).GetValue<int64_t>();
		csr = duckpgq_state->GetCSR(csr_id);
	} else {
		// path-finding phase
		lstate.Sink(input, child);
	}
}

unique_ptr<GlobalSinkState> PhysicalPathFinding::GetGlobalSinkState(ClientContext &context) const {
	D_ASSERT(!sink_state);
	return make_uniq<PathFindingGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalPathFinding::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PathFindingLocalSinkState>(context.client, *this);
}

SinkResultType PhysicalPathFinding::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PathFindingLocalSinkState>();
	gstate.Sink(chunk, lstate);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalPathFinding::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PathFindingLocalSinkState>();
	if (gstate.child == 0) {
		return SinkCombineResultType::FINISHED;
	}
	gstate.global_pairs->Combine(lstate.local_pairs);
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

SinkFinalizeType PhysicalPathFinding::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
	if (gstate.csr == nullptr) {
		throw InternalException("CSR not initialized");
	}

	if (gstate.child == 0) {
		return FinalizeCSRBuildPhase(gstate, pipeline, event, *this, context);
	}
	return FinalizePathFindingPhase(gstate, pipeline, event, *this, context);
}

InsertionOrderPreservingMap<string> PhysicalPathFinding::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Mode"] = mode;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalPathFinding::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                        GlobalOperatorState &gstate, OperatorState &state) const {
	return OperatorResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

class PathFindingLocalSourceState : public LocalSourceState {
public:
	explicit PathFindingLocalSourceState(ClientContext &context, const PhysicalPathFinding &op) : op(op) {
	}

	const PhysicalPathFinding &op;
};

class PathFindingGlobalSourceState : public GlobalSourceState {
public:
	explicit PathFindingGlobalSourceState(const PhysicalPathFinding &op) : op(op), initialized(false) {
	}

public:
	idx_t MaxThreads() override {
		return 1;
	}
	const PhysicalPathFinding &op;

	mutex lock;
	bool initialized;
};

unique_ptr<GlobalSourceState> PhysicalPathFinding::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PathFindingGlobalSourceState>(*this);
}

unique_ptr<LocalSourceState> PhysicalPathFinding::GetLocalSourceState(ExecutionContext &context,
                                                                      GlobalSourceState &gstate) const {
	return make_uniq<PathFindingLocalSourceState>(context.client, *this);
}

SourceResultType PhysicalPathFinding::GetDataInternal(ExecutionContext &context, DataChunk &result,
                                                      OperatorSourceInput &input) const {
	auto &pf_sink = sink_state->Cast<PathFindingGlobalSinkState>();
	// If there are no pairs, we're done
	if (pf_sink.global_pairs->Count() == 0) {
		return SourceResultType::FINISHED;
	}
	D_ASSERT(pf_sink.result_scan_idx < pf_sink.bfs_states.size());
	auto current_state = pf_sink.bfs_states[pf_sink.result_scan_idx];
	D_ASSERT(current_state->batch->output_index == pf_sink.result_scan_idx);
	auto result_types = current_state->pairs->GetTypes();
	result_types.push_back(current_state->bfs_type);
	current_state->pf_results->SetChildCardinality(current_state->pairs->size());
	current_state->pairs->Fuse(*current_state->pf_results);
	result.Move(*current_state->pairs);

	pf_sink.result_scan_idx++;
	if (pf_sink.result_scan_idx == pf_sink.bfs_states.size()) {
		return SourceResultType::FINISHED;
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalPathFinding::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	if (meta_pipeline.HasRecursiveCTE()) {
		throw NotImplementedException("Path Finding is not supported in recursive CTEs yet");
	}

	// becomes a source after both children fully sink their data
	meta_pipeline.GetState().SetPipelineSource(current, *this);

	// Create one child meta pipeline that will hold the LHS and RHS pipelines
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);

	// Build out LHS
	auto lhs_pipeline = child_meta_pipeline.GetBasePipeline();
	children[1].get().BuildPipelines(*lhs_pipeline, child_meta_pipeline);

	// Build out RHS
	auto &rhs_pipeline = child_meta_pipeline.CreatePipeline();
	children[0].get().BuildPipelines(rhs_pipeline, child_meta_pipeline);

	// Despite having the same sink, RHS and everything created after it need
	// their own (same) PipelineFinishEvent
	child_meta_pipeline.AddFinishEvent(rhs_pipeline);
}

} // namespace duckdb
