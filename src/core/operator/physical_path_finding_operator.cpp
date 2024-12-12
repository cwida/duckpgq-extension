#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckpgq/core/operator/bfs_state.hpp"
#include <duckpgq/core/operator/event/shortest_path_event.hpp>
#include <thread>
#include <duckpgq_state.hpp>
#include <duckpgq/core/operator/event/iterative_length_event.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {


PhysicalPathFinding::PhysicalPathFinding(LogicalExtensionOperator &op,
                                         unique_ptr<PhysicalOperator> pairs,
                                         unique_ptr<PhysicalOperator> csr)
    : PhysicalComparisonJoin(op, TYPE, {}, JoinType::INNER, op.estimated_cardinality) {
  children.push_back(std::move(pairs));
  children.push_back(std::move(csr));

  // TODO check if there are expressions
  expressions = std::move(op.expressions);
  estimated_cardinality = op.estimated_cardinality;
  auto &path_finding_op = op.Cast<LogicalPathFindingOperator>();
  mode = path_finding_op.mode;
}


//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
PathFindingLocalSinkState::PathFindingLocalSinkState(ClientContext &context, const PhysicalPathFinding &op)
      : local_pairs(context, op.children[0]->GetTypes()) {}

void PathFindingLocalSinkState::Sink(DataChunk &input, idx_t child) {
  if (child == 1) {
    // Add the tasks (src, dst) to sink
    // Optimizations: Eliminate duplicate sources/destinations
    local_pairs.Append(input);
  }
}


PathFindingGlobalSinkState::PathFindingGlobalSinkState(ClientContext &context,
                       const PhysicalPathFinding &op) : context_(context) {
  global_pairs =
      make_uniq<ColumnDataCollection>(context, op.children[0]->GetTypes());
  global_csr_column_data =
      make_uniq<ColumnDataCollection>(context, op.children[1]->GetTypes());

  child = 0;
  mode = op.mode;
  auto &scheduler = TaskScheduler::GetScheduler(context);
  num_threads = scheduler.NumberOfThreads();
}

void PathFindingGlobalSinkState::Sink(DataChunk &input, PathFindingLocalSinkState &lstate) {
  if (child == 0) {
    // CSR phase
    csr_id = input.GetValue(0, 0).GetValue<int64_t>();
    auto duckpgq_state = GetDuckPGQState(context_);
    csr = duckpgq_state->GetCSR(csr_id);
  } else {
    // path-finding phase
    lstate.Sink(input, child);
  }
}

unique_ptr<GlobalSinkState>
PhysicalPathFinding::GetGlobalSinkState(ClientContext &context) const {
  D_ASSERT(!sink_state);
  return make_uniq<PathFindingGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState>
PhysicalPathFinding::GetLocalSinkState(ExecutionContext &context) const {
  return make_uniq<PathFindingLocalSinkState>(context.client, *this);
}

SinkResultType PhysicalPathFinding::Sink(ExecutionContext &context,
                                         DataChunk &chunk,
                                         OperatorSinkInput &input) const {
  auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
  auto &lstate = input.local_state.Cast<PathFindingLocalSinkState>();
  gstate.Sink(chunk, lstate);
  return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalPathFinding::Combine(ExecutionContext &context,
                             OperatorSinkCombineInput &input) const {
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

SinkFinalizeType
PhysicalPathFinding::Finalize(Pipeline &pipeline, Event &event,
                              ClientContext &context,
                              OperatorSinkFinalizeInput &input) const {
  auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
  auto &global_tasks = gstate.global_pairs;
  auto duckpgq_state = GetDuckPGQState(context);
  if (gstate.csr == nullptr) {
    throw InternalException("CSR not initialized");
  }

  // Check if we have to do anything for CSR child
  if (gstate.child == 0) {
    ++gstate.child;
    return SinkFinalizeType::READY;
  }
  if (global_tasks->Count() == 0) {
    return SinkFinalizeType::READY;
  }

  gstate.global_bfs_state = make_shared_ptr<GlobalBFSState>(global_tasks, gstate.csr, gstate.csr->vsize-2,
    gstate.num_threads, mode, context);
  gstate.global_bfs_state->InitializeBFS(pipeline, event, this);

  // Move to the next input child
  ++gstate.child;
  duckpgq_state->csr_to_delete.insert(gstate.csr_id);
  return SinkFinalizeType::READY;
}

void GlobalBFSState::ScheduleBFSBatch(Pipeline &pipeline, Event &event) {
  std::cout << "Starting BFS for " << current_pairs_batch->size() << " pairs with " << LANE_LIMIT << " batch size." << std::endl;
  std::cout << "Number of started searches: " << started_searches << std::endl;
  auto &result_validity = FlatVector::Validity(current_pairs_batch->data[0]);
  std::bitset<LANE_LIMIT> seen_mask;
  seen_mask.set();

  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    lane_to_num[lane] = -1;
    while (started_searches < current_pairs_batch->size()) {
      auto search_num = started_searches++;
      int64_t src_pos = vdata_src.sel->get_index(search_num);
      if (!vdata_src.validity.RowIsValid(src_pos)) {
        result_validity.SetInvalid(search_num);
      } else {
        visit1[src[src_pos]][lane] = true;
        // bfs_state->seen[bfs_state->src[src_pos]][lane] = true;
        lane_to_num[lane] = search_num; // active lane
        active++;
        seen_mask[lane] = false;
        break;
      }
    }
  }
  for (int64_t i = 0; i < v_size; i++) {
    seen[i] = seen_mask;
  }

  if (mode == "iterativelength") {
    throw NotImplementedException("Iterative length has not been implemented yet");
    // event.InsertEvent(
    // make_shared_ptr<ParallelIterativeEvent>(gstate, pipeline, *this));
  } else if (mode == "shortestpath") {
    std::cout << "Scheduling shortest path event" << std::endl;
    event.InsertEvent(
      make_shared_ptr<ShortestPathEvent>(shared_from_this(), pipeline, *op));
  } else {
    throw NotImplementedException("Mode not supported");
  }
}


void GlobalBFSState::InitializeBFS(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op_) {
  current_pairs_batch = make_uniq<DataChunk>();
  pairs->InitializeScanChunk(input_scan_state, *current_pairs_batch);
  pairs->Scan(input_scan_state, *current_pairs_batch);
  op = op_;

  path_finding_result = make_uniq<DataChunk>();
  path_finding_result->Initialize(context, {LogicalType::LIST(LogicalType::BIGINT)});
  current_batch_path_list_len = 0;

  started_searches = 0; // reset
  active = 0;

  auto &src_data = current_pairs_batch->data[0];
  auto &dst_data = current_pairs_batch->data[1];
  src_data.ToUnifiedFormat(current_pairs_batch->size(), vdata_src);
  dst_data.ToUnifiedFormat(current_pairs_batch->size(), vdata_dst);
  src = FlatVector::GetData<int64_t>(src_data);
  dst = FlatVector::GetData<int64_t>(dst_data);

  // remaining pairs for current batch
  while (started_searches < current_pairs_batch->size()) {
    ScheduleBFSBatch(pipeline, event);
    Clear();
  }
  if (started_searches != current_pairs_batch->size()) {
    throw InternalException("Number of started searches does not match the number of pairs");
  }
  // path_finding_result->SetCardinality(current_pairs_batch->size());
  // path_finding_result->Print();
  // current_pairs_batch->Fuse(*path_finding_result);
  // current_pairs_batch->Print();
  // results->Append(*current_pairs_batch);
  // total_pairs_processed += current_pairs_batch->size();
  // std::cout << "Total pairs processed: " << total_pairs_processed << std::endl;
  // if (total_pairs_processed < pairs->Count()) {
  //   InitializeBFS(pipeline, event, op);
  // }
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
OperatorResultType PhysicalPathFinding::ExecuteInternal(
    ExecutionContext &context, DataChunk &input, DataChunk &chunk,
    GlobalOperatorState &gstate, OperatorState &state) const {
  return OperatorResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

class PathFindingLocalSourceState : public LocalSourceState {
public:
  explicit PathFindingLocalSourceState(ClientContext &context,
                                       const PhysicalPathFinding &op)
      : op(op) {}

  const PhysicalPathFinding &op;
};

class PathFindingGlobalSourceState : public GlobalSourceState {
public:
  explicit PathFindingGlobalSourceState(const PhysicalPathFinding &op)
      : op(op), initialized(false) {}

public:
  idx_t MaxThreads() override {
    return 1;
  }
  const PhysicalPathFinding &op;

  mutex lock;
  bool initialized;
};

unique_ptr<GlobalSourceState>
PhysicalPathFinding::GetGlobalSourceState(ClientContext &context) const {
  return make_uniq<PathFindingGlobalSourceState>(*this);
}

unique_ptr<LocalSourceState>
PhysicalPathFinding::GetLocalSourceState(ExecutionContext &context,
                                         GlobalSourceState &gstate) const {
  return make_uniq<PathFindingLocalSourceState>(context.client, *this);
}

SourceResultType PhysicalPathFinding::GetData(ExecutionContext &context, DataChunk &result,
                             OperatorSourceInput &input) const {
  auto &pf_sink = sink_state->Cast<PathFindingGlobalSinkState>();
  pf_sink.global_bfs_state->path_finding_result->SetCardinality(pf_sink.global_bfs_state->pairs->Count());
  pf_sink.global_bfs_state->path_finding_result->Print();
  // If there are no pairs, we're done
  if (pf_sink.global_bfs_state->results->Count() == 0) {
    return SourceResultType::FINISHED;
  }
  pf_sink.global_bfs_state->results->Scan(pf_sink.global_bfs_state->result_scan_state, result);
  if (pf_sink.global_bfs_state->result_scan_state.current_row_index == pf_sink.global_bfs_state->results->Count()) {
    return SourceResultType::FINISHED;
  }
  return SourceResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalPathFinding::BuildPipelines(Pipeline &current,
                                         MetaPipeline &meta_pipeline) {
  D_ASSERT(children.size() == 2);
  if (meta_pipeline.HasRecursiveCTE()) {
    throw NotImplementedException(
        "Path Finding is not supported in recursive CTEs yet");
  }

  // becomes a source after both children fully sink their data
  meta_pipeline.GetState().SetPipelineSource(current, *this);

  // Create one child meta pipeline that will hold the LHS and RHS pipelines
  auto &child_meta_pipeline =
      meta_pipeline.CreateChildMetaPipeline(current, *this);

  // Build out LHS
  auto lhs_pipeline = child_meta_pipeline.GetBasePipeline();
  children[1]->BuildPipelines(*lhs_pipeline, child_meta_pipeline);

  // Build out RHS
  auto &rhs_pipeline = child_meta_pipeline.CreatePipeline();
  children[0]->BuildPipelines(rhs_pipeline, child_meta_pipeline);

  // Despite having the same sink, RHS and everything created after it need
  // their own (same) PipelineFinishEvent
  child_meta_pipeline.AddFinishEvent(rhs_pipeline);
}

} // namespace core
} // namespace duckpgq
