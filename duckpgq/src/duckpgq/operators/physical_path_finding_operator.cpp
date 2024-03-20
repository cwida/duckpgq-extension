#include "duckpgq/operators/physical_path_finding_operator.hpp"

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include <thread>

namespace duckdb {


PhysicalPathFinding::PhysicalPathFinding(LogicalExtensionOperator &op,
                                         unique_ptr<PhysicalOperator> left,
                                         unique_ptr<PhysicalOperator> right)
    : PhysicalComparisonJoin(op, TYPE, {}, JoinType::INNER, 0) {
  children.push_back(std::move(left));
  children.push_back(std::move(right));
  expressions = std::move(op.expressions);
}

void PhysicalPathFinding::GlobalCompressedSparseRow::InitializeVertex(int64_t v_size_) {
  lock_guard<mutex> csr_init_lock(csr_lock);

  if (initialized_v) {
    return;
  }
  v_size = v_size_ + 2;
  try {
    v = new std::atomic<int64_t>[v_size];
  } catch (std::bad_alloc const &) {
    throw InternalException("Unable to initialize vector of size for csr vertex table "
                                             "representation");
  }
  for (idx_t i = 0; i < v_size; ++i) {
    v[i].store(0);
  }
  initialized_v = true;
}
void PhysicalPathFinding::GlobalCompressedSparseRow::InitializeEdge(
    int64_t e_size) {
  const lock_guard<mutex> csr_init_lock(csr_lock);
  try {
    e.resize(e_size, 0);
    edge_ids.resize(e_size, 0);
  } catch (std::bad_alloc const &) {
    throw InternalException("Unable to initialize vector of size for csr "
                            "edge table representation");
  }
  for (idx_t i = 1; i < v_size; i++) {
    v[i] += v[i-1];
  }
  initialized_e = true;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PathFindingLocalState : public LocalSinkState {
public:
  using GlobalCompressedSparseRow =
      PhysicalPathFinding::GlobalCompressedSparseRow;
  PathFindingLocalState(ClientContext &context, const PhysicalPathFinding &op,
                        const idx_t child) : local_tasks(context, op.children[0]->GetTypes()) {
  }

  void Sink(DataChunk &input, GlobalCompressedSparseRow &global_csr) {
    if (global_csr.is_ready) {
      // Add the tasks (src, dst) to sink
      // Optimizations: Eliminate duplicate sources/destinations
      input.Print();
      local_tasks.Append(input);
      local_tasks.Print();
      return;
    }
    CreateCSR(input, global_csr);
  }

  static void CreateCSR(DataChunk &input,
                        GlobalCompressedSparseRow &global_csr);

  ColumnDataCollection local_tasks;
};

void PathFindingLocalState::CreateCSR(DataChunk &input,
                                      GlobalCompressedSparseRow &global_csr) {
  if (!global_csr.initialized_v) {
    const auto v_size = input.data[8].GetValue(0).GetValue<int64_t>();
    global_csr.InitializeVertex(v_size);
  }
  auto result = Vector(LogicalTypeId::BIGINT);
  BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
      input.data[6], input.data[5], result, input.size(),
      [&](const int64_t src, const int64_t cnt) {
        int64_t edge_count = 0;
        global_csr.v[src + 2] = cnt;
        edge_count = edge_count + cnt;
        return edge_count;
      });

  if (!global_csr.initialized_e) {
    const auto e_size = input.data[7].GetValue(0).GetValue<int64_t>();
    global_csr.InitializeEdge(e_size);
  }
  TernaryExecutor::Execute<int64_t, int64_t, int64_t, int32_t>(
      input.data[6], input.data[4], input.data[2], result, input.size(),
      [&](int64_t src, int64_t dst, int64_t edge_id) {
        const auto pos = ++global_csr.v[src + 1];
        global_csr.e[static_cast<int64_t>(pos) - 1] = dst;
        global_csr.edge_ids[static_cast<int64_t>(pos) - 1] = edge_id;
        return 1;
      });
  global_csr.Print(); // Debug print
}

class PathFindingGlobalState : public GlobalSinkState {
public:
  using GlobalCompressedSparseRow = PhysicalPathFinding::GlobalCompressedSparseRow;
  PathFindingGlobalState(ClientContext &context,
                         const PhysicalPathFinding &op) : global_tasks(context, op.children[0]->GetTypes()) {
    RowLayout lhs_layout;
    lhs_layout.Initialize(op.children[0]->types);
    RowLayout rhs_layout;
    rhs_layout.Initialize(op.children[1]->types);
    global_csr = make_uniq<GlobalCompressedSparseRow>(context, rhs_layout);
  }

  PathFindingGlobalState(PathFindingGlobalState &prev)
      : GlobalSinkState(prev), global_tasks(prev.global_tasks),
      global_csr(std::move(prev.global_csr)), child(prev.child + 1) {}

  void Sink(DataChunk &input, PathFindingLocalState &lstate) const {
    lstate.Sink(input, *global_csr);
  }

  unique_ptr<GlobalCompressedSparseRow> global_csr;
  size_t child;

  ColumnDataCollection global_tasks;
  ColumnDataScanState scan_state;
  ColumnDataAppendState append_state;
};

unique_ptr<GlobalSinkState>
PhysicalPathFinding::GetGlobalSinkState(ClientContext &context) const {
  D_ASSERT(!sink_state);
  return make_uniq<PathFindingGlobalState>(context, *this);
}

unique_ptr<LocalSinkState>
PhysicalPathFinding::GetLocalSinkState(ExecutionContext &context) const {
  idx_t sink_child = 0;
  if (sink_state) {
     const auto &pathfinding_sink = sink_state->Cast<PathFindingGlobalState>();
     sink_child = pathfinding_sink.child;
  }
  return make_uniq<PathFindingLocalState>(context.client, *this, sink_child);
}

SinkResultType PhysicalPathFinding::Sink(ExecutionContext &context,
                                         DataChunk &chunk,
                                         OperatorSinkInput &input) const {
  auto &gstate = input.global_state.Cast<PathFindingGlobalState>();
  auto &lstate = input.local_state.Cast<PathFindingLocalState>();
  gstate.Sink(chunk, lstate);
  gstate.global_csr->is_ready = true;
  return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalPathFinding::Combine(ExecutionContext &context,
                             OperatorSinkCombineInput &input) const {
  auto &gstate = input.global_state.Cast<PathFindingGlobalState>();
  auto &lstate = input.local_state.Cast<PathFindingLocalState>();
  auto &client_profiler = QueryProfiler::Get(context.client);

  gstate.global_tasks.Combine(lstate.local_tasks);
  client_profiler.Flush(context.thread.profiler);
  gstate.global_tasks.Print();
  return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType
PhysicalPathFinding::Finalize(Pipeline &pipeline, Event &event,
                              ClientContext &context,
                              OperatorSinkFinalizeInput &input) const {
  auto &gstate = input.global_state.Cast<PathFindingGlobalState>();

  return SinkFinalizeType::READY;
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
      : op(op){
  }

  const PhysicalPathFinding &op;
};

class PathFindingGlobalSourceState : public GlobalSourceState {
public:
  explicit PathFindingGlobalSourceState(const PhysicalPathFinding &op)
      : op(op), initialized(false) {}

  void Initialize(PathFindingGlobalState &sink_state) {
    lock_guard<mutex> initializing(lock);
    if (initialized) {
      return;
    }
    initialized = true;
  }

public:
  idx_t MaxThreads() override {
    const auto &sink_state = (op.sink_state->Cast<PathFindingGlobalState>());
    return 1;
  }

  void GetNextPair(ClientContext &client, PathFindingGlobalState &gstate,
                   PathFindingLocalSourceState &lstate) {
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

SourceResultType
PhysicalPathFinding::GetData(ExecutionContext &context, DataChunk &result,
                             OperatorSourceInput &input) const {
  auto &pf_sink = sink_state->Cast<PathFindingGlobalState>();
  auto &pf_gstate = input.global_state.Cast<PathFindingGlobalSourceState>();
  auto &pf_lstate = input.local_state.Cast<PathFindingLocalSourceState>();
  pf_sink.global_tasks.Scan(pf_sink.scan_state, result);
  result.Print();
  pf_gstate.Initialize(pf_sink);

  return result.size() == 0 ? SourceResultType::FINISHED
                            : SourceResultType::HAVE_MORE_OUTPUT;
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

} // namespace duckdb

