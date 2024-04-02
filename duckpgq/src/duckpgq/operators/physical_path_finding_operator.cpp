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
      // input.Print();
      local_tasks.Append(input);
      // local_tasks.Print();
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
  // global_csr.Print(); // Debug print
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

  // pairs is a 2-column table with src and dst
  ColumnDataCollection global_tasks;
  // pairs with path exists
  // ColumnDataCollection global_results;
  ColumnDataScanState scan_state;
  ColumnDataAppendState append_state;

  unique_ptr<GlobalCompressedSparseRow> global_csr;
  size_t child;
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
  // gstate.global_tasks.Print();
  return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

static bool IterativeLength(int64_t v_size, int64_t *v, vector<int64_t> &e,
                            vector<std::bitset<LANE_LIMIT>> &seen,
                            vector<std::bitset<LANE_LIMIT>> &visit,
                            vector<std::bitset<LANE_LIMIT>> &next) {
  bool change = false;
  for (auto i = 0; i < v_size; i++) {
    next[i] = 0;
  }
  for (auto i = 0; i < v_size; i++) {
    if (visit[i].any()) {
      for (auto offset = v[i]; offset < v[i + 1]; offset++) {
        auto n = e[offset];
        next[n] = next[n] | visit[i];
      }
    }
  }
  for (auto i = 0; i < v_size; i++) {
    next[i] = next[i] & ~seen[i];
    seen[i] = seen[i] | next[i];
    change |= next[i].any();
  }
  return change;
}

static void IterativeLengthFunction(const unique_ptr<PathFindingGlobalState::GlobalCompressedSparseRow> &csr, 
                                    DataChunk &pairs, Vector &result) {
  int64_t v_size = csr->v_size;
  int64_t *v = (int64_t *)csr->v;
  vector<int64_t> &e = csr->e;

  // get src and dst vectors for searches
  auto &src = pairs.data[0];
  auto &dst = pairs.data[1];
  UnifiedVectorFormat vdata_src;
  UnifiedVectorFormat vdata_dst;
  src.ToUnifiedFormat(pairs.size(), vdata_src);
  dst.ToUnifiedFormat(pairs.size(), vdata_dst);

  auto src_data = FlatVector::GetData<int64_t>(src);
  auto dst_data = FlatVector::GetData<int64_t>(dst);

  ValidityMask &result_validity = FlatVector::Validity(result);

  // create result vector
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<int64_t>(result);

  // create temp SIMD arrays
  vector<std::bitset<LANE_LIMIT>> seen(v_size);
  vector<std::bitset<LANE_LIMIT>> visit1(v_size);
  vector<std::bitset<LANE_LIMIT>> visit2(v_size);

  // maps lane to search number
  short lane_to_num[LANE_LIMIT];
  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    lane_to_num[lane] = -1; // inactive
  }

  idx_t started_searches = 0;
  while (started_searches < pairs.size()) {

    // empty visit vectors
    for (auto i = 0; i < v_size; i++) {
      seen[i] = 0;
      visit1[i] = 0;
    }

    // add search jobs to free lanes
    uint64_t active = 0;
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      lane_to_num[lane] = -1;
      while (started_searches < pairs.size()) {
        int64_t search_num = started_searches++;
        int64_t src_pos = vdata_src.sel->get_index(search_num);
        int64_t dst_pos = vdata_dst.sel->get_index(search_num);
        if (!vdata_src.validity.RowIsValid(src_pos)) {
          result_validity.SetInvalid(search_num);
          result_data[search_num] = (uint64_t)-1; /* no path */
        } else if (src_data[src_pos] == dst_data[dst_pos]) {
          result_data[search_num] =
              (uint64_t)0; // path of length 0 does not require a search
        } else {
          visit1[src_data[src_pos]][lane] = true;
          lane_to_num[lane] = search_num; // active lane
          active++;
          break;
        }
      }
    }

    // make passes while a lane is still active
    for (int64_t iter = 1; active; iter++) {
      if (!IterativeLength(v_size, v, e, seen, (iter & 1) ? visit1 : visit2,
                           (iter & 1) ? visit2 : visit1)) {
        break;
      }
      // detect lanes that finished
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        int64_t search_num = lane_to_num[lane];
        if (search_num >= 0) { // active lane
          int64_t dst_pos = vdata_dst.sel->get_index(search_num);
          if (seen[dst_data[dst_pos]][lane]) {
            result_data[search_num] =
                iter;               /* found at iter => iter = path length */
            lane_to_num[lane] = -1; // mark inactive
            active--;
          }
        }
      }
    }

    // no changes anymore: any still active searches have no path
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = lane_to_num[lane];
      if (search_num >= 0) { // active lane
        result_validity.SetInvalid(search_num);
        result_data[search_num] = (int64_t)-1; /* no path */
        lane_to_num[lane] = -1;                // mark inactive
      }
    }
  }
}


SinkFinalizeType
PhysicalPathFinding::Finalize(Pipeline &pipeline, Event &event,
                              ClientContext &context,
                              OperatorSinkFinalizeInput &input) const {
  auto &gstate = input.global_state.Cast<PathFindingGlobalState>();
  auto &csr = gstate.global_csr;
  auto &global_tasks = gstate.global_tasks;
  if (global_tasks.Count() != 0) {
    DataChunk pairs;
    global_tasks.InitializeScanChunk(pairs);
    ColumnDataScanState scan_state;
    global_tasks.InitializeScan(scan_state);
    while (global_tasks.Scan(scan_state, pairs)) {
      Vector result(LogicalType::BIGINT, true, true);
      IterativeLengthFunction(csr, pairs, result);
      // debug print
      Printer::Print(result.ToString(pairs.size()));
    }
  }

	// Move to the next input child
	++gstate.child;

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
    // const auto &sink_state = (op.sink_state->Cast<PathFindingGlobalState>());
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
  // auto &pf_lstate = input.local_state.Cast<PathFindingLocalSourceState>();
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

