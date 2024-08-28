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
#include "duckpgq/core/utils/duckpgq_barrier.hpp"
#include <algorithm>
#include <duckpgq/core/operator/event/csr_edge_creation_event.hpp>
#include <duckpgq/core/operator/event/shortest_path_event.hpp>
#include <thread>

#include <duckpgq/core/operator/event/iterative_length_event.hpp>

namespace duckpgq {

namespace core {


PhysicalPathFinding::PhysicalPathFinding(LogicalExtensionOperator &op,
                                         unique_ptr<PhysicalOperator> left,
                                         unique_ptr<PhysicalOperator> right)
    : PhysicalComparisonJoin(op, TYPE, {}, JoinType::INNER, 0) {
  children.push_back(std::move(left));
  children.push_back(std::move(right));
  expressions = std::move(op.expressions);
  auto &path_finding_op = op.Cast<LogicalPathFindingOperator>();
  mode = path_finding_op.mode;
}

void PhysicalPathFinding::GlobalCompressedSparseRow::InitializeVertex(
    int64_t v_size_) {
  lock_guard<mutex> csr_init_lock(csr_lock);
  if (initialized_v) {
    return;
  }
  v_size = v_size_ + 2;
  try {
    v = new std::atomic<int64_t>[v_size];
  } catch (std::bad_alloc const &) {
    throw InternalException(
        "Unable to initialize vector of size for csr vertex table "
        "representation");
  }
  for (idx_t i = 0; i < v_size; ++i) {
    v[i].store(0);
  }
  initialized_v = true;
}
void PhysicalPathFinding::GlobalCompressedSparseRow::InitializeEdge(
    int64_t e_size) {
  lock_guard<mutex> csr_init_lock(csr_lock);
  if (initialized_e) {
    return;
  }
  try {
    e.resize(e_size, 0);
    edge_ids.resize(e_size, 0);
  } catch (std::bad_alloc const &) {
    throw InternalException("Unable to initialize vector of size for csr "
                            "edge table representation");
  }
  for (idx_t i = 1; i < v_size; i++) {
    v[i] += v[i - 1];
  }
  initialized_e = true;
}

GlobalBFSState::GlobalBFSState(shared_ptr<GlobalCompressedSparseRow> csr_,
               shared_ptr<DataChunk> pairs_, int64_t v_size_,
               idx_t num_threads_, idx_t mode_, ClientContext &context_)
    : csr(std::move(csr_)), pairs(pairs_), iter(1), v_size(v_size_), change(false),
      started_searches(0), total_len(0), context(context_), seen(v_size_),
      visit1(v_size_), visit2(v_size_), num_threads(num_threads_),
      element_locks(v_size_),
      mode(mode_), parents_ve(v_size_) {
  result.Initialize(
      context, {LogicalType::BIGINT, LogicalType::LIST(LogicalType::BIGINT)},
      pairs_->size());
  auto &src_data = pairs->data[0];
  auto &dst_data = pairs->data[1];
  src_data.ToUnifiedFormat(pairs->size(), vdata_src);
  dst_data.ToUnifiedFormat(pairs->size(), vdata_dst);
  src = FlatVector::GetData<int64_t>(src_data);
  dst = FlatVector::GetData<int64_t>(dst_data);

  CreateTasks();
  size_t number_of_threads_to_schedule = std::min(num_threads, (idx_t)global_task_queue.size());
  barrier = make_uniq<Barrier>(number_of_threads_to_schedule);
}

void GlobalBFSState::Clear() {
  iter = 1;
  active = 0;
  change = false;
  // empty visit vectors
  for (auto i = 0; i < v_size; i++) {
    visit1[i] = 0;
    if (mode == 1) {
      for (auto j = 0; j < LANE_LIMIT; j++) {
        parents_ve[i][j] = {-1, -1};
      }
    }
  }
}

void GlobalBFSState::CreateTasks() {
  // workerTasks[workerId] = [task1, task2, ...]
  // vector<vector<pair<idx_t, idx_t>>> worker_tasks(num_threads);
  // auto cur_worker = 0;
  int64_t *v = (int64_t *)csr->v;
  int64_t current_task_edges = 0;
  idx_t current_task_start = 0;
  for (idx_t v_idx = 0; v_idx < (idx_t)v_size; v_idx++) {
    auto number_of_edges = v[v_idx + 1] - v[v_idx];
    if (current_task_edges + number_of_edges > split_size) {
      global_task_queue.push_back({current_task_start, v_idx});
      current_task_start = v_idx;
      current_task_edges = 0; // reset
    }

    current_task_edges += number_of_edges;
  }

  // Final task if there are any remaining edges
  if (current_task_start < (idx_t)v_size) {
    global_task_queue.push_back({current_task_start, v_size});
  }
}


optional_ptr<pair<idx_t, idx_t>> GlobalBFSState::FetchTask() {
  std::unique_lock<std::mutex> lock(queue_mutex);  // Lock the mutex to access the queue

  // Wait until the queue is not empty or some other condition to continue
  queue_cv.wait(lock, [this]() { return !global_task_queue.empty(); });

  // If all tasks are processed, return an empty optional
  if (current_task_index >= global_task_queue.size()) {
    return nullptr;
  }

  // Fetch the task using the current index
  auto task = global_task_queue[current_task_index];
  current_task_index++;  // Move to the next task

  return task;
}

pair<idx_t, idx_t> GlobalBFSState::BoundaryCalculation(idx_t worker_id) const {
  idx_t block_size = ceil((double)v_size / num_threads);
  block_size = block_size == 0 ? 1 : block_size;
  idx_t left = block_size * worker_id;
  idx_t right = std::min(block_size * (worker_id + 1), (idx_t)v_size);
  return {left, right};
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
PathFindingLocalState::PathFindingLocalState(ClientContext &context, const PhysicalPathFinding &op,
                        const idx_t child)
      : local_tasks(context, op.children[0]->GetTypes()),
        local_inputs(context, op.children[1]->GetTypes()) {}

void PathFindingLocalState::Sink(DataChunk &input, GlobalCompressedSparseRow &global_csr,
          idx_t child) {
  if (child == 1) {
    // Add the tasks (src, dst) to sink
    // Optimizations: Eliminate duplicate sources/destinations
    local_tasks.Append(input);
    local_tasks.Print();
  } else {
    // Create CSR
    local_inputs.Append(input);
    CreateCSRVertex(input, global_csr);
  }
}

void PathFindingLocalState::CreateCSRVertex(
    DataChunk &input, GlobalCompressedSparseRow &global_csr) {
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
}

PathFindingGlobalState::PathFindingGlobalState(ClientContext &context,
                       const PhysicalPathFinding &op) {
  global_tasks =
      make_uniq<ColumnDataCollection>(context, op.children[0]->GetTypes());
  global_inputs =
      make_uniq<ColumnDataCollection>(context, op.children[1]->GetTypes());
  global_csr = make_uniq<GlobalCompressedSparseRow>(context);
  child = 0;
  mode = op.mode;
}

PathFindingGlobalState::PathFindingGlobalState(PathFindingGlobalState &prev)
    : GlobalSinkState(prev), global_tasks(std::move(prev.global_tasks)),
      global_inputs(std::move(prev.global_inputs)),
      global_csr(std::move(prev.global_csr)),
      global_bfs_state(std::move(prev.global_bfs_state)),
      child(prev.child + 1), mode(prev.mode) {}

void PathFindingGlobalState::Sink(DataChunk &input, PathFindingLocalState &lstate) {
  lstate.Sink(input, *global_csr, child);
}

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
  return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalPathFinding::Combine(ExecutionContext &context,
                             OperatorSinkCombineInput &input) const {
  auto &gstate = input.global_state.Cast<PathFindingGlobalState>();
  auto &lstate = input.local_state.Cast<PathFindingLocalState>();
  auto &client_profiler = QueryProfiler::Get(context.client);

  gstate.global_tasks->Combine(lstate.local_tasks);
  gstate.global_inputs->Combine(lstate.local_inputs);
  client_profiler.Flush(context.thread.profiler);
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
  auto &csr = gstate.global_csr;
  auto &global_tasks = gstate.global_tasks;

  if (gstate.child == 0) {
    auto csr_event = make_shared_ptr<CSREdgeCreationEvent>(gstate, pipeline);
    event.InsertEvent(std::move(csr_event));
  } else if (gstate.child == 1 && global_tasks->Count() > 0) {
    auto all_pairs = make_shared_ptr<DataChunk>();
    DataChunk pairs;
    global_tasks->InitializeScanChunk(*all_pairs);
    global_tasks->InitializeScanChunk(pairs);
    ColumnDataScanState scan_state;
    global_tasks->InitializeScan(scan_state);
    while (global_tasks->Scan(scan_state, pairs)) {
      all_pairs->Append(pairs, true);
    }

    auto &ts = TaskScheduler::GetScheduler(context);
    idx_t num_threads = ts.NumberOfThreads();
    idx_t mode = this->mode == "iterativelength" ? 0 : 1;
    gstate.global_bfs_state = make_uniq<GlobalBFSState>(
          csr, all_pairs, csr->v_size - 2, num_threads, mode, context);

    Value task_size_value;
    context.TryGetCurrentSetting("experimental_path_finding_operator_task_size",
                                 task_size_value);
    gstate.global_bfs_state->split_size = task_size_value.GetValue<idx_t>();
    ;

    // Schedule the first round of BFS tasks
    if (all_pairs->size() > 0) {
      ScheduleBFSEvent(pipeline, event, gstate);
    }
  }

  // Move to the next input child
  ++gstate.child;

  return SinkFinalizeType::READY;
}

void PhysicalPathFinding::ScheduleBFSEvent(Pipeline &pipeline, Event &event,
                                           GlobalSinkState &state) {
  auto &gstate = state.Cast<PathFindingGlobalState>();
  auto &bfs_state = gstate.global_bfs_state;

  // for every batch of pairs, schedule a BFS task
  bfs_state->Clear();

  // remaining pairs
  if (bfs_state->started_searches < gstate.global_tasks->Count()) {
    auto result_data = FlatVector::GetData<int64_t>(bfs_state->result.data[0]);
    auto &result_validity = FlatVector::Validity(bfs_state->result.data[0]);
    std::bitset<LANE_LIMIT> seen_mask;
    seen_mask.set();

    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      bfs_state->lane_to_num[lane] = -1;
      while (bfs_state->started_searches < gstate.global_tasks->Count()) {
        int64_t search_num = bfs_state->started_searches++;
        int64_t src_pos = bfs_state->vdata_src.sel->get_index(search_num);
        int64_t dst_pos = bfs_state->vdata_dst.sel->get_index(search_num);
        if (!bfs_state->vdata_src.validity.RowIsValid(src_pos)) {
          result_validity.SetInvalid(search_num);
          result_data[search_num] = (uint64_t)-1; /* no path */
        } else if (bfs_state->src[src_pos] == bfs_state->dst[dst_pos]) {
          result_data[search_num] =
              (uint64_t)0; // path of length 0 does not require a search
        } else {
          bfs_state->visit1[bfs_state->src[src_pos]][lane] = true;
          bfs_state->lane_to_num[lane] = search_num; // active lane
          bfs_state->active++;
          seen_mask[lane] = false;
          break;
        }
      }
    }
    for (int64_t i = 0; i < bfs_state->v_size; i++) {
      bfs_state->seen[i] = seen_mask;
    }

    if (gstate.mode == "iterativelength") {
      auto bfs_event =
          make_shared_ptr<ParallelIterativeEvent>(gstate, pipeline);
      event.InsertEvent(std::move(bfs_event));
    } else if (gstate.mode == "shortestpath") {
      auto bfs_event =
          make_shared_ptr<ParallelShortestPathEvent>(gstate, pipeline);
      event.InsertEvent(std::move(bfs_event));
    }
  }
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
                   PathFindingLocalSourceState &lstate) {}

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
  auto results_start_time = std::chrono::high_resolution_clock::now();
  auto &pf_sink = sink_state->Cast<PathFindingGlobalState>();
  auto &pf_bfs_state = pf_sink.global_bfs_state;
  if (pf_bfs_state->pairs->size() == 0) {
    return SourceResultType::FINISHED;
  }
  pf_bfs_state->result.SetCardinality(*pf_bfs_state->pairs);

  result.Move(*pf_bfs_state->pairs);
  auto result_path = make_uniq<DataChunk>();
  //! Split off the path from the path length, and then fuse into the result
  pf_bfs_state->result.Split(*result_path, 1);
  if (pf_sink.mode == "iterativelength") {
    result.Fuse(pf_bfs_state->result);
  } else if (pf_sink.mode == "shortestpath") {
    result.Fuse(*result_path);
  } else {
    throw NotImplementedException("Unrecognized mode for Path Finding");
  }

  // result.Print();
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

} // namespace core
} // namespace duckpgq
