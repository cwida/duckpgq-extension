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

GlobalBFSState::GlobalBFSState(shared_ptr<DataChunk> pairs_, int64_t v_size_,
               idx_t num_threads_, idx_t mode_, ClientContext &context_)
    : pairs(pairs_), iter(1), v_size(v_size_), change(false),
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


shared_ptr<pair<idx_t, idx_t>> GlobalBFSState::FetchTask() {
  std::unique_lock<std::mutex> lock(queue_mutex);  // Lock the mutex to access the queue

  // Check if there are no more tasks to process
  if (current_task_index >= global_task_queue.size()) {
    return nullptr;  // No more tasks, return immediately
  }

  // Wait until the queue is not empty or some other condition to continue
  queue_cv.wait(lock, [this]() { return current_task_index < global_task_queue.size(); });

  // If all tasks are processed, return an empty optional
  if (current_task_index >= global_task_queue.size()) {
    return nullptr;
  }

  // Fetch the task using the current index
  auto task = make_shared_ptr<pair<idx_t, idx_t>>(global_task_queue[current_task_index]);
  current_task_index++;  // Move to the next task

  return task;
}

void GlobalBFSState::ResetTaskIndex() {
  std::lock_guard<std::mutex> lock(queue_mutex);  // Lock to reset index safely
  current_task_index = 0;  // Reset the task index for the next stage
  queue_cv.notify_all();  // Notify all threads that tasks are available
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
}

void PathFindingGlobalSinkState::Sink(DataChunk &input, PathFindingLocalSinkState &lstate) {
  if (child == 0) {
    // CSR phase
    int32_t csr_id = input.GetValue(0, 0).GetValue<int64_t>();
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
  if (gstate.child == 0) {
    std::cout << "Sink phase CSR" << std::endl;
  } else {
    std::cout << "Sink phase PF pairs" << std::endl;
  }
  chunk.Print();
  gstate.Sink(chunk, lstate);
  return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType
PhysicalPathFinding::Combine(ExecutionContext &context,
                             OperatorSinkCombineInput &input) const {
  auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
  auto &lstate = input.local_state.Cast<PathFindingLocalSinkState>();
  auto &client_profiler = QueryProfiler::Get(context.client);

  gstate.global_pairs->Combine(lstate.local_pairs);
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
  auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
  auto &global_tasks = gstate.global_pairs;
  auto duckpgq_state = GetDuckPGQState(context);
  // TODO
  auto csr = duckpgq_state->GetCSR(0);
  // Check if we have to do anything for CSR child


  if (gstate.child == 1 && global_tasks->Count() > 0) {
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
    // TODO
    gstate.global_bfs_state = make_uniq<GlobalBFSState>(all_pairs, 0,
      num_threads, mode, context);

    Value task_size_value;
    context.TryGetCurrentSetting("experimental_path_finding_operator_task_size",
                                 task_size_value);
    gstate.global_bfs_state->split_size = task_size_value.GetValue<idx_t>();

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
                                           GlobalSinkState &state) const {
  auto &gstate = state.Cast<PathFindingGlobalSinkState>();
  auto &bfs_state = gstate.global_bfs_state;

  // for every batch of pairs, schedule a BFS task
  bfs_state->Clear();

  // remaining pairs
  if (bfs_state->started_searches < gstate.global_pairs->Count()) {
    auto result_data = FlatVector::GetData<int64_t>(bfs_state->result.data[0]);
    auto &result_validity = FlatVector::Validity(bfs_state->result.data[0]);
    std::bitset<LANE_LIMIT> seen_mask;
    seen_mask.set();

    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      bfs_state->lane_to_num[lane] = -1;
      while (bfs_state->started_searches < gstate.global_pairs->Count()) {
        int64_t search_num = bfs_state->started_searches++;
        int64_t src_pos = bfs_state->vdata_src.sel->get_index(search_num);
        if (!bfs_state->vdata_src.validity.RowIsValid(src_pos)) {
          result_validity.SetInvalid(search_num);
          result_data[search_num] = (uint64_t)-1; /* no path */
        } else {
          bfs_state->visit1[bfs_state->src[src_pos]][lane] = true;
          // bfs_state->seen[bfs_state->src[src_pos]][lane] = true;
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
          make_shared_ptr<ParallelIterativeEvent>(gstate, pipeline, *this);
      event.InsertEvent(std::move(bfs_event));
    } else if (gstate.mode == "shortestpath") {
      auto bfs_event =
          make_shared_ptr<ParallelShortestPathEvent>(gstate, pipeline, *this);
      event.InsertEvent(std::move(bfs_event));
    }
  }
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

  void Initialize(PathFindingGlobalState &sink_state) {
    lock_guard<mutex> initializing(lock);
    if (initialized) {
      return;
    }
    initialized = true;
  }

public:
  idx_t MaxThreads() override {
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
  auto &pf_sink = sink_state->Cast<PathFindingGlobalSinkState>();
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
