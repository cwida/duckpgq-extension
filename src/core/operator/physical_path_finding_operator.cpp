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

GlobalBFSState::GlobalBFSState(shared_ptr<DataChunk> pairs_, CSR* csr_, int64_t vsize_,
               idx_t num_threads_, string mode_, ClientContext &context_)
    : pairs(pairs_), iter(1), csr(csr_), v_size(vsize_), change(false),
      started_searches(0), total_len(0), context(context_), seen(vsize_),
      visit1(vsize_), visit2(vsize_), num_threads(num_threads_),
      element_locks(vsize_),
      mode(std::move(mode_)), parents_ve(vsize_) {
  if (mode == "iterativelength") { // length
    result.Initialize(
        context, {LogicalType::BIGINT},
        pairs_->size());
  } else if (mode == "shortestpath") { // path
    result.Initialize(
      context, {LogicalType::LIST(LogicalType::BIGINT)},
      pairs_->size());
  } else {
    throw NotImplementedException("Mode not supported");
  }
  result.SetCardinality(pairs_->size());

  auto &src_data = pairs->data[0];
  auto &dst_data = pairs->data[1];
  src_data.ToUnifiedFormat(pairs->size(), vdata_src);
  dst_data.ToUnifiedFormat(pairs->size(), vdata_dst);
  src = FlatVector::GetData<int64_t>(src_data);
  dst = FlatVector::GetData<int64_t>(dst_data);
  result.SetCapacity(pairs->size());

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
    if (mode == "shortestpath") {
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
  // std::cout << "Set the number of tasks to " << global_task_queue.size() << std::endl;
}


shared_ptr<std::pair<idx_t, idx_t>> GlobalBFSState::FetchTask() {
  std::unique_lock<std::mutex> lock(queue_mutex);  // Lock the mutex to access the queue

  // Log entry into FetchTask
  // std::cout << "FetchTask: Checking tasks. Current index: " << current_task_index
  //           << ", Total tasks: " << global_task_queue.size() << std::endl;

  // Avoid unnecessary waiting if no tasks are available
  if (current_task_index >= global_task_queue.size()) {
    // std::cout << "FetchTask: No more tasks available. Exiting." << std::endl;
    return nullptr;  // No more tasks
  }

  // Wait until a task is available or the queue is finalized
  queue_cv.wait(lock, [this]() {
    return current_task_index < global_task_queue.size();
  });

  // Fetch the next task and increment the task index
  if (current_task_index < global_task_queue.size()) {
    auto task = make_shared_ptr<std::pair<idx_t, idx_t>>(global_task_queue[current_task_index]);
    current_task_index++;

    // Log the fetched task
    // std::cout << "FetchTask: Fetched task " << current_task_index - 1
              // << " -> [" << task->first << ", " << task->second << "]" << std::endl;

    return task;
  }

  // Log no tasks available after wait
  // std::cout << "FetchTask: No more tasks available after wait. Exiting." << std::endl;
  return nullptr;
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

  auto result_types = op.children[0]->GetTypes();
  if (op.mode == "iterativelength") {
    result_types.push_back(LogicalType::BIGINT);
  } else {
    result_types.push_back(LogicalType::LIST(LogicalType::BIGINT));
  }
  results = make_uniq<ColumnDataCollection>(context, result_types);
  child = 0;
  mode = op.mode;
  pairs_processed = 0;
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

  while (gstate.pairs_processed < global_tasks->Count()) {
    DataChunk pairs_to_process;
    ColumnDataScanState scan_state;
    global_tasks->Scan(scan_state, pairs_to_process);

    auto global_bfs_state = make_uniq<GlobalBFSState>(pairs_to_process, gstate.csr, gstate.csr->vsize-2,
      gstate.num_threads, mode, context);
    global_bfs_state->ScheduleBFSEvent(pipeline, event, gstate);
    ColumnDataAppendState append_state;
    gstate.results->Append(append_state, global_bfs_state->result);
  }

  // if (global_tasks->Count() > 0) {
  //   auto all_pairs = make_shared_ptr<DataChunk>();
  //   DataChunk pairs;
  //   global_tasks->InitializeScanChunk(*all_pairs);
  //   global_tasks->InitializeScanChunk(pairs);
  //   ColumnDataScanState scan_state;
  //   global_tasks->InitializeScan(scan_state);
  //   while (global_tasks->Scan(scan_state, pairs)) {
  //     all_pairs->Append(pairs, true);
  //   }
  //
  //   auto &ts = TaskScheduler::GetScheduler(context);
  //   idx_t num_threads = ts.NumberOfThreads();
  //   idx_t mode = this->mode == "iterativelength" ? 0 : 1;
  //   gstate.global_bfs_state = make_uniq<GlobalBFSState>(all_pairs, gstate.csr, gstate.csr->vsize-2,
  //     num_threads, mode, context);
  //
  //   Value task_size_value;
  //   context.TryGetCurrentSetting("experimental_path_finding_operator_task_size",
  //                                task_size_value);
  //   gstate.global_bfs_state->split_size = task_size_value.GetValue<idx_t>();
  //
  //   // Schedule the first round of BFS tasks
  //   if (all_pairs->size() > 0) {
  //     ScheduleBFSEvent(pipeline, event, gstate);
  //   }
  // }

  // Move to the next input child
  ++gstate.child;
  duckpgq_state->csr_to_delete.insert(gstate.csr_id);
  return SinkFinalizeType::READY;
}

void GlobalBFSState::ScheduleBFSEvent(Pipeline &pipeline, Event &event,
                                           GlobalSinkState &state) {
  auto &gstate = state.Cast<PathFindingGlobalSinkState>();
  // remaining pairs
  if (started_searches < gstate.global_pairs->Count()) {
    auto &result_validity = FlatVector::Validity(result.data[0]);
    std::bitset<LANE_LIMIT> seen_mask;
    seen_mask.set();

    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      lane_to_num[lane] = -1;
      while (started_searches < gstate.global_pairs->Count()) {
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
      auto bfs_event =
          make_shared_ptr<ParallelIterativeEvent>(gstate, pipeline, *this);
      event.InsertEvent(std::move(bfs_event));
    } else if (gstate.mode == "shortestpath") {
      auto bfs_event =
          make_shared_ptr<ShortestPathEvent>(gstate, pipeline, *this);
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

SourceResultType PhysicalPathFinding::GetData(ExecutionContext &context, DataChunk &result,
                             OperatorSourceInput &input) const {
  auto &pf_sink = sink_state->Cast<PathFindingGlobalSinkState>();

  // If there are no pairs, we're done
  if (pf_sink.results->Count() == 0) {
    return SourceResultType::FINISHED;
  }

  // // Track the current offset to handle batches larger than STANDARD_VECTOR_SIZE
  // static idx_t current_offset = 0;
  //
  // // Determine the number of tuples to process in this call
  // auto tuples_to_copy = std::min<idx_t>(STANDARD_VECTOR_SIZE, pf_bfs_state->pairs->size() - current_offset);
  // // If there are no tuples left, we're finished
  // if (tuples_to_copy == 0) {
  //   current_offset = 0; // Reset for future calls
  //   return SourceResultType::FINISHED;
  // }
  //
  // std::cout << "Current offset: " << current_offset
  //         << ", Tuples to copy: " << tuples_to_copy
  //         << ", Pairs size: " << pf_bfs_state->pairs->size()
  //         << ", Result size: " << pf_bfs_state->result.size() << std::endl;
  //
  // // Slice the pairs and result data to the appropriate size for this batch
  // DataChunk temp_pairs;
  // temp_pairs.Initialize(context.client, pf_bfs_state->pairs->GetTypes());
  // pf_bfs_state->pairs->Copy(temp_pairs, current_offset);
  //
  // DataChunk temp_result;
  // temp_result.Initialize(context.client, pf_bfs_state->result.GetTypes());
  // pf_bfs_state->result.Copy(temp_result, current_offset);
  //
  // // Move the sliced data into the result DataChunk
  // result.SetCapacity(tuples_to_copy);
  // result.Move(temp_pairs);
  // result.Fuse(temp_result);
  // result.SetCardinality(tuples_to_copy);
  //
  // // Update the current offset
  // current_offset += tuples_to_copy;

  // Return appropriate status based on whether there are more tuples to process
  // return current_offset >= pf_bfs_state->pairs->size() ? SourceResultType::FINISHED
                                                       // : SourceResultType::HAVE_MORE_OUTPUT;
  return SourceResultType::FINISHED;
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
