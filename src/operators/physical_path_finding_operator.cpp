#include "duckpgq/operators/physical_path_finding_operator.hpp"

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <duckpgq/operators/logical_path_finding_operator.hpp>
#include <pthread.h>
#include <numeric>
#include <thread>

namespace duckdb {

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
    reverse_v = new std::atomic<int64_t>[v_size];
  } catch (std::bad_alloc const &) {
    throw InternalException(
        "Unable to initialize vector of size for csr vertex table "
        "representation");
  }
  for (idx_t i = 0; i < v_size; ++i) {
    v[i].store(0);
    reverse_v[i].store(0);
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
    reverse_e.resize(e_size, 0);
    edge_ids.resize(e_size, 0);
    reverse_edge_ids.resize(e_size, 0);
  } catch (std::bad_alloc const &) {
    throw InternalException("Unable to initialize vector of size for csr "
                            "edge table representation");
  }
  for (idx_t i = 1; i < v_size; i++) {
    v[i] += v[i - 1];
    reverse_v[i] += reverse_v[i - 1];
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
                        const idx_t child) : local_tasks(context, op.children[0]->GetTypes()),
                        local_inputs(context, op.children[1]->GetTypes()) {
  }

  void Sink(DataChunk &input, GlobalCompressedSparseRow &global_csr, idx_t child) {
    if (child == 1) {
      // Add the tasks (src, dst) to sink
      // Optimizations: Eliminate duplicate sources/destinations
      // input.Print();
      local_tasks.Append(input);
      // local_tasks.Print();
    } else {
      // Create CSR
      local_inputs.Append(input);
      CreateCSRVertex(input, global_csr);
    }
  }

  static void CreateCSRVertex(DataChunk &input,
                        GlobalCompressedSparseRow &global_csr);

  ColumnDataCollection local_tasks;
  ColumnDataCollection local_inputs;
};

void PathFindingLocalState::CreateCSRVertex(DataChunk &input,
                                      GlobalCompressedSparseRow &global_csr) {
  if (!global_csr.initialized_v) {
    const auto v_size = input.data[9].GetValue(0).GetValue<int64_t>();
    global_csr.InitializeVertex(v_size);
  }
  auto result = Vector(LogicalTypeId::BIGINT);
  BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
      input.data[4], input.data[3], result, input.size(),
      [&](const int64_t src, const int64_t cnt) {
        int64_t edge_count = 0;
        global_csr.v[src + 2] = cnt;
        edge_count = edge_count + cnt;
        return edge_count;
      });
  BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
      input.data[7], input.data[6], result, input.size(),
      [&](const int64_t dst, const int64_t cnt) {
        int64_t edge_count = 0;
        global_csr.reverse_v[dst + 2] = cnt;
        edge_count = edge_count + cnt;
        return edge_count;
      });
}

class Barrier {
public:
  explicit Barrier(std::size_t iCount) :
    mThreshold(iCount),
    mCount(iCount),
    mGeneration(0) {
  }

  void Wait() {
    std::unique_lock<std::mutex> lLock{mMutex};
    auto lGen = mGeneration.load();
    if (!--mCount) {
        mGeneration++;
        mCount = mThreshold;
        mCond.notify_all();
    } else {
        mCond.wait(lLock, [this, lGen] { return lGen != mGeneration; });
    }
  }

private:
  std::mutex mMutex;
  std::condition_variable mCond;
  std::size_t mThreshold;
  std::atomic<std::size_t> mCount;
  std::atomic<std::size_t> mGeneration;
};

class GlobalBFSState {
  using GlobalCompressedSparseRow =
      PhysicalPathFinding::GlobalCompressedSparseRow;
public:
  GlobalBFSState(shared_ptr<GlobalCompressedSparseRow> csr_, shared_ptr<DataChunk> pairs_, int64_t v_size_, 
                idx_t num_threads_, idx_t mode_, ClientContext &context_)
      : csr(csr_), pairs(pairs_), iter(1), v_size(v_size_), change(false), 
        started_searches(0), total_len(0), context(context_), seen(v_size_), visit1(v_size_), visit2(v_size_),
        top_down_cost(0), bottom_up_cost(0), is_top_down(true), num_threads(num_threads_), task_queues(num_threads_), 
        task_queues_reverse(num_threads_), barrier(num_threads_), locks(v_size_), mode(mode_) {
    result.Initialize(context, {LogicalType::BIGINT, LogicalType::LIST(LogicalType::BIGINT)}, pairs_->size());
    auto &src_data = pairs->data[0];
    auto &dst_data = pairs->data[1];
    src_data.ToUnifiedFormat(pairs->size(), vdata_src);
    dst_data.ToUnifiedFormat(pairs->size(), vdata_dst);
    src = FlatVector::GetData<int64_t>(src_data);
    dst = FlatVector::GetData<int64_t>(dst_data);

    if (mode == 1) {
      parents_v.resize(v_size_, std::vector<int64_t>(LANE_LIMIT));
      parents_e.resize(v_size_, std::vector<int64_t>(LANE_LIMIT));
    }

    CreateTasks();
  }

  void Clear() {
    iter = 1;
    active = 0;
    change = false;
    // empty visit vectors
    for (auto i = 0; i < v_size; i++) {
      visit1[i] = 0;
      if (mode == 1) {
        for (auto j = 0; j < LANE_LIMIT; j++) {
          parents_v[i][j] = -1;
          parents_e[i][j] = -1;
        }
      }
    }
  }


  void CreateTasks() {
    // workerTasks[workerId] = [task1, task2, ...]
    auto queues = {&task_queues, &task_queues_reverse};
    is_top_down = true;
    for (auto& queue : queues) {
      vector<vector<pair<idx_t, idx_t>>> worker_tasks(num_threads);
      auto cur_worker = 0;
      int64_t *v = is_top_down ? (int64_t*)csr->v : (int64_t*)csr->reverse_v;
      int64_t current_task_edges = 0;
      idx_t current_task_start = 0;
      for (idx_t i = 0; i < (idx_t)v_size; i++) {
        auto vertex_edges = v[i + 1] - v[i];
        if (current_task_edges + vertex_edges > split_size && i != current_task_start) {
          auto worker_id = cur_worker % num_threads;
          pair<idx_t, idx_t> range = {current_task_start, i};
          worker_tasks[worker_id].push_back(range);
          current_task_start = i;
          current_task_edges = 0;
          cur_worker++;
        }
        current_task_edges += vertex_edges;
      }
      if (current_task_start < (idx_t)v_size) {
        auto worker_id = cur_worker % num_threads;
        pair<idx_t, idx_t> range = {current_task_start, v_size};
        worker_tasks[worker_id].push_back(range);
      }
      for (idx_t worker_id = 0; worker_id < num_threads; worker_id++) {
        queue->at(worker_id).first.store(0);
        queue->at(worker_id).second = worker_tasks[worker_id];
      }
      is_top_down = false;
    }
    is_top_down = true;
  }

  void InitTask(idx_t worker_id) {
    if (is_top_down) {
      task_queues[worker_id].first.store(0);
    } else {
      task_queues_reverse[worker_id].first.store(0);
    }
  }

  pair<idx_t, idx_t> FetchTask(idx_t worker_id) {
    auto& task_queue = is_top_down ? task_queues : task_queues_reverse;
    idx_t offset = 0;
    do {
      auto worker_idx = (worker_id + offset) % task_queue.size();
      auto cur_task_ix = task_queue[worker_idx].first.fetch_add(1);
      if (cur_task_ix < task_queue[worker_idx].second.size()) {
        return task_queue[worker_idx].second[cur_task_ix];
      } else {
        offset++;
      }
    } while (offset < task_queue.size());
    return {0, 0};
  }

  pair<idx_t, idx_t> BoundaryCalculation(idx_t worker_id) {
    idx_t block_size = ceil((double)v_size / num_threads);
    block_size = block_size == 0 ? 1 : block_size;
    idx_t left = block_size * worker_id;
    idx_t right = std::min(block_size * (worker_id + 1), (idx_t)v_size);
    return {left, right};
  }

  void DirectionSwitch() {
    // Determine the switch of algorithms
    // debug print
    if (top_down_cost * alpha > bottom_up_cost) {
      is_top_down = false;
    } else {
      is_top_down = true;
    }
    change = top_down_cost ? true : false;
    // clear the counters after the switch
    top_down_cost = 0;
    bottom_up_cost = 0;
  }

public:
  shared_ptr<GlobalCompressedSparseRow> csr;
  shared_ptr<DataChunk> pairs;
  int64_t iter;
  int64_t v_size;
  bool change;
  idx_t started_searches;
  int64_t total_len;
  int64_t *src;
  int64_t *dst;
  UnifiedVectorFormat vdata_src;
  UnifiedVectorFormat vdata_dst;
  int64_t lane_to_num[LANE_LIMIT];
  idx_t active = 0;
  DataChunk result; // 0 for length, 1 for path
  ClientContext& context;
  vector<std::bitset<LANE_LIMIT>> seen;
  vector<std::bitset<LANE_LIMIT>> visit1;
  vector<std::bitset<LANE_LIMIT>> visit2;
  vector<std::vector<int64_t>> parents_v;
  vector<std::vector<int64_t>> parents_e;

  atomic<int64_t> top_down_cost;
  atomic<int64_t> bottom_up_cost;
  double alpha = 1;
  atomic<bool> is_top_down;

  idx_t num_threads;
  // task_queues[workerId] = {curTaskIx, queuedTasks}
  // queuedTasks[curTaskIx] = {start, end}
  vector<pair<atomic<idx_t>, vector<pair<idx_t, idx_t>>>> task_queues;
  vector<pair<atomic<idx_t>, vector<pair<idx_t, idx_t>>>> task_queues_reverse;
  int64_t split_size = 256;

  Barrier barrier;

  // lock for next
  mutable vector<mutex> locks;

  idx_t mode;
};

class PathFindingGlobalState : public GlobalSinkState {
public:
  using GlobalCompressedSparseRow =
      PhysicalPathFinding::GlobalCompressedSparseRow;
  PathFindingGlobalState(ClientContext &context,
                         const PhysicalPathFinding &op) {
    global_tasks = make_uniq<ColumnDataCollection>(context, op.children[0]->GetTypes());
    global_inputs = make_uniq<ColumnDataCollection>(context, op.children[1]->GetTypes());
    global_csr = make_uniq<GlobalCompressedSparseRow>(context);
    child = 0;
    mode = op.mode;
  }

  PathFindingGlobalState(PathFindingGlobalState &prev)
      : GlobalSinkState(prev), global_tasks(std::move(prev.global_tasks)),
      global_inputs(std::move(prev.global_inputs)),
      global_csr(std::move(prev.global_csr)), 
      global_bfs_state(std::move(prev.global_bfs_state)), child(prev.child + 1), mode(prev.mode) {
  }

  void Sink(DataChunk &input, PathFindingLocalState &lstate) {
    lstate.Sink(input, *global_csr, child);
  }

  // pairs is a 2-column table with src and dst
  unique_ptr<ColumnDataCollection> global_tasks;
  unique_ptr<ColumnDataCollection> global_inputs;
  // pairs with path exists
  // ColumnDataCollection global_results;
  ColumnDataScanState scan_state;
  ColumnDataAppendState append_state;

  shared_ptr<GlobalCompressedSparseRow> global_csr;
  // state for BFS
  unique_ptr<GlobalBFSState> global_bfs_state;
  size_t child;
  string mode;
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

class PhysicalIterativeTask : public ExecutorTask {
public:
	PhysicalIterativeTask(shared_ptr<Event> event_p, ClientContext &context, PathFindingGlobalState &state, idx_t worker_id)
	    : ExecutorTask(context, std::move(event_p)), context(context), state(state), worker_id(worker_id) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
    auto& bfs_state = state.global_bfs_state;
    auto& change = bfs_state->change;
    auto& barrier = bfs_state->barrier;

    auto bound = bfs_state->BoundaryCalculation(worker_id);
    left = bound.first;
    right = bound.second;

    do {
      bfs_state->InitTask(worker_id);

      if (bfs_state->is_top_down) {
        IterativeLengthTopDown();
      } else {
        IterativeLengthBottomUp();
      }

      barrier.Wait();

      if (worker_id == 0) {
        ReachDetect();
        bfs_state->DirectionSwitch();
      }
      barrier.Wait();
    } while (change);

    if (worker_id == 0) {
      UnReachableSet();
    }

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
  void IterativeLengthTopDown() {
    auto& bfs_state = state.global_bfs_state;
    auto& seen = bfs_state->seen;
    auto& visit = bfs_state->iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
    auto& next = bfs_state->iter & 1 ? bfs_state->visit2 : bfs_state->visit1;
    auto& barrier = bfs_state->barrier;
    int64_t *v = (int64_t *)state.global_csr->v;
    int64_t *reverse_v = (int64_t *)state.global_csr->reverse_v;
    vector<int64_t> &e = state.global_csr->e;
    auto& top_down_cost = bfs_state->top_down_cost;
    auto& bottom_up_cost = bfs_state->bottom_up_cost;
    auto& lane_to_num = bfs_state->lane_to_num;

    // clear next before each iteration
    for (auto i = left; i < right; i++) {
      next[i] = 0;
    }

    barrier.Wait();

    while (true) {
      auto task = bfs_state->FetchTask(worker_id);
      if (task.first == task.second) {
        break;
      }
      auto start = task.first;
      auto end = task.second;

      for (auto i = start; i < end; i++) {
        if (visit[i].any()) {
          for (auto offset = v[i]; offset < v[i + 1]; offset++) {
            auto n = e[offset];
            std::lock_guard<std::mutex> lock(bfs_state->locks[n]);
            next[n] |= visit[i];
          }
        }
      }
    }

    barrier.Wait();

    for (auto i = left; i < right; i++) {
      if (next[i].any()) {
        next[i] &= ~seen[i];
        seen[i] |= next[i];
        top_down_cost += v[i + 1] - v[i];
      }
      if (~(seen[i].all())) {
        bottom_up_cost += reverse_v[i + 1] - reverse_v[i];
      }
    }
  }

  void IterativeLengthBottomUp() {
    auto& bfs_state = state.global_bfs_state;
    auto& seen = bfs_state->seen;
    auto& visit = bfs_state->iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
    auto& next = bfs_state->iter & 1 ? bfs_state->visit2 : bfs_state->visit1;
    auto& barrier = bfs_state->barrier;
    int64_t *v = (int64_t *)state.global_csr->reverse_v;
    int64_t *normal_v = (int64_t *)state.global_csr->v;
    vector<int64_t> &e = state.global_csr->reverse_e;
    auto& top_down_cost = bfs_state->top_down_cost;
    auto& bottom_up_cost = bfs_state->bottom_up_cost;

    // clear next before each iteration
    for (auto i = left; i < right; i++) {
      next[i] = 0;
    }
    
    barrier.Wait();

    while (true) {
      auto task = bfs_state->FetchTask(worker_id);
      if (task.first == task.second) {
        break;
      }
      auto start = task.first;
      auto end = task.second;

      for (auto i = start; i < end; i++) {
        if (seen[i].all()) {
          continue;
        }

        for (auto offset = v[i]; offset < v[i + 1]; offset++) {
          auto n = e[offset];
          next[i] |= visit[n];
        }

        if (next[i].any()) {
          next[i] &= ~seen[i];
          seen[i] |= next[i];
          top_down_cost += normal_v[i + 1] - normal_v[i];
        }
        if (~(seen[i].all())) {
          bottom_up_cost += v[i + 1] - v[i];
        }
      }
    }
  }

  void ReachDetect() {
    auto &bfs_state = state.global_bfs_state;
    auto result_data = FlatVector::GetData<int64_t>(bfs_state->result.data[0]);

    // detect lanes that finished
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = bfs_state->lane_to_num[lane];
      if (search_num >= 0) { // active lane
        int64_t dst_pos = bfs_state->vdata_dst.sel->get_index(search_num);
        if (bfs_state->seen[bfs_state->dst[dst_pos]][lane]) {
          result_data[search_num] =
              bfs_state->iter;               /* found at iter => iter = path length */
          bfs_state->lane_to_num[lane] = -1; // mark inactive
          bfs_state->active--;
        }
      }
    }
    if (bfs_state->active == 0) {
      bfs_state->change = false;
    }
    // into the next iteration
    bfs_state->iter++;
  }

  void UnReachableSet() {
    auto &bfs_state = state.global_bfs_state;
    auto result_data = FlatVector::GetData<int64_t>(bfs_state->result.data[0]);
    auto &result_validity = FlatVector::Validity(bfs_state->result.data[0]);

    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = bfs_state->lane_to_num[lane];
      if (search_num >= 0) { // active lane
        result_validity.SetInvalid(search_num);
        result_data[search_num] = (int64_t)-1; /* no path */
        bfs_state->lane_to_num[lane] = -1;     // mark inactive
      }
    }
  }
private:
	ClientContext &context;
	PathFindingGlobalState &state;
  // [left, right)
  idx_t left;
  idx_t right;
  idx_t worker_id;
};

class PhysicalCSREdgeCreationTask : public ExecutorTask {
public:
  PhysicalCSREdgeCreationTask(shared_ptr<Event> event_p, ClientContext &context, PathFindingGlobalState &state)
      : ExecutorTask(context, std::move(event_p)), context(context), state(state) {
  }

  TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
    auto& global_inputs = state.global_inputs;
    auto& global_csr = state.global_csr;
    auto& scan_state = state.scan_state;

    DataChunk input;
    global_inputs->InitializeScanChunk(input);
    auto result = Vector(LogicalTypeId::BIGINT);
    while (true) {
      {
        lock_guard<mutex> lock(global_csr->csr_lock);
        if (!global_inputs->Scan(scan_state, input)) {
          break;
        }
      }
      if (!global_csr->initialized_e) {
        const auto e_size = input.data[8].GetValue(0).GetValue<int64_t>();
        global_csr->InitializeEdge(e_size);
      }
      TernaryExecutor::Execute<int64_t, int64_t, int64_t, int32_t>(
          input.data[4], input.data[7], input.data[2], result, input.size(),
          [&](int64_t src, int64_t dst, int64_t edge_id) {
            const auto pos = ++global_csr->v[src + 1];
            global_csr->e[static_cast<int64_t>(pos) - 1] = dst;
            global_csr->edge_ids[static_cast<int64_t>(pos) - 1] = edge_id;
            return 1;
          });
      TernaryExecutor::Execute<int64_t, int64_t, int64_t, int32_t>(
          input.data[7], input.data[4], input.data[2], result, input.size(),
          [&](int64_t dst, int64_t src, int64_t edge_id) {
            const auto pos = ++global_csr->reverse_v[dst + 1];
            global_csr->reverse_e[static_cast<int64_t>(pos) - 1] = src;
            global_csr->reverse_edge_ids[static_cast<int64_t>(pos) - 1] = edge_id;
            return 1;
          });
    }
    event->FinishTask();
    return TaskExecutionResult::TASK_FINISHED;
  }

private:
  ClientContext &context;
  PathFindingGlobalState &state;
};

class CSREdgeCreationEvent : public BasePipelineEvent {
public:
  CSREdgeCreationEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p)
      : BasePipelineEvent(pipeline_p), gstate(gstate_p) {
  }

  PathFindingGlobalState &gstate;

  void Schedule() override {
    auto &context = pipeline->GetClientContext();
    auto &ts = TaskScheduler::GetScheduler(context);
    idx_t num_threads = ts.NumberOfThreads();
    auto& scan_state = gstate.scan_state;
    auto& global_inputs = gstate.global_inputs;

    global_inputs->InitializeScan(scan_state);

    vector<shared_ptr<Task>> tasks;
    for (idx_t tnum = 0; tnum < num_threads; tnum++) {
      tasks.push_back(make_uniq<PhysicalCSREdgeCreationTask>(shared_from_this(), context, gstate));
    }
    SetTasks(std::move(tasks));
  }

  void FinishEvent() override {
    auto &gstate = this->gstate;
    auto &global_csr = gstate.global_csr;
    global_csr->is_ready = true;
    // debug print
    // global_csr->Print();
  }
};

class ParallelIterativeEvent : public BasePipelineEvent {
public:
	ParallelIterativeEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p) {
	}

	PathFindingGlobalState &gstate;

public:
	void Schedule() override {
    auto &bfs_state = gstate.global_bfs_state;
		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> bfs_tasks;
    for (idx_t tnum = 0; tnum < bfs_state->num_threads; tnum++) {
      bfs_tasks.push_back(make_uniq<PhysicalIterativeTask>(shared_from_this(), context, gstate, tnum));
    }
		SetTasks(std::move(bfs_tasks));
	}

	void FinishEvent() override {
		auto& bfs_state = gstate.global_bfs_state;

    // if remaining pairs, schedule the BFS for the next batch
    if (bfs_state->started_searches < gstate.global_tasks->Count()) {
      PhysicalPathFinding::ScheduleBFSTasks(*pipeline, *this, gstate);
    }
  }
};

class SequentialIterativeEvent : public BasePipelineEvent {
public:
  SequentialIterativeEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p)
      : BasePipelineEvent(pipeline_p), gstate(gstate_p) {
  }

  PathFindingGlobalState &gstate;

private:
  std::chrono::high_resolution_clock::time_point start_time;

public:
  void Schedule() override {
    auto &bfs_state = gstate.global_bfs_state;

    auto& pairs = *bfs_state->pairs;
    auto& result = bfs_state->result.data[0];
    start_time = std::chrono::high_resolution_clock::now();
    IterativeLengthFunction(gstate.global_csr, pairs, result);
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  }

private:
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

  static void IterativeLengthFunction(const shared_ptr<PathFindingGlobalState::GlobalCompressedSparseRow> &csr,
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
};

class PhysicalShortestPathTask : public ExecutorTask {
public:
  PhysicalShortestPathTask(shared_ptr<Event> event_p, ClientContext &context, PathFindingGlobalState &state, idx_t worker_id)
      : ExecutorTask(context, std::move(event_p)), context(context), state(state), worker_id(worker_id) {
  }

  TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
    auto& bfs_state = state.global_bfs_state;
    auto& change = bfs_state->change;
    auto& barrier = bfs_state->barrier;

    auto bound = bfs_state->BoundaryCalculation(worker_id);
    left = bound.first;
    right = bound.second;

    do {
      bfs_state->InitTask(worker_id);

      if (bfs_state->is_top_down) {
        IterativePathTopDown();
      } else {
        IterativePathBottomUp();
      }
      barrier.Wait();

      if (worker_id == 0) {
        ReachDetect();
        bfs_state->DirectionSwitch();
      }

      barrier.Wait();
    } while (change);

    if (worker_id == 0) {
      PathConstruction();
    }

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
  }

private:
  void IterativePathTopDown() {
    auto& bfs_state = state.global_bfs_state;
    auto& seen = bfs_state->seen;
    auto& visit = bfs_state->iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
    auto& next = bfs_state->iter & 1 ? bfs_state->visit2 : bfs_state->visit1;
    auto& barrier = bfs_state->barrier;
    int64_t *v = (int64_t *)state.global_csr->v;
    int64_t *reverse_v = (int64_t *)state.global_csr->reverse_v;
    vector<int64_t> &e = state.global_csr->e;
    auto& edge_ids = state.global_csr->edge_ids;
    auto& top_down_cost = bfs_state->top_down_cost;
    auto& bottom_up_cost = bfs_state->bottom_up_cost;
    auto& parents_v = bfs_state->parents_v;
    auto& parents_e = bfs_state->parents_e;

    // clear next before each iteration
    for (auto i = left; i < right; i++) {
      next[i] = 0;
    }

    barrier.Wait();

    while (true) {
      auto task = bfs_state->FetchTask(worker_id);
      if (task.first == task.second) {
        break;
      }
      auto start = task.first;
      auto end = task.second;

      for (auto i = start; i < end; i++) {
        if (visit[i].any()) {
          for (auto offset = v[i]; offset < v[i + 1]; offset++) {
            auto n = e[offset];
            auto edge_id = edge_ids[offset];
            std::lock_guard<std::mutex> lock(bfs_state->locks[n]);
            next[n] |= visit[i];
            for (auto l = 0; l < LANE_LIMIT; l++) {
              parents_v[n][l] = ((parents_v[n][l] == -1) && visit[i][l])
                      ? i : parents_v[n][l];
              parents_e[n][l] = ((parents_e[n][l] == -1) && visit[i][l])
                      ? edge_id : parents_e[n][l];
            }
          }
        }
      }
    }

    barrier.Wait();

    for (auto i = left; i < right; i++) {
      if (next[i].any()) {
        next[i] &= ~seen[i];
        seen[i] |= next[i];
        top_down_cost += v[i + 1] - v[i];
      }
      if (~(seen[i].all())) {
        bottom_up_cost += reverse_v[i + 1] - reverse_v[i];
      }
    }
  }

  void IterativePathBottomUp() {
    auto& bfs_state = state.global_bfs_state;
    auto& seen = bfs_state->seen;
    auto& visit = bfs_state->iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
    auto& next = bfs_state->iter & 1 ? bfs_state->visit2 : bfs_state->visit1;
    auto& barrier = bfs_state->barrier;
    int64_t *v = (int64_t *)state.global_csr->reverse_v;
    int64_t *normal_v = (int64_t *)state.global_csr->v;
    vector<int64_t> &e = state.global_csr->reverse_e;
    auto& edge_ids = state.global_csr->reverse_edge_ids;
    auto& top_down_cost = bfs_state->top_down_cost;
    auto& bottom_up_cost = bfs_state->bottom_up_cost;
    auto& parents_v = bfs_state->parents_v;
    auto& parents_e = bfs_state->parents_e;

    // clear next before each iteration
    for (auto i = left; i < right; i++) {
      next[i] = 0;
    }
    
    barrier.Wait();

    while (true) {
      auto task = bfs_state->FetchTask(worker_id);
      if (task.first == task.second) {
        break;
      }
      auto start = task.first;
      auto end = task.second;

      for (auto i = start; i < end; i++) {
        if (seen[i].all()) {
          continue;
        }

        for (auto offset = v[i]; offset < v[i + 1]; offset++) {
          auto n = e[offset];
          auto edge_id = edge_ids[offset];
          next[i] |= visit[n];
          for (auto l = 0; l < LANE_LIMIT; l++) {
            parents_v[i][l] = ((parents_v[i][l] == -1) && visit[n][l])
                    ? n : parents_v[i][l];
            parents_e[i][l] = ((parents_e[i][l] == -1) && visit[n][l])
                    ? edge_id : parents_e[i][l];
          }
        }

        if (next[i].any()) {
          next[i] &= ~seen[i];
          seen[i] |= next[i];
          top_down_cost += normal_v[i + 1] - normal_v[i];
        }
        if (~(seen[i].any())) {
          bottom_up_cost += v[i + 1] - v[i];
        }
      }
    }
  }

  void ReachDetect() {
    auto &bfs_state = state.global_bfs_state;
    auto &change = bfs_state->change;
    auto &top_down_cost = bfs_state->top_down_cost;
    auto &bottom_up_cost = bfs_state->bottom_up_cost;
    // detect lanes that finished
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = bfs_state->lane_to_num[lane];

      if (search_num >= 0) { // active lane
        //! Check if dst for a source has been seen
        int64_t dst_pos = bfs_state->vdata_dst.sel->get_index(search_num);
        if (bfs_state->seen[bfs_state->dst[dst_pos]][lane]) {
          bfs_state->active--;
        }
      }
    }
    if (bfs_state->active == 0) {
      change = false;
    }
    // into the next iteration
    bfs_state->iter++;
  }

  void PathConstruction() {
    auto &bfs_state = state.global_bfs_state;
    auto &result = bfs_state->result.data[1];
    auto result_data = FlatVector::GetData<list_entry_t>(result);
    auto &result_validity = FlatVector::Validity(result);
    //! Reconstruct the paths
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = bfs_state->lane_to_num[lane];
      if (search_num == -1) { // empty lanes
        continue;
      }

      //! Searches that have stopped have found a path
      int64_t src_pos = bfs_state->vdata_src.sel->get_index(search_num);
      int64_t dst_pos = bfs_state->vdata_dst.sel->get_index(search_num);
      if (bfs_state->src[src_pos] == bfs_state->dst[dst_pos]) { // Source == destination
        unique_ptr<Vector> output =
            make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
        ListVector::PushBack(*output, bfs_state->src[src_pos]);
        ListVector::Append(result, ListVector::GetEntry(*output),
                          ListVector::GetListSize(*output));
        result_data[search_num].length = ListVector::GetListSize(*output);
        result_data[search_num].offset = bfs_state->total_len;
        bfs_state->total_len += result_data[search_num].length;
        continue;
      }
      std::vector<int64_t> output_vector;
      std::vector<int64_t> output_edge;
      auto source_v = bfs_state->src[src_pos]; // Take the source

      auto parent_vertex =
          bfs_state->parents_v[bfs_state->dst[dst_pos]]
                  [lane]; // Take the parent vertex of the destination vertex
      auto parent_edge =
          bfs_state->parents_e[bfs_state->dst[dst_pos]]
                  [lane]; // Take the parent edge of the destination vertex

      output_vector.push_back(bfs_state->dst[dst_pos]); // Add destination vertex
      output_vector.push_back(parent_edge);
      while (parent_vertex != source_v) { // Continue adding vertices until we
                                          // have reached the source vertex
        //! -1 is used to signify no parent
        if (parent_vertex == -1 ||
            parent_vertex == bfs_state->parents_v[parent_vertex][lane]) {
          result_validity.SetInvalid(search_num);
          break;
        }
        output_vector.push_back(parent_vertex);
        parent_edge = bfs_state->parents_e[parent_vertex][lane];
        parent_vertex = bfs_state->parents_v[parent_vertex][lane];
        output_vector.push_back(parent_edge);
      }

      if (!result_validity.RowIsValid(search_num)) {
        continue;
      }
      output_vector.push_back(source_v);
      std::reverse(output_vector.begin(), output_vector.end());
      auto output = make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
      for (auto val : output_vector) {
        Value value_to_insert = val;
        ListVector::PushBack(*output, value_to_insert);
      }

      result_data[search_num].length = ListVector::GetListSize(*output);
      result_data[search_num].offset = bfs_state->total_len;
      ListVector::Append(result, ListVector::GetEntry(*output),
                        ListVector::GetListSize(*output));
      bfs_state->total_len += result_data[search_num].length;
    }
  }

  ClientContext &context;
	PathFindingGlobalState &state;
  // [left, right)
  idx_t left;
  idx_t right;
  idx_t worker_id;
};

class ParallelShortestPathEvent : public BasePipelineEvent {
public:
  ParallelShortestPathEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p)
      : BasePipelineEvent(pipeline_p), gstate(gstate_p) {
  }

  PathFindingGlobalState &gstate;

public:
  void Schedule() override {
    auto &bfs_state = gstate.global_bfs_state;
		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> bfs_tasks;
    for (idx_t tnum = 0; tnum < bfs_state->num_threads; tnum++) {
      bfs_tasks.push_back(make_uniq<PhysicalShortestPathTask>(shared_from_this(), context, gstate, tnum));
    }
		SetTasks(std::move(bfs_tasks));
  }

  void FinishEvent() override {
    auto &bfs_state = gstate.global_bfs_state;

    // if remaining pairs, schedule the BFS for the next batch
    if (bfs_state->started_searches < gstate.global_tasks->Count()) {
      PhysicalPathFinding::ScheduleBFSTasks(*pipeline, *this, gstate);
    }
  }
};

class SequentialShortestPathEvent : public BasePipelineEvent {
public:
  SequentialShortestPathEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p)
      : BasePipelineEvent(pipeline_p), gstate(gstate_p) {
  }

  PathFindingGlobalState &gstate;

public:
  void Schedule() override {
    auto &bfs_state = gstate.global_bfs_state;
    auto &pairs = *bfs_state->pairs;
    auto &result = bfs_state->result.data[1];
    auto start_time = std::chrono::high_resolution_clock::now();
    ShortestPathFunction(gstate.global_csr, pairs, result);
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  }

private:
  static bool IterativeLength(int64_t v_size, int64_t *V, vector<int64_t> &E,
                              vector<int64_t> &edge_ids,
                              vector<std::vector<int64_t>> &parents_v,
                              vector<std::vector<int64_t>> &parents_e,
                              vector<std::bitset<LANE_LIMIT>> &seen,
                              vector<std::bitset<LANE_LIMIT>> &visit,
                              vector<std::bitset<LANE_LIMIT>> &next) {
    bool change = false;
    for (auto v = 0; v < v_size; v++) {
      next[v] = 0;
    }
    //! Keep track of edge id through which the node was reached
    for (auto v = 0; v < v_size; v++) {
      if (visit[v].any()) {
        for (auto e = V[v]; e < V[v + 1]; e++) {
          auto n = E[e];
          auto edge_id = edge_ids[e];
          next[n] = next[n] | visit[v];
          for (auto l = 0; l < LANE_LIMIT; l++) {
            parents_v[n][l] =
                ((parents_v[n][l] == -1) && visit[v][l]) ? v : parents_v[n][l];
            parents_e[n][l] = ((parents_e[n][l] == -1) && visit[v][l])
                                  ? edge_id
                                  : parents_e[n][l];
          }
        }
      }
    }

    for (auto v = 0; v < v_size; v++) {
      next[v] = next[v] & ~seen[v];
      seen[v] = seen[v] | next[v];
      change |= next[v].any();
    }
    return change;
  }

  static void ShortestPathFunction(const shared_ptr<PathFindingGlobalState::GlobalCompressedSparseRow> &csr,
                                      DataChunk &pairs, Vector &result) {
    int64_t v_size = csr->v_size;
    int64_t *v = (int64_t *)csr->v;
    vector<int64_t> &e = csr->e;
    vector<int64_t> &edge_ids = csr->edge_ids;

    auto &src = pairs.data[0];
    auto &dst = pairs.data[1];
    UnifiedVectorFormat vdata_src;
    UnifiedVectorFormat vdata_dst;
    src.ToUnifiedFormat(pairs.size(), vdata_src);
    dst.ToUnifiedFormat(pairs.size(), vdata_dst);

    auto src_data = (int64_t *)vdata_src.data;
    auto dst_data = (int64_t *)vdata_dst.data;

    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto result_data = FlatVector::GetData<list_entry_t>(result);
    ValidityMask &result_validity = FlatVector::Validity(result);

    // create temp SIMD arrays
    vector<std::bitset<LANE_LIMIT>> seen(v_size);
    vector<std::bitset<LANE_LIMIT>> visit1(v_size);
    vector<std::bitset<LANE_LIMIT>> visit2(v_size);
    vector<std::vector<int64_t>> parents_v(v_size,
                                          std::vector<int64_t>(LANE_LIMIT, -1));
    vector<std::vector<int64_t>> parents_e(v_size,
                                          std::vector<int64_t>(LANE_LIMIT, -1));

    // maps lane to search number
    int16_t lane_to_num[LANE_LIMIT];
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      lane_to_num[lane] = -1; // inactive
    }
    int64_t total_len = 0;

    idx_t started_searches = 0;
    while (started_searches < pairs.size()) {

      // empty visit vectors
      for (auto i = 0; i < v_size; i++) {
        seen[i] = 0;
        visit1[i] = 0;
        for (auto j = 0; j < LANE_LIMIT; j++) {
          parents_v[i][j] = -1;
          parents_e[i][j] = -1;
        }
      }

      // add search jobs to free lanes
      uint64_t active = 0;
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        lane_to_num[lane] = -1;
        while (started_searches < pairs.size()) {
          int64_t search_num = started_searches++;
          int64_t src_pos = vdata_src.sel->get_index(search_num);
          if (!vdata_src.validity.RowIsValid(src_pos)) {
            result_validity.SetInvalid(search_num);
          } else {
            visit1[src_data[src_pos]][lane] = true;
            parents_v[src_data[src_pos]][lane] =
                src_data[src_pos]; // Mark source with source id
            parents_e[src_data[src_pos]][lane] =
                -2; // Mark the source with -2, there is no incoming edge for the
                    // source.
            lane_to_num[lane] = search_num; // active lane
            active++;
            break;
          }
        }
      }

      //! make passes while a lane is still active
      for (int64_t iter = 1; active; iter++) {
        //! Perform one step of bfs exploration
        if (!IterativeLength(v_size, v, e, edge_ids, parents_v, parents_e, seen,
                            (iter & 1) ? visit1 : visit2,
                            (iter & 1) ? visit2 : visit1)) {
          break;
        }
        int64_t finished_searches = 0;
        // detect lanes that finished
        for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
          int64_t search_num = lane_to_num[lane];
          if (search_num >= 0) { // active lane
            //! Check if dst for a source has been seen
            int64_t dst_pos = vdata_dst.sel->get_index(search_num);
            if (seen[dst_data[dst_pos]][lane]) {
              finished_searches++;
            }
          }
        }
        if (finished_searches == LANE_LIMIT) {
          break;
        }
      }
      //! Reconstruct the paths
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        int64_t search_num = lane_to_num[lane];
        if (search_num == -1) { // empty lanes
          continue;
        }

        //! Searches that have stopped have found a path
        int64_t src_pos = vdata_src.sel->get_index(search_num);
        int64_t dst_pos = vdata_dst.sel->get_index(search_num);
        if (src_data[src_pos] == dst_data[dst_pos]) { // Source == destination
          unique_ptr<Vector> output =
              make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
          ListVector::PushBack(*output, src_data[src_pos]);
          ListVector::Append(result, ListVector::GetEntry(*output),
                            ListVector::GetListSize(*output));
          result_data[search_num].length = ListVector::GetListSize(*output);
          result_data[search_num].offset = total_len;
          total_len += result_data[search_num].length;
          continue;
        }
        std::vector<int64_t> output_vector;
        std::vector<int64_t> output_edge;
        auto source_v = src_data[src_pos]; // Take the source

        auto parent_vertex =
            parents_v[dst_data[dst_pos]]
                    [lane]; // Take the parent vertex of the destination vertex
        auto parent_edge =
            parents_e[dst_data[dst_pos]]
                    [lane]; // Take the parent edge of the destination vertex

        output_vector.push_back(dst_data[dst_pos]); // Add destination vertex
        output_vector.push_back(parent_edge);
        while (parent_vertex != source_v) { // Continue adding vertices until we
                                            // have reached the source vertex
          //! -1 is used to signify no parent
          if (parent_vertex == -1 ||
              parent_vertex == parents_v[parent_vertex][lane]) {
            result_validity.SetInvalid(search_num);
            break;
          }
          output_vector.push_back(parent_vertex);
          parent_edge = parents_e[parent_vertex][lane];
          parent_vertex = parents_v[parent_vertex][lane];
          output_vector.push_back(parent_edge);
        }

        if (!result_validity.RowIsValid(search_num)) {
          continue;
        }
        output_vector.push_back(source_v);
        std::reverse(output_vector.begin(), output_vector.end());
        auto output = make_uniq<Vector>(LogicalType::LIST(LogicalType::BIGINT));
        for (auto val : output_vector) {
          Value value_to_insert = val;
          ListVector::PushBack(*output, value_to_insert);
        }

        result_data[search_num].length = ListVector::GetListSize(*output);
        result_data[search_num].offset = total_len;
        ListVector::Append(result, ListVector::GetEntry(*output),
                          ListVector::GetListSize(*output));
        total_len += result_data[search_num].length;
      }
    }
  }
};

SinkFinalizeType
PhysicalPathFinding::Finalize(Pipeline &pipeline, Event &event,
                              ClientContext &context,
                              OperatorSinkFinalizeInput &input) const {
  auto &gstate = input.global_state.Cast<PathFindingGlobalState>();
  auto &csr = gstate.global_csr;
  auto &global_tasks = gstate.global_tasks;

  if (gstate.child == 0) {
    auto csr_event = make_shared<CSREdgeCreationEvent>(gstate, pipeline);
    event.InsertEvent(std::move(std::dynamic_pointer_cast<BasePipelineEvent>(csr_event)));
  } else if (gstate.child == 1 && global_tasks->Count() > 0) {
    auto all_pairs = make_shared<DataChunk>();
    DataChunk pairs;
    global_tasks->InitializeScanChunk(*all_pairs);
    global_tasks->InitializeScanChunk(pairs);
    ColumnDataScanState scan_state;
    global_tasks->InitializeScan(scan_state);
    while (global_tasks->Scan(scan_state, pairs)) {
      all_pairs->Append(pairs, true);
    }
    // debug print
    // all_pairs->Print();


    auto &ts = TaskScheduler::GetScheduler(context);
		idx_t num_threads = ts.NumberOfThreads();
    auto& client_config = ClientConfig::GetConfig(context);
    idx_t mode = this->mode == "iterativelength" ? 0 : 1;
    gstate.global_bfs_state = make_uniq<GlobalBFSState>(csr, all_pairs, csr->v_size - 2, num_threads, mode, context);

    auto const task_size = client_config.set_variables.find("experimental_path_finding_operator_task_size");
    gstate.global_bfs_state->split_size = task_size != client_config.set_variables.end() ? task_size->second.GetValue<idx_t>() : 256;

    auto const alpha = client_config.set_variables.find("experimental_path_finding_operator_alpha");
    gstate.global_bfs_state->alpha = alpha != client_config.set_variables.end() ? alpha->second.GetValue<double>() : 1;

    auto const sequential = client_config.set_variables.find("experimental_path_finding_operator_sequential");
    if (sequential != client_config.set_variables.end() && sequential->second.GetValue<bool>()) {
      if (gstate.mode == "iterativelength") {
        auto bfs_event = make_shared<SequentialIterativeEvent>(gstate, pipeline);
        event.InsertEvent(std::move(std::dynamic_pointer_cast<BasePipelineEvent>(bfs_event)));
      } else if (gstate.mode == "shortestpath") {
        auto bfs_event = make_shared<SequentialShortestPathEvent>(gstate, pipeline);
        event.InsertEvent(std::move(std::dynamic_pointer_cast<BasePipelineEvent>(bfs_event)));
      }
    } else {
      // Schedule the first round of BFS tasks
      if (all_pairs->size() > 0) {
        ScheduleBFSTasks(pipeline, event, gstate);
      }
    }
  }

  // Move to the next input child
  ++gstate.child;

  return SinkFinalizeType::READY;
}

void PhysicalPathFinding::ScheduleBFSTasks(Pipeline &pipeline, Event &event,
                                           GlobalSinkState &state) {
  auto &gstate = state.Cast<PathFindingGlobalState>();
  auto &bfs_state = gstate.global_bfs_state;

  // for every batch of pairs, schedule a BFS task
  bfs_state->Clear();

  // remaining pairs
  if (bfs_state->started_searches < gstate.global_tasks->Count()) {

    auto result_data = FlatVector::GetData<int64_t>(bfs_state->result.data[0]);
    auto& result_validity = FlatVector::Validity(bfs_state->result.data[0]);
    std::bitset<LANE_LIMIT> seen_mask = ~0;

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
      auto bfs_event = make_shared<ParallelIterativeEvent>(gstate, pipeline);
      event.InsertEvent(std::move(std::dynamic_pointer_cast<BasePipelineEvent>(bfs_event)));
    } else if (gstate.mode == "shortestpath") {
      auto bfs_event = make_shared<ParallelShortestPathEvent>(gstate, pipeline);
      event.InsertEvent(std::move(std::dynamic_pointer_cast<BasePipelineEvent>(bfs_event)));
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
  return result.size() == 0
      ? SourceResultType::FINISHED
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
