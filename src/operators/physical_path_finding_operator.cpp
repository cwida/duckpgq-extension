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

struct ve {
  // higher 30 bits for v, lower 34 bits for e
  const uint8_t v_bits = 30;
  const uint8_t e_bits = 34;
  uint64_t value;
  const uint64_t v_mask = UINT64_MAX << e_bits;
  const uint64_t e_mask = UINT64_MAX >> v_bits;
  ve() : value(UINT64_MAX) {}
  ve(uint64_t value) : value(value) {}
  ve(int64_t v, int64_t e) {
    uint64_t new_value = 0;
    new_value |= v < 0 ? v_mask : (v << e_bits);
    new_value |= e < 0 ? e_mask : (e & e_mask);
    value = new_value;
  }
  ve& operator=(std::initializer_list<int64_t> list) {
    uint64_t new_value = 0;
    auto it = list.begin();
    new_value |= *it < 0 ? v_mask : (*it << e_bits);
    new_value |= *(++it) < 0 ? e_mask : (*it & e_mask);
    value = new_value;
    return *this;
  }
  inline int64_t GetV() {
    return (value & v_mask) == v_mask ? -1 : static_cast<int64_t>(value >> e_bits);
  }
  inline int64_t GetE() {
    return (value & e_mask) == e_mask ? -1 : static_cast<int64_t>(value & e_mask);
  }
};

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
        num_threads(num_threads_), task_queues(num_threads_), barrier(num_threads_), element_locks(v_size_), mode(mode_) {
    result.Initialize(context, {LogicalType::BIGINT, LogicalType::LIST(LogicalType::BIGINT)}, pairs_->size());
    auto &src_data = pairs->data[0];
    auto &dst_data = pairs->data[1];
    src_data.ToUnifiedFormat(pairs->size(), vdata_src);
    dst_data.ToUnifiedFormat(pairs->size(), vdata_dst);
    src = FlatVector::GetData<int64_t>(src_data);
    dst = FlatVector::GetData<int64_t>(dst_data);

    CreateTasks();
  }

  GlobalBFSState(shared_ptr<GlobalCompressedSparseRow> csr_, shared_ptr<DataChunk> pairs_, int64_t v_size_, 
                idx_t num_threads_, idx_t mode_, ClientContext &context_, bool is_path_)
      : csr(csr_), pairs(pairs_), iter(1), v_size(v_size_), change(false), 
        started_searches(0), total_len(0), context(context_), seen(v_size_), visit1(v_size_), visit2(v_size_),
        parents_ve(v_size_), num_threads(num_threads_), task_queues(num_threads_), barrier(num_threads_), 
        element_locks(v_size_), mode(mode_) {
    result.Initialize(context, {LogicalType::BIGINT, LogicalType::LIST(LogicalType::BIGINT)}, pairs_->size());
    auto &src_data = pairs->data[0];
    auto &dst_data = pairs->data[1];
    src_data.ToUnifiedFormat(pairs->size(), vdata_src);
    dst_data.ToUnifiedFormat(pairs->size(), vdata_dst);
    src = FlatVector::GetData<int64_t>(src_data);
    dst = FlatVector::GetData<int64_t>(dst_data);

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
          parents_ve[i][j] = {-1, -1};
        }
      }
    }
  }


  void CreateTasks() {
    // workerTasks[workerId] = [task1, task2, ...]
    auto queues = &task_queues;
    vector<vector<pair<idx_t, idx_t>>> worker_tasks(num_threads);
    auto cur_worker = 0;
    int64_t *v = (int64_t*)csr->v;
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
      queues->at(worker_id).first.store(0);
      queues->at(worker_id).second = worker_tasks[worker_id];
    }
  }

  void InitTask(idx_t worker_id) {
    task_queues[worker_id].first.store(0);
  }

  pair<idx_t, idx_t> FetchTask(idx_t worker_id) {
    auto& task_queue = task_queues;
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
  vector<std::array<ve, LANE_LIMIT>> parents_ve;

  idx_t num_threads;
  // task_queues[workerId] = {curTaskIx, queuedTasks}
  // queuedTasks[curTaskIx] = {start, end}
  vector<pair<atomic<idx_t>, vector<pair<idx_t, idx_t>>>> task_queues;
  int64_t split_size = 256;

  Barrier barrier;

  // lock for next
  mutable vector<mutex> element_locks;

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

      IterativeLength();

      barrier.Wait();

      if (worker_id == 0) {
        ReachDetect();
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
  void IterativeLength() {
    auto& bfs_state = state.global_bfs_state;
    auto& seen = bfs_state->seen;
    auto& visit = bfs_state->iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
    auto& next = bfs_state->iter & 1 ? bfs_state->visit2 : bfs_state->visit1;
    auto& barrier = bfs_state->barrier;
    int64_t *v = (int64_t *)state.global_csr->v;
    vector<int64_t> &e = state.global_csr->e;
    auto& lane_to_num = bfs_state->lane_to_num;
    auto& change = bfs_state->change;

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
            std::lock_guard<std::mutex> lock(bfs_state->element_locks[n]);
            next[n] |= visit[i];
          }
        }
      }
    }

    change = false;
    barrier.Wait();

    for (auto i = left; i < right; i++) {
      if (next[i].any()) {
        next[i] &= ~seen[i];
        seen[i] |= next[i];
        change |= next[i].any();
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
        const auto e_size = input.data[7].GetValue(0).GetValue<int64_t>();
        global_csr->InitializeEdge(e_size);
      }
      TernaryExecutor::Execute<int64_t, int64_t, int64_t, int32_t>(
          input.data[6], input.data[4], input.data[2], result, input.size(),
          [&](int64_t src, int64_t dst, int64_t edge_id) {
            const auto pos = ++global_csr->v[src + 1];
            global_csr->e[static_cast<int64_t>(pos) - 1] = dst;
            global_csr->edge_ids[static_cast<int64_t>(pos) - 1] = edge_id;
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

      IterativePath();
      barrier.Wait();

      if (worker_id == 0) {
        ReachDetect();
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
  void IterativePath() {
    auto& bfs_state = state.global_bfs_state;
    auto& seen = bfs_state->seen;
    auto& visit = bfs_state->iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
    auto& next = bfs_state->iter & 1 ? bfs_state->visit2 : bfs_state->visit1;
    auto& barrier = bfs_state->barrier;
    int64_t *v = (int64_t *)state.global_csr->v;
    vector<int64_t> &e = state.global_csr->e;
    auto& edge_ids = state.global_csr->edge_ids;
    auto& parents_ve = bfs_state->parents_ve;
    auto& change = bfs_state->change;

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
            {
              std::lock_guard<std::mutex> lock(bfs_state->element_locks[n]);
              next[n] |= visit[i];
            }
            for (auto l = 0; l < LANE_LIMIT; l++) {
              if (parents_ve[n][l].GetV() == -1 && visit[i][l]) {
                parents_ve[n][l] = {static_cast<int64_t>(i), edge_id};
              }
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
        change |= next[i].any();
      }
    }
  }

  void ReachDetect() {
    auto &bfs_state = state.global_bfs_state;
    auto &change = bfs_state->change;
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

      auto parent_vertex = bfs_state->parents_ve[bfs_state->dst[dst_pos]][lane].GetV();
      auto parent_edge = bfs_state->parents_ve[bfs_state->dst[dst_pos]][lane].GetE();

      output_vector.push_back(bfs_state->dst[dst_pos]); // Add destination vertex
      output_vector.push_back(parent_edge);
      while (parent_vertex != source_v) { // Continue adding vertices until we
                                          // have reached the source vertex
        //! -1 is used to signify no parent
        if (parent_vertex == -1 ||
            parent_vertex == bfs_state->parents_ve[parent_vertex][lane].GetV()) {
          result_validity.SetInvalid(search_num);
          break;
        }
        output_vector.push_back(parent_vertex);
        parent_edge = bfs_state->parents_ve[parent_vertex][lane].GetE();
        parent_vertex = bfs_state->parents_ve[parent_vertex][lane].GetV();
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
    auto& client_config = ClientConfig::GetConfig(context);
    idx_t mode = this->mode == "iterativelength" ? 0 : 1;
    if (mode == 0) {
      gstate.global_bfs_state = make_uniq<GlobalBFSState>(csr, all_pairs, csr->v_size - 2, num_threads, mode, context);
    } else {
      gstate.global_bfs_state = make_uniq<GlobalBFSState>(csr, all_pairs, csr->v_size - 2, num_threads, mode, context, true);
    }

    auto const task_size = client_config.set_variables.find("experimental_path_finding_operator_task_size");
    gstate.global_bfs_state->split_size = task_size != client_config.set_variables.end() ? task_size->second.GetValue<idx_t>() : 256;

    // Schedule the first round of BFS tasks
    if (all_pairs->size() > 0) {
      ScheduleBFSTasks(pipeline, event, gstate);
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
  int64_t *v = (int64_t *)gstate.global_csr->v;

  // for every batch of pairs, schedule a BFS task
  bfs_state->Clear();

  // remaining pairs
  if (bfs_state->started_searches < gstate.global_tasks->Count()) {

    auto result_data = FlatVector::GetData<int64_t>(bfs_state->result.data[0]);
    auto& result_validity = FlatVector::Validity(bfs_state->result.data[0]);
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
      auto bfs_event = make_shared_ptr<ParallelIterativeEvent>(gstate, pipeline);
      event.InsertEvent(std::move(bfs_event));
    } else if (gstate.mode == "shortestpath") {
      auto bfs_event = make_shared_ptr<ParallelShortestPathEvent>(gstate, pipeline);
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
