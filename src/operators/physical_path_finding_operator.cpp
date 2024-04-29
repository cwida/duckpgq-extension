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
        auto lGen = mGeneration;
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
    std::size_t mCount;
    std::size_t mGeneration;
};

class GlobalBFSState {
public:
  GlobalBFSState(shared_ptr<DataChunk> pairs_, int64_t v_size_, idx_t num_threads_, ClientContext &context_)
      : pairs(pairs_), iter(1), v_size(v_size_), change(false), started_searches(0),
        seen(v_size_), visit1(v_size_), visit2(v_size_), context(context_),
        total_len(0), parents_v(v_size_, std::vector<int64_t>(LANE_LIMIT, -1)), parents_e(v_size_, std::vector<int64_t>(LANE_LIMIT, -1)),
        frontier_size(0), unseen_size(v_size_), num_threads(num_threads_), task_queues(num_threads_), barrier(num_threads_) {
    result.Initialize(context, {LogicalType::BIGINT, LogicalType::LIST(LogicalType::BIGINT)}, STANDARD_VECTOR_SIZE);
    for (auto i = 0; i < LANE_LIMIT; i++) {
      lane_to_num[i] = -1;
    }
    auto &src_data = pairs->data[0];
    auto &dst_data = pairs->data[1];
    src_data.ToUnifiedFormat(pairs->size(), vdata_src);
    dst_data.ToUnifiedFormat(pairs->size(), vdata_dst);
    src = FlatVector::GetData<int64_t>(src_data);
    dst = FlatVector::GetData<int64_t>(dst_data);
    for (auto i = 0; i < v_size; i++) {
      for (auto j = 0; j < 8; j++) {
        seen[i][j] = 0;
        visit1[i][j] = 0;
      }
      for (auto j = 0; j < LANE_LIMIT; j++) {
        parents_v[i][j] = -1;
        parents_e[i][j] = -1;
      }
    }

    CreateTasks();
  }

  void Clear() {
    iter = 1;
    change = false;
    frontier_size = 0;
    unseen_size = v_size;
    for (auto i = 0; i < LANE_LIMIT; i++) {
      lane_to_num[i] = -1;
    }
    // empty visit vectors
    for (auto i = 0; i < v_size; i++) {
      for (auto j = 0; j < 8; j++) {
        seen[i][j] = 0;
        visit1[i][j] = 0;
      }
      for (auto j = 0; j < LANE_LIMIT; j++) {
        parents_v[i][j] = -1;
        parents_e[i][j] = -1;
      }
    }
  }


  void CreateTasks() {
    // workerTasks[workerId] = [task1, task2, ...]
    vector<vector<pair<idx_t, idx_t>>> worker_tasks(num_threads);
    auto cur_worker = 0;

    for (auto offset = 0; offset < v_size; offset += split_size) {
      auto worker_id = cur_worker % num_threads;
      pair<idx_t, idx_t> range = {offset, std::min(offset + split_size, v_size)};
      worker_tasks[worker_id].push_back(range);
      cur_worker++;
    }

    for (idx_t worker_id = 0; worker_id < num_threads; worker_id++) {
      task_queues[worker_id].first.store(0);
      task_queues[worker_id].second = worker_tasks[worker_id];
    }

  }

public:
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
  DataChunk result; // 0 for length, 1 for path
  ClientContext& context;
  vector<array<atomic<idx_t>, 8>> seen;
  vector<array<atomic<idx_t>, 8>> visit1;
  vector<array<atomic<idx_t>, 8>> visit2;
  unique_ptr<Vector> result_length;
  unique_ptr<Vector> result_path;

  vector<std::vector<int64_t>> parents_v;
  vector<std::vector<int64_t>> parents_e;

  atomic<int64_t> frontier_size;
  atomic<int64_t> unseen_size;
  constexpr static int64_t alpha = 1024;
  constexpr static int64_t beta = 64;
  bool is_top_down = true;

  idx_t num_threads;
  // task_queues[workerId] = {curTaskIx, queuedTasks}
  // queuedTasks[curTaskIx] = {start, end}
  vector<pair<atomic<idx_t>, vector<pair<idx_t, idx_t>>>> task_queues;
  constexpr static int64_t split_size = 256;

  Barrier barrier;

  std::chrono::microseconds time_elapsed = std::chrono::microseconds(0);

  // lock for next
  mutable mutex lock;
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

  void Sink(DataChunk &input, PathFindingLocalState &lstate) const {
    lstate.Sink(input, *global_csr, child);
  }

  void CSRCreateEdge() {
    DataChunk input;
    global_inputs->InitializeScanChunk(input);
    ColumnDataScanState scan_state;
    global_inputs->InitializeScan(scan_state);
    auto result = Vector(LogicalTypeId::BIGINT);
    while (global_inputs->Scan(scan_state, input)) {
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
  }

  // pairs is a 2-column table with src and dst
  unique_ptr<ColumnDataCollection> global_tasks;
  unique_ptr<ColumnDataCollection> global_inputs;
  // pairs with path exists
  // ColumnDataCollection global_results;
  ColumnDataScanState scan_state;
  ColumnDataAppendState append_state;

  unique_ptr<GlobalCompressedSparseRow> global_csr;
  // state for BFS
  unique_ptr<GlobalBFSState> global_bfs_state;
  string mode;
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

class PhysicalIterativeTopDownTask : public ExecutorTask {
public:
	PhysicalIterativeTopDownTask(shared_ptr<Event> event_p, ClientContext &context, PathFindingGlobalState &state, idx_t worker_id)
	    : ExecutorTask(context, std::move(event_p)), context(context), state(state), worker_id(worker_id) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
    auto& bfs_state = state.global_bfs_state;
    auto& change = bfs_state->change;
    auto& seen = bfs_state->seen;
    auto& barrier = bfs_state->barrier;
    auto result_data = FlatVector::GetData<int64_t>(bfs_state->result.data[0]);
    ValidityMask &result_validity = FlatVector::Validity(bfs_state->result.data[0]);
    auto& iter = bfs_state->iter;

    int64_t *v = (int64_t *)state.global_csr->v;
    vector<int64_t> &e = state.global_csr->e;

    BoundaryCalculation();

    do {
      barrier.Wait();
      change = false;
      InitTask();

      auto& visit = iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
      auto& next = iter & 1 ? bfs_state->visit2 : bfs_state->visit1;

      IterativeLength(v, change, barrier, e, seen, visit, next);
      barrier.Wait();

      if (worker_id == 0) {
        ReachDetect(result_data);
      }

    } while (change);

    if (worker_id == 0) {
      UnReachableSet(result_data, result_validity);
    }

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
  void InitTask() {
    auto& task_queue = state.global_bfs_state->task_queues;
    task_queue[worker_id].first.store(0);
  }

  pair<idx_t, idx_t> FetchTask() {
    auto& task_queue = state.global_bfs_state->task_queues;
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

  void BoundaryCalculation() {
    auto& bfs_state = state.global_bfs_state;
    auto& v_size = bfs_state->v_size;
    idx_t block_size = ceil((double)v_size / bfs_state->num_threads);
    block_size = block_size == 0 ? 1 : block_size;
    left = block_size * worker_id;
    right = std::min(block_size * (worker_id + 1), (idx_t)v_size);
  }

  void IterativeLength(int64_t *v, bool& change, Barrier& barrier,
                      vector<int64_t> &e,
                      vector<array<atomic<idx_t>, 8>> &seen,
                      vector<array<atomic<idx_t>, 8>> &visit,
                      vector<array<atomic<idx_t>, 8>> &next) {
    // clear next before each iteration
    for (auto i = left; i < right; i++) {
      for (auto j = 0; j < 8; j++) {
        next[i][j].store(0, std::memory_order_relaxed);
      }
    }

    barrier.Wait();

    while (true) {
      auto task = FetchTask();
      if (task.first == task.second) {
        break;
      }
      auto start = task.first;
      auto end = task.second;

      for (auto i = start; i < end; i++) {
        for (auto j = 0; j < 8; j++) {
          if (visit[i][j].load(std::memory_order_relaxed)) {
            for (auto offset = v[i]; offset < v[i + 1]; offset++) {
              auto n = e[offset];
              next[n][j].store(next[n][j].load(std::memory_order_relaxed) | visit[i][j].load(std::memory_order_relaxed), std::memory_order_relaxed);
            }
          }
        }
      }
    }

    barrier.Wait();

    for (auto i = left; i < right; i++) {
      for (auto j = 0; j < 8; j++) {
        if (next[i][j].load(std::memory_order_relaxed)) {
          next[i][j].store(next[i][j].load(std::memory_order_relaxed) & ~seen[i][j].load(std::memory_order_relaxed), std::memory_order_relaxed);
          seen[i][j].store(seen[i][j].load(std::memory_order_relaxed) | next[i][j].load(std::memory_order_relaxed), std::memory_order_relaxed);
          change |= next[i][j].load(std::memory_order_relaxed);
        }
      }
    }
  }

void ReachDetect(int64_t *result_data) {
  auto &bfs_state = state.global_bfs_state;
  // detect lanes that finished
  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    int64_t search_num = bfs_state->lane_to_num[lane];
    if (search_num >= 0) { // active lane
      int64_t dst_pos = bfs_state->vdata_dst.sel->get_index(search_num);
      if (bfs_state->seen[bfs_state->dst[dst_pos]][lane / 64] & ((idx_t)1 << (lane % 64))) {
        result_data[search_num] =
            bfs_state->iter;               /* found at iter => iter = path length */
        bfs_state->lane_to_num[lane] = -1; // mark inactive
      }
    }
  }
  // into the next iteration
  bfs_state->iter++;
}

void UnReachableSet(int64_t *result_data, ValidityMask &result_validity) {
  auto &bfs_state = state.global_bfs_state;
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

// class PhysicalBFSBottomUpTask : public ExecutorTask {
// public:
// 	PhysicalBFSBottomUpTask(shared_ptr<Event> event_p, ClientContext &context, PathFindingGlobalState &state, idx_t start, idx_t end)
// 	    : ExecutorTask(context, std::move(event_p)), context(context), state(state), start(start), end(end) {
// 	}

// 	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
//     auto& bfs_state = state.global_bfs_state;
//     auto& change = bfs_state->change;
//     auto& seen = bfs_state->seen;
//     auto& visit = bfs_state->iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
//     auto& next = bfs_state->iter & 1 ? bfs_state->visit2 : bfs_state->visit1;
//     auto& barrier = bfs_state->barrier;
//     auto& frontier_size = bfs_state->frontier_size;
//     auto& unseen_size = bfs_state->unseen_size;

//     int64_t *v = (int64_t *)state.global_csr->v;
//     vector<int64_t> &e = state.global_csr->e;

//     for (auto i = start; i < end; i++) {
//       next[i] = 0;
//     }

//     barrier.Wait();

//     for (auto i = start; i < end; i++) {
//       if (seen[i].all()) {
//         unseen_size -= 1;
//         continue;
//       }
//       for (auto offset = v[i]; offset < v[i + 1]; offset++) {
//         auto n = e[offset];
//         next[i] = next[i] | visit[n];
//       }
//       next[i] = next[i] & ~seen[i];
//       seen[i] = seen[i] | next[i];
//       change |= next[i].any();

//       frontier_size = next[i].any() ? frontier_size + 1 : frontier_size.load();
//       unseen_size = seen[i].all() ? unseen_size - 1 : unseen_size.load();
//     }

// 		event->FinishTask();
// 		return TaskExecutionResult::TASK_FINISHED;
// 	}

// private:
// 	ClientContext &context;
// 	PathFindingGlobalState &state;
//   // [start, end)
//   idx_t start;
//   idx_t end;
// };

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

private:
  std::chrono::high_resolution_clock::time_point start_time;

public:
	void Schedule() override {
    auto &bfs_state = gstate.global_bfs_state;
		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> bfs_tasks;
    for (idx_t tnum = 0; tnum < bfs_state->num_threads; tnum++) {
      bfs_tasks.push_back(make_uniq<PhysicalIterativeTopDownTask>(shared_from_this(), context, gstate, tnum));
    }
		SetTasks(std::move(bfs_tasks));
    start_time = std::chrono::high_resolution_clock::now();
	}

	void FinishEvent() override {
		auto& bfs_state = gstate.global_bfs_state;

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    bfs_state->time_elapsed += duration;

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
    bfs_state->time_elapsed += duration;
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
};

class PhysicalShortestPathTopDownTask : public ExecutorTask {
public:
  PhysicalShortestPathTopDownTask(shared_ptr<Event> event_p, ClientContext &context, PathFindingGlobalState &state, idx_t worker_id)
      : ExecutorTask(context, std::move(event_p)), context(context), state(state), worker_id(worker_id) {
  }

  TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
    auto& bfs_state = state.global_bfs_state;
    auto& change = bfs_state->change;
    auto& seen = bfs_state->seen;
    auto& v_size = bfs_state->v_size;
    auto& barrier = bfs_state->barrier;
    auto& parents_v = bfs_state->parents_v;
    auto& parents_e = bfs_state->parents_e;
    auto& result_path = bfs_state->result.data[1];
    auto result_data = FlatVector::GetData<list_entry_t>(result_path);
    ValidityMask &result_validity = FlatVector::Validity(result_path);
    auto& iter = bfs_state->iter;

    int64_t *v = (int64_t *)state.global_csr->v;
    vector<int64_t> &e = state.global_csr->e;
    vector<int64_t> &edge_ids = state.global_csr->edge_ids;

    BoundaryCalculation();

    do {
      InitTask();

      auto& visit = iter & 1 ? bfs_state->visit1 : bfs_state->visit2;
      auto& next = iter & 1 ? bfs_state->visit2 : bfs_state->visit1;

      IterativeLength(v_size, change, barrier, v, e, edge_ids, parents_v, parents_e, seen, visit, next);

      barrier.Wait();

      if (worker_id == 0) {
        ReachDetect(result_data, change);
      }

      barrier.Wait();
    } while (change);

    if (worker_id == 0) {
      PathConstruction(result_path, result_data, result_validity);
    }

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
  }

private:
  void InitTask() {
    auto& task_queue = state.global_bfs_state->task_queues;
    task_queue[worker_id].first.store(0);
  }

  pair<idx_t, idx_t> FetchTask() {
    auto& task_queue = state.global_bfs_state->task_queues;
    idx_t offset = 0;
    do {
      auto worker_idx = (worker_id + offset) % task_queue.size();
      auto cur_task_ix = task_queue[worker_idx].first.fetch_add(1);
      if (cur_task_ix < task_queue[worker_idx].second.size()) {
        return task_queue[worker_idx].second[cur_task_ix];
      }
      offset++;
    } while (offset < task_queue.size());
    return {0, 0};
  }

  void BoundaryCalculation() {
    auto& bfs_state = state.global_bfs_state;
    auto& v_size = bfs_state->v_size;
    idx_t block_size = ceil((double)v_size / bfs_state->num_threads);
    block_size = block_size == 0 ? 1 : block_size;
    left = block_size * worker_id;
    right = std::min(block_size * (worker_id + 1), (idx_t)v_size);
  }

  void IterativeLength(int64_t v_size, bool& change, Barrier& barrier, int64_t *v,
                      vector<int64_t> &e, vector<int64_t> &edge_ids,
                      vector<std::vector<int64_t>> &parents_v,
                      vector<std::vector<int64_t>> &parents_e,
                      vector<array<atomic<idx_t>, 8>> &seen,
                      vector<array<atomic<idx_t>, 8>> &visit,
                      vector<array<atomic<idx_t>, 8>> &next) {
    // clear next before each iteration
    for (auto i = left; i < right; i++) {
      for (auto j = 0; j < 8; j++) {
        next[i][j] = 0;
      }
    }

    barrier.Wait();

    while (true) {
      auto task = FetchTask();
      if (task.first == task.second) {
        break;
      }
      auto start = task.first;
      auto end = task.second;

      for (auto i = start; i < end; i++) {
        for (auto j = 0; j < 8; j++) {
          if (visit[i][j].load(std::memory_order_relaxed)) {
            for (auto offset = v[i]; offset < v[i + 1]; offset++) {
              auto n = e[offset];
              auto edge_id = edge_ids[offset];
              // lock_guard<mutex> lock(state.global_bfs_state->lock);
              // next[n][j] = next[n][j] | visit[i][j];
              next[n][j].store(next[n][j].load(std::memory_order_relaxed) | visit[i][j].load(std::memory_order_relaxed), std::memory_order_relaxed);
              for (auto l = 0; l < LANE_LIMIT; l++) {
                // parents_v[n][l] = ((parents_v[n][l] == -1) && visit[i][l / 64] & ((idx_t)1 << (l % 64)))
                        // ? i : parents_v[n][l];
                // parents_e[n][l] = ((parents_e[n][l] == -1) && visit[i][l / 64] & ((idx_t)1 << (l % 64)))
                        // ? edge_id : parents_e[n][l];
                parents_v[n][l] = ((parents_v[n][l] == -1) && visit[i][l / 64].load(std::memory_order_relaxed) & ((idx_t)1 << (l % 64)))
                        ? i : parents_v[n][l];
                parents_e[n][l] = ((parents_e[n][l] == -1) && visit[i][l / 64].load(std::memory_order_relaxed) & ((idx_t)1 << (l % 64)))
                        ? edge_id : parents_e[n][l];
              }
            }
          }
        }
      }
    }

    change = false;
    barrier.Wait();

    for (auto i = left; i < right; i++) {
      for (auto j = 0; j < 8; j++) {
        if (next[i][j].load(std::memory_order_relaxed)) {
          next[i][j].store(next[i][j].load(std::memory_order_relaxed) & ~seen[i][j].load(std::memory_order_relaxed), std::memory_order_relaxed);
          seen[i][j].store(seen[i][j].load(std::memory_order_relaxed) | next[i][j].load(std::memory_order_relaxed), std::memory_order_relaxed);
          change |= next[i][j].load(std::memory_order_relaxed);
        }
      }
    }
  }

  void ReachDetect(list_entry_t *result_data, bool& change) {
    auto &bfs_state = state.global_bfs_state;
    int64_t finished_searches = 0;
    // detect lanes that finished
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = bfs_state->lane_to_num[lane];

      if (search_num >= 0) { // active lane
        //! Check if dst for a source has been seen
        int64_t dst_pos = bfs_state->vdata_dst.sel->get_index(search_num);
        if (bfs_state->seen[bfs_state->dst[dst_pos]][lane / 64] & ((idx_t)1 << (lane % 64))) {
          finished_searches++;
        }
      }
    }
    if (finished_searches == LANE_LIMIT) {
      change = false;
    }
    // into the next iteration
    bfs_state->iter++;
  }

  void PathConstruction(Vector &result, list_entry_t *result_data, ValidityMask &result_validity) {
    auto &bfs_state = state.global_bfs_state;
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

private:
  std::chrono::high_resolution_clock::time_point start_time;

public:
  void Schedule() override {
    auto &bfs_state = gstate.global_bfs_state;
		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> bfs_tasks;
    for (idx_t tnum = 0; tnum < bfs_state->num_threads; tnum++) {
      bfs_tasks.push_back(make_uniq<PhysicalShortestPathTopDownTask>(shared_from_this(), context, gstate, tnum));
    }
    start_time = std::chrono::high_resolution_clock::now();
		SetTasks(std::move(bfs_tasks));
  }

  void FinishEvent() override {
    auto &bfs_state = gstate.global_bfs_state;
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
    bfs_state->time_elapsed += duration;

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
    bfs_state->time_elapsed = duration;
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

  static void ShortestPathFunction(const unique_ptr<PathFindingGlobalState::GlobalCompressedSparseRow> &csr,
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
    gstate.global_bfs_state = make_uniq<GlobalBFSState>(all_pairs, csr->v_size - 2, num_threads, context);

    // auto bfs_event = make_shared<SequentialShortestPathEvent>(gstate, pipeline);
    // event.InsertEvent(std::move(std::dynamic_pointer_cast<BasePipelineEvent>(bfs_event)));

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

  // for every batch of pairs, schedule a BFS task
  bfs_state->Clear();

  // remaining pairs
  if (bfs_state->started_searches < gstate.global_tasks->Count()) {

    auto result_data = FlatVector::GetData<int64_t>(bfs_state->result.data[0]);
    auto& result_validity = FlatVector::Validity(bfs_state->result.data[0]);

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
          bfs_state->frontier_size++;
          bfs_state->visit1[bfs_state->src[src_pos]][lane / 64] |= ((idx_t)1 << (lane % 64));
          bfs_state->lane_to_num[lane] = search_num; // active lane
          break;
        }
      }
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
  auto &pf_sink = sink_state->Cast<PathFindingGlobalState>();
  auto &pf_bfs_state = pf_sink.global_bfs_state;
  if (pf_bfs_state->pairs->size() == 0) {
    return SourceResultType::FINISHED;
  }
  pf_bfs_state->result.SetCardinality(*pf_bfs_state->pairs);
  // pf_bfs_state->result.Print();
  string message = "Algorithm running time: " + to_string(pf_bfs_state->time_elapsed.count()) + " us";
  Printer::Print(message);

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
