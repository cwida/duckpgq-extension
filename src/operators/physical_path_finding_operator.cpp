#include "duckpgq/operators/physical_path_finding_operator.hpp"

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include <thread>
#include <cmath>
#include <algorithm>

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

class GlobalBFSState {
public:

  void init(shared_ptr<DataChunk> pairs_, int64_t v_size_) {
    iter = 1;
    v_size = v_size_;
    pairs = pairs_;
    auto &src_data = pairs->data[0];
    auto &dst_data = pairs->data[1];
    src_data.ToUnifiedFormat(pairs->size(), vdata_src);
    dst_data.ToUnifiedFormat(pairs->size(), vdata_dst);
    src = FlatVector::GetData<int64_t>(src_data);
    dst = FlatVector::GetData<int64_t>(dst_data);
    started_searches = 0;
    for (auto i = 0; i < LANE_LIMIT; i++) {
      lane_to_num[i] = -1;
    }
    seen.resize(v_size_);
    visit1.resize(v_size_);
    visit2.resize(v_size_);
    for (auto i = 0; i < v_size; i++) {
      seen[i] = 0;
      visit1[i] = 0;
    }
    change = false;
    result = make_uniq<Vector>(LogicalType::BIGINT, true, true, pairs->size());
  }

  void clear() {
    iter = 1;
    change = false;
    for (auto i = 0; i < LANE_LIMIT; i++) {
      lane_to_num[i] = -1;
    }
    // empty visit vectors
    for (auto i = 0; i < v_size; i++) {
      seen[i] = 0;
      visit1[i] = 0;
    }
  }
public:
  shared_ptr<DataChunk> pairs;
  int64_t iter;
  int64_t v_size;
  int64_t *src;
  int64_t *dst;
  UnifiedVectorFormat vdata_src;
  UnifiedVectorFormat vdata_dst;
  idx_t started_searches;
  int64_t lane_to_num[LANE_LIMIT];
  vector<std::bitset<LANE_LIMIT>> seen;
  vector<std::bitset<LANE_LIMIT>> visit1;
  vector<std::bitset<LANE_LIMIT>> visit2;
  bool change;
  unique_ptr<Vector> result;
};

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
      global_csr(std::move(prev.global_csr)), 
      global_bfs_state(std::move(prev.global_bfs_state)), child(prev.child + 1) {
  }

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
  // state for BFS
  GlobalBFSState global_bfs_state;

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

class PhysicalBFSTask : public ExecutorTask {
public:
	PhysicalBFSTask(shared_ptr<Event> event_p, ClientContext &context, PathFindingGlobalState &state, idx_t start, idx_t end)
	    : ExecutorTask(context, std::move(event_p)), context(context), state(state), start(start), end(end) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
    auto& change = state.global_bfs_state.change;
    auto& seen = state.global_bfs_state.seen;
    auto& visit = state.global_bfs_state.iter & 1 ? state.global_bfs_state.visit1 : state.global_bfs_state.visit2;
    auto& next = state.global_bfs_state.iter & 1 ? state.global_bfs_state.visit2 : state.global_bfs_state.visit1;
    int64_t *v = (int64_t *)state.global_csr->v;
    vector<int64_t> &e = state.global_csr->e;

    for (auto i = start; i < end; i++) {
      next[i] = 0;
    }
    for (auto i = start; i < end; i++) {
      if (visit[i].any()) {
        for (auto offset = v[i]; offset < v[i + 1]; offset++) {
          auto n = e[offset];
          next[n] = next[n] | visit[i];

          next[n] = next[n] & ~seen[n];
          seen[n] = seen[n] | next[n];
          change |= next[n].any();
        }
      }
    }
    // for (auto i = start; i < end; i++) {
    //   next[i] = next[i] & ~seen[i];
    //   seen[i] = seen[i] | next[i];
    //   change |= next[i].any();
    // }

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	ClientContext &context;
	PathFindingGlobalState &state;
  // [start, end)
  idx_t start;
  idx_t end;
};

class BFSIterativeEvent : public BasePipelineEvent {
public:
	BFSIterativeEvent(PathFindingGlobalState &gstate_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p) {
	}

	PathFindingGlobalState &gstate;

public:
	void Schedule() override {
    auto &bfs_state = gstate.global_bfs_state;
		auto &context = pipeline->GetClientContext();

    bfs_state.change = false;

		// Schedule tasks equal to the number of threads, which will each merge multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		// idx_t num_threads = ts.NumberOfThreads();
    idx_t num_threads = std::min((int64_t)ts.NumberOfThreads(), bfs_state.v_size);
    idx_t blocks = floor(bfs_state.v_size / (float)num_threads);

		vector<shared_ptr<Task>> bfs_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
      bfs_tasks.push_back(make_uniq<PhysicalBFSTask>(shared_from_this(), context, gstate, 
        tnum * blocks, std::min(tnum * blocks + blocks, (idx_t)bfs_state.v_size)));
		}
		SetTasks(std::move(bfs_tasks));
	}

	void FinishEvent() override {
		auto& bfs_state = gstate.global_bfs_state;

    auto result_data = FlatVector::GetData<int64_t>(*bfs_state.result);
    ValidityMask &result_validity = FlatVector::Validity(*bfs_state.result);

		if (bfs_state.change) {
      // detect lanes that finished
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        int64_t search_num = bfs_state.lane_to_num[lane];
        if (search_num >= 0) { // active lane
          int64_t dst_pos = bfs_state.vdata_dst.sel->get_index(search_num);
          if (bfs_state.seen[bfs_state.dst[dst_pos]][lane]) {
            result_data[search_num] =
                bfs_state.iter;               /* found at iter => iter = path length */
            bfs_state.lane_to_num[lane] = -1; // mark inactive
          }
        }
      }
      // into the next iteration
      bfs_state.iter++;
      auto bfs_event = std::make_shared<BFSIterativeEvent>(gstate, *pipeline);
      this->InsertEvent(std::dynamic_pointer_cast<BasePipelineEvent>(bfs_event));
		} else {
      // no changes anymore: any still active searches have no path
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        int64_t search_num = bfs_state.lane_to_num[lane];
        if (search_num >= 0) { // active lane
          result_validity.SetInvalid(search_num);
          result_data[search_num] = (int64_t)-1; /* no path */
          bfs_state.lane_to_num[lane] = -1;     // mark inactive
        }
      }

      // if remaining pairs, schedule the BFS for the next batch
      if (bfs_state.started_searches < gstate.global_tasks.Count()) {
        PhysicalPathFinding::ScheduleBFSTasks(*pipeline, *this, gstate);
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
  if (global_tasks.Count() != 0) {
    auto all_pairs = make_shared<DataChunk>();
    DataChunk pairs;
    global_tasks.InitializeScanChunk(*all_pairs);
    global_tasks.InitializeScanChunk(pairs);
    ColumnDataScanState scan_state;
    global_tasks.InitializeScan(scan_state);
    while (global_tasks.Scan(scan_state, pairs)) {
      all_pairs->Append(pairs, true);
    }
    // debug print
    all_pairs->Print();
    gstate.global_bfs_state.init(all_pairs, csr->v_size);

    // Schedule the first round of BFS tasks
    if (all_pairs->size() > 0) {
      ScheduleBFSTasks(pipeline, event, gstate);
    }

  }


	// Move to the next input child
	++gstate.child;

  return SinkFinalizeType::READY;
}

void PhysicalPathFinding::ScheduleBFSTasks(Pipeline &pipeline, Event &event, GlobalSinkState &state) {
  auto &gstate = state.Cast<PathFindingGlobalState>();
  auto &bfs_state = gstate.global_bfs_state;

  // for every batch of pairs, schedule a BFS task
  bfs_state.clear();

  // remaining pairs
  if (bfs_state.started_searches < gstate.global_tasks.Count()) {

    auto result_data = FlatVector::GetData<int64_t>(*bfs_state.result);
    auto& result_validity = FlatVector::Validity(*bfs_state.result);

    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      bfs_state.lane_to_num[lane] = -1;
      while (bfs_state.started_searches < gstate.global_tasks.Count()) {
        int64_t search_num = bfs_state.started_searches++;
        int64_t src_pos = bfs_state.vdata_src.sel->get_index(search_num);
        int64_t dst_pos = bfs_state.vdata_dst.sel->get_index(search_num);
        if (!bfs_state.vdata_src.validity.RowIsValid(src_pos)) {
          result_validity.SetInvalid(search_num);
          result_data[search_num] = (uint64_t)-1; /* no path */
        } else if (bfs_state.src[src_pos] == bfs_state.dst[dst_pos]) {
          result_data[search_num] =
              (uint64_t)0; // path of length 0 does not require a search
        } else {
          bfs_state.visit1[bfs_state.src[src_pos]][lane] = true;
          bfs_state.lane_to_num[lane] = search_num; // active lane
          break;
        }
      }
    }

    auto bfs_event = make_shared<BFSIterativeEvent>(gstate, pipeline);
    event.InsertEvent(std::move(std::dynamic_pointer_cast<BasePipelineEvent>(bfs_event)));
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
  auto &pf_bfs_state = pf_sink.global_bfs_state;
  // auto &pf_lstate = input.local_state.Cast<PathFindingLocalSourceState>();
  pf_bfs_state.result->Print(pf_bfs_state.pairs->size());
  // pf_gstate.Initialize(pf_sink);

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

