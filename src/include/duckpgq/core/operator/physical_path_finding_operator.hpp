//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckpgq/core/operator/physical_path_finding_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckpgq/common.hpp"
#include "duckdb/common/types/row/row_layout.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckpgq/core/utils/duckpgq_barrier.hpp"
#include "duckpgq/core/utils/duckpgq_path_reconstruction.hpp"

namespace duckpgq {

namespace core {

class PhysicalPathFinding : public PhysicalComparisonJoin {
#define LANE_LIMIT 512

public:
  class GlobalCompressedSparseRow {
  public:
    explicit GlobalCompressedSparseRow(ClientContext &context){
    };
    ~GlobalCompressedSparseRow() {
      if (v) {
        delete[] v;
      }
    }

    atomic<int64_t> *v;
    vector<int64_t> e;
    vector<int64_t> edge_ids;
    vector<int64_t> w;
    vector<double> w_double;
    bool initialized_v = false;
    bool initialized_e = false;
    bool initialized_w = false;
    size_t v_size;
    bool is_ready = false;

    std::mutex csr_lock;
  public:
    void InitializeVertex(int64_t v_size);
    void InitializeEdge(int64_t e_size);
    void Print() {
      string result;
      result += "CSR:\nV: ";
      if (initialized_v) {
        for (idx_t i = 0; i < v_size; ++i) {
          result += std::to_string(v[i]) + ' ';
        }
      } else {
        result += "not initialized";
      }
      result += "\nE: ";
      if (initialized_e) {
        for (auto i : e) {
          result += std::to_string(i) + " ";
        }
      } else {
        result += "not initialized";
      }
      result += "\nEdge IDs: ";
      if (initialized_e) {
        for (auto i : edge_ids) {
          result += std::to_string(i) + " ";
        }
      } else {
        result += "not initialized";
      }
      result += "\nW: ";
      if (initialized_w) {
        for (auto i : w) {
          result += std::to_string(i) + " ";
        }
      } else {
        result += "not initialized";
      }
      Printer::Print(result);
    };
  };

  class LocalCompressedSparseRow {
  public:
    LocalCompressedSparseRow(ClientContext &context,
                             const PhysicalPathFinding &op);



    static bool IterativeLength(int64_t v_size, int64_t *v, vector<int64_t> &e,
                    vector<std::bitset<LANE_LIMIT>> &seen,
                    vector<std::bitset<LANE_LIMIT>> &visit,
                    vector<std::bitset<LANE_LIMIT>> &next);

    //! The hosting operator
    const PhysicalPathFinding &op;
    //! Local copy of the expression executor
    ExpressionExecutor executor;
    //! Final result for the path-finding pairs
    DataChunk local_results;
  };

  PhysicalPathFinding(LogicalExtensionOperator &op,
                      unique_ptr<PhysicalOperator> left,
                      unique_ptr<PhysicalOperator> right);

    static constexpr PhysicalOperatorType TYPE =
      PhysicalOperatorType::EXTENSION;
  vector<unique_ptr<Expression>> expressions;
  string mode; // "iterativelength" or "shortestpath"


public:
  // CachingOperator Interface
  OperatorResultType ExecuteInternal(ExecutionContext &context,
                                     DataChunk &input, DataChunk &chunk,
                                     GlobalOperatorState &gstate,
                                     OperatorState &state) const override;

public:
  // Source interface
  unique_ptr<LocalSourceState>
  GetLocalSourceState(ExecutionContext &context,
                      GlobalSourceState &gstate) const override;
  unique_ptr<GlobalSourceState>
  GetGlobalSourceState(ClientContext &context) const override;
  SourceResultType GetData(ExecutionContext &context, DataChunk &chunk,
                           OperatorSourceInput &input) const override;

  bool IsSource() const override { return true; }
  bool ParallelSource() const override { return true; }

public:
  // Sink Interface
  unique_ptr<GlobalSinkState>
  GetGlobalSinkState(ClientContext &context) const override;
  unique_ptr<LocalSinkState>
  GetLocalSinkState(ExecutionContext &context) const override;
  SinkResultType Sink(ExecutionContext &context, DataChunk &chunk,
                      OperatorSinkInput &input) const override;
  SinkCombineResultType Combine(ExecutionContext &context,
                                OperatorSinkCombineInput &input) const override;
  SinkFinalizeType Finalize(Pipeline &pipeline, Event &event,
                            ClientContext &context,
                            OperatorSinkFinalizeInput &input) const override;

  bool IsSink() const override { return true; }
  bool ParallelSink() const override { return true; }

  void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

  // //! Schedules tasks to calculate the next iteration of the path-finding
	static void ScheduleBFSEvent(Pipeline &pipeline, Event &event, GlobalSinkState &state);
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PathFindingLocalState : public LocalSinkState {
public:
  using GlobalCompressedSparseRow =
      PhysicalPathFinding::GlobalCompressedSparseRow;
  PathFindingLocalState(ClientContext &context, const PhysicalPathFinding &op,
                        idx_t child);

  void Sink(DataChunk &input, GlobalCompressedSparseRow &global_csr,
            idx_t child);

  static void CreateCSRVertex(DataChunk &input,
                              GlobalCompressedSparseRow &global_csr);

  ColumnDataCollection local_tasks;
  ColumnDataCollection local_inputs;
};

class GlobalBFSState {
  using GlobalCompressedSparseRow =
      PhysicalPathFinding::GlobalCompressedSparseRow;

public:
  GlobalBFSState(shared_ptr<GlobalCompressedSparseRow> csr_,
                 shared_ptr<DataChunk> pairs_, int64_t v_size_,
                 idx_t num_threads_, idx_t mode_, ClientContext &context_);

  void Clear();

  void CreateTasks();

  void InitTask(idx_t worker_id);

  pair<idx_t, idx_t> FetchTask(idx_t worker_id);

  pair<idx_t, idx_t> BoundaryCalculation(idx_t worker_id) const;

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
  ClientContext &context;
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
                         const PhysicalPathFinding &op);

  PathFindingGlobalState(PathFindingGlobalState &prev);

  void Sink(DataChunk &input, PathFindingLocalState &lstate);

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

} // namespace core
} // namespace duckpgq

