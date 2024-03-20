//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckpgq/operators/physical_path_finding_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/row_layout.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

class PhysicalPathFinding : public PhysicalComparisonJoin {
#define LANE_LIMIT 512

public:
  class GlobalCompressedSparseRow {
  public:
    GlobalCompressedSparseRow(ClientContext &context,
                              RowLayout &payload_layout){

    };
//    ~GlobalCompressedSparseRow() { delete[] v; }

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

public:
  static constexpr const PhysicalOperatorType TYPE =
      PhysicalOperatorType::EXTENSION;

public:
  PhysicalPathFinding(LogicalExtensionOperator &op,
                      unique_ptr<PhysicalOperator> left,
                      unique_ptr<PhysicalOperator> right);
public:
  vector<unique_ptr<Expression>> expressions;

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


};

} // namespace duckdb
