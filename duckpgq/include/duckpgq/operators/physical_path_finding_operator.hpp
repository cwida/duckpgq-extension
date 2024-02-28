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

struct GlobalCompressedSparseRow;

class PhysicalPathFinding : public PhysicalComparisonJoin {
public:
  class LocalCompressedSparseRow {
  public:
    LocalCompressedSparseRow(ClientContext &context,
                             const PhysicalPathFinding &op);

    void Sink(DataChunk &input, GlobalCompressedSparseRow &global_csr);


    //! The hosting operator
    const PhysicalPathFinding &op;
    //! Holds a vector of incoming columns
    DataChunk keys;
    //! Local copy of the expression executor
    ExpressionExecutor executor

  };

  class GlobalCompressedSparseRow {
  public:
    GlobalCompressedSparseRow(ClientContext &context,
                              RowLayout &payload_layout){

    };
    ~GlobalCompressedSparseRow() { delete[] v; }

    atomic<int64_t> *v;
    vector<int64_t> e;
    vector<int64_t> edge_ids;
    vector<int64_t> w;
    vector<double> w_double;
    bool initialized_v = false;
    bool initialized_e = false;
    bool initialized_w = false;
    size_t vsize;
  };

public:
  static constexpr const PhysicalOperatorType TYPE =
      PhysicalOperatorType::EXTENSION;

public:
  PhysicalPathFinding(LogicalExtensionOperator &op,
                      unique_ptr<PhysicalOperator> left,
                      unique_ptr<PhysicalOperator> right);

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
