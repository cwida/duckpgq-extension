#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckpgq/core/operator/bfs_state.hpp"
#include <duckpgq/core/operator/iterative_length/iterative_length_state.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <duckpgq_state.hpp>
#include <thread>
#include <algorithm>
#include <cmath> // for std::sqrt
#include <duckpgq/core/option/duckpgq_option.hpp>
#include <fstream>
#include <numeric> // for std::accumulate

namespace duckpgq {

namespace core {


PhysicalPathFinding::PhysicalPathFinding(LogicalExtensionOperator &op,
                                         unique_ptr<PhysicalOperator> pairs,
                                         unique_ptr<PhysicalOperator> csr)
    : PhysicalComparisonJoin(op, TYPE, {}, JoinType::INNER, op.estimated_cardinality) {
  children.push_back(std::move(pairs));
  children.push_back(std::move(csr));
  expressions = std::move(op.expressions);
  estimated_cardinality = op.estimated_cardinality;
  auto &path_finding_op = op.Cast<LogicalPathFindingOperator>();
  mode = path_finding_op.mode;
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
    //   - For example: group by source, list(destination)
    local_pairs.Append(input);
  }
}


PathFindingGlobalSinkState::PathFindingGlobalSinkState(ClientContext &context,
                       const PhysicalPathFinding &op) : context_(context) {
  global_pairs =
      make_uniq<ColumnDataCollection>(context, op.children[0]->GetTypes());
  global_csr_column_data =
      make_uniq<ColumnDataCollection>(context, op.children[1]->GetTypes());

  global_pairs->InitializeScan(global_scan_state);
  result_scan_idx = 0;

  child = 0;
  mode = op.mode;
  auto &scheduler = TaskScheduler::GetScheduler(context);
  num_threads = scheduler.NumberOfThreads();
}

#define PARTITION_SIZE 500000  // Adjust between 100K-500K based on experiments

void FillLocalCSR(shared_ptr<LocalCSR> &local_csr, idx_t start_vertex, idx_t end_vertex, CSR* csr) {
    idx_t v_offset = 0;
    local_csr->v.reserve(csr->vsize);

    for (idx_t j = 0; j < csr->vsize - 1; j++) {
        local_csr->v.push_back(v_offset);  // Store vertex offset
        for (idx_t e_offset = csr->v[j]; e_offset < csr->v[j + 1]; e_offset++) {
            auto dst = csr->e[e_offset];
            if (dst >= start_vertex && dst < end_vertex) {
                v_offset++;
                local_csr->e.push_back(dst);
            }
        }
    }
    local_csr->v.push_back(v_offset);  // Final offset for last vertex
}

// Recursive function to partition graph dynamically
void PathFindingGlobalSinkState::PartitionGraph(idx_t start_vertex, idx_t end_vertex) {
    // Calculate total edge count in this range
    idx_t edge_count = 0;
    for (idx_t v = start_vertex; v < end_vertex; v++) {
        edge_count += (csr->v[v + 1] - csr->v[v]);
    }

    if (edge_count <= PARTITION_SIZE) {
        // Acceptable partition size → create local CSR
        auto local_csr = make_shared_ptr<LocalCSR>();
        FillLocalCSR(local_csr, start_vertex, end_vertex, csr);
        if (!local_csr->e.empty()) {
            local_csrs.push_back(local_csr);
            partition_ranges.emplace_back(start_vertex, end_vertex);
        }
    } else {
        // Too large → Try to split into two smaller partitions
        idx_t mid_vertex = (start_vertex + end_vertex) / 2;

        // **Prevent infinite splitting**
        if (mid_vertex == start_vertex || mid_vertex == end_vertex) {
            // We cannot split further → **Keep this large partition**
            auto local_csr = make_shared_ptr<LocalCSR>();
            FillLocalCSR(local_csr, start_vertex, end_vertex, csr);
            if (!local_csr->e.empty()) {
              local_csrs.push_back(local_csr);
              partition_ranges.emplace_back(start_vertex, end_vertex);
            }
            return;
        }

        // Recursive partitioning
        PartitionGraph(start_vertex, mid_vertex);
        PartitionGraph(mid_vertex, end_vertex);
    }
}

// Entry function: Initialize partitioning
void PathFindingGlobalSinkState::CreateThreadLocalCSRs() {
    local_csrs.clear();
    partition_ranges.clear();

    idx_t total_vertices = csr->vsize - 2;  // Number of vertices

    // Start recursive partitioning
    PartitionGraph(0, total_vertices);

    // Optional: Sort partitions by edge size (for load balancing)
    std::sort(local_csrs.begin(), local_csrs.end(),
              [](const shared_ptr<LocalCSR>& a, const shared_ptr<LocalCSR>& b) {
                  return a->GetEdgeSize() > b->GetEdgeSize();  // Sort by edge count
              });

    // Printer::PrintF("Number of partitions %d", local_csrs.size());
    // for (const auto &local_csr : local_csrs) {
    //   Printer::PrintF("Vsize: %d, esize: %d, partition_size: %d", local_csr->GetVertexSize(), local_csr->GetEdgeSize(), PARTITION_SIZE);
    // }

    // for (const auto &partition_ranges : partition_ranges) {
      // Printer::PrintF("start %d, end %d", local_csr->getvertexsize(), local_csr->getedgesize(), partition_size);
    // }
}

//   // Compute balance statistics
  //   idx_t min_edges = *std::min_element(edges_per_partition.begin(), edges_per_partition.end());
  //   idx_t max_edges = *std::max_element(edges_per_partition.begin(), edges_per_partition.end());
  //   idx_t avg_edges = std::accumulate(edges_per_partition.begin(), edges_per_partition.end(), 0) / total_partitions;
  //
  //   double variance = 0.0;
  //   for (auto edges : edges_per_partition) {
  //       variance += std::pow(edges - avg_edges, 2);
  //   }
  //   variance /= total_partitions;
  //   double std_dev = std::sqrt(variance);
  //
  // std::ofstream log_file("partition_metrics.csv");
  // if (log_file.is_open()) {
  //   log_file << "total_vertices,total_edges,total_partitions,min_edges,max_edges,avg_edges,std_dev\n";
  //   log_file << total_vertices << ","
  //            << total_edges << ","
  //            << total_partitions << ","
  //            << min_edges << ","
  //            << max_edges << ","
  //            << avg_edges << ","
  //            << std_dev << "\n\n";
  //
  //   log_file << "partition_id,edges\n";
  //   for (size_t i = 0; i < edges_per_partition.size(); i++) {
  //     log_file << i << "," << edges_per_partition[i] << "\n";
  //   }
  //   log_file.close();
  // } else {
  //   std::cerr << "Error opening log file for writing metrics.\n";
  // }
// }


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
  auto duckpgq_state = GetDuckPGQState(context);
  if (gstate.csr == nullptr) {
    throw InternalException("CSR not initialized");
  }

  // Check if we have to do anything for CSR child
  if (gstate.child == 0) {
    ++gstate.child;
    gstate.CreateThreadLocalCSRs();
    return SinkFinalizeType::READY;
  }
  if (gstate.global_pairs->Count() == 0) {
    return SinkFinalizeType::READY;
  }

  while (gstate.global_scan_state.next_row_index < gstate.global_pairs->Count()) {
    // Schedule the BFS Event for the current DataChunk
    auto current_chunk = make_shared_ptr<DataChunk>();
    current_chunk->Initialize(context, gstate.global_pairs->Types());
    gstate.global_pairs->Scan(gstate.global_scan_state, *current_chunk);
    if (gstate.mode == "iterativelength") {

      auto bfs_state = make_shared_ptr<IterativeLengthState>(current_chunk, gstate.local_csrs, gstate.partition_ranges, gstate.num_threads, context, gstate.csr->vsize);
      bfs_state->ScheduleBFSBatch(pipeline, event, this);
      gstate.bfs_states.push_back(std::move(bfs_state));
    } else if (gstate.mode == "shortestpath") {
      // TODO(dtenwolde) implement also for shortest path
      throw NotImplementedException("Shortest path operator has not been implemented yet.");
    } else {
      throw InvalidInputException("Unknown mode specified %s", gstate.mode);
    }
    }

  // Move to the next input child
  ++gstate.child;
  duckpgq_state->csr_to_delete.insert(gstate.csr_id);
  return SinkFinalizeType::READY;
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

public:
  idx_t MaxThreads() override {
    return 1;
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

SourceResultType PhysicalPathFinding::GetData(ExecutionContext &context, DataChunk &result,
                             OperatorSourceInput &input) const {
  auto &pf_sink = sink_state->Cast<PathFindingGlobalSinkState>();
  // pf_sink.global_bfs_state->path_finding_result->SetCardinality(pf_sink.global_bfs_state->pairs->Count());
  // pf_sink.global_bfs_state->path_finding_result->Print();
  // If there are no pairs, we're done
  if (pf_sink.global_pairs->Count() == 0) {
    return SourceResultType::FINISHED;
  }
  D_ASSERT(pf_sink.result_scan_idx < pf_sink.bfs_states.size());
  auto current_state = pf_sink.bfs_states[pf_sink.result_scan_idx];
  auto result_types = current_state->pairs->GetTypes();
  result_types.push_back(current_state->bfs_type);
  current_state->pf_results->SetCardinality(*current_state->pairs);
  current_state->pairs->Fuse(*current_state->pf_results);
  result.Move(*current_state->pairs);

  pf_sink.result_scan_idx++;
  if (pf_sink.result_scan_idx == pf_sink.bfs_states.size()) {
    return SourceResultType::FINISHED;
  }
  return SourceResultType::HAVE_MORE_OUTPUT;
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
