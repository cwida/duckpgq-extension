#include "duckpgq/core/operator/local_csr/local_csr_event.hpp"

#include <duckpgq/core/option/duckpgq_option.hpp>
#include <fstream>

namespace duckpgq {
namespace core {

LocalCSREvent::LocalCSREvent(shared_ptr<LocalCSRState> local_csr_state_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p, ClientContext &context_p)
    : BasePipelineEvent(pipeline_p), local_csr_state(std::move(local_csr_state_p)), op(op_p), context(context_p) {
}

void LocalCSREvent::Schedule() {
  auto &context = pipeline->GetClientContext();
  vector<shared_ptr<Task>> csr_tasks;
  for (idx_t tnum = 0; tnum < local_csr_state->num_threads; tnum++) {
    csr_tasks.push_back(make_uniq<LocalCSRTask>(
        shared_from_this(), context, local_csr_state, tnum, op));
    local_csr_state->tasks_scheduled++;
  }
  local_csr_state->barrier = make_uniq<Barrier>(local_csr_state->tasks_scheduled);
  SetTasks(std::move(csr_tasks));
}

void LocalCSREvent::FinishEvent() {
  // Assume at least one partition exists
  D_ASSERT(!local_csr_state->partition_csrs.empty());

  std::sort(local_csr_state->partition_csrs.begin(), local_csr_state->partition_csrs.end(),
          [](const shared_ptr<LocalCSR>& a, const shared_ptr<LocalCSR>& b) {
              return a->GetEdgeSize() > b->GetEdgeSize();  // Sort by edge count
          });

  // size_t vertex_count = local_csr_state->partition_csrs[0]->GetVertexSize();
  //
  // // Create a filename with the number of vertices
  // std::string filename = "partition_stats_" + std::to_string(vertex_count) + "_vertices.csv";
  // std::ofstream outfile(filename);
  // outfile << "PartitionID,StartVertex,EndVertex,VertexCount,EdgeCount,EdgePerVertex,VertexMemBytes,EdgeMemBytes,TotalMemBytes\n";
  //
  // idx_t partition_id = 0;
  // for (const auto &local_csr : local_csr_state->partition_csrs) {
  //   auto edge_count = local_csr->GetEdgeSize();
  //   double edge_per_vertex = vertex_count > 0 ? static_cast<double>(edge_count) / vertex_count : 0.0;
  //
  //   size_t vertex_mem = local_csr->v_array_size * sizeof(std::atomic<uint32_t>);
  //   size_t edge_mem = local_csr->e.capacity() * sizeof(uint16_t);
  //   size_t total_mem = vertex_mem + edge_mem;
  //
  //   outfile << partition_id << ","
  //           << local_csr->start_vertex << ","
  //           << local_csr->end_vertex << ","
  //           << vertex_count << ","
  //           << edge_count << ","
  //           << edge_per_vertex << ","
  //           << vertex_mem << ","
  //           << edge_mem << ","
  //           << total_mem << "\n";
  //
  //   partition_id++;
  // }
  //
  // outfile.close();
}

} // namespace core
} // namespace duckpgq