#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/option/duckpgq_option.hpp>
#include <duckpgq/core/utils/duckpgq_barrier.hpp>
#include <duckpgq/core/utils/duckpgq_path_reconstruction.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {
class PhysicalPathFinding; // Forward declaration

class LocalCSR {
public:
  explicit LocalCSR(const CSR &global_csr, size_t start_v, size_t end_v) {
    // Share the vertex array (global_v) but store a subset
    global_v = global_csr.v; // Still referencing the global `v` array

    // Store offsets of `e` array for this LocalCSR
    v_offset = start_v; // Start offset in the global `v` array
    v_end = end_v;

    size_t start_e = global_csr.v[start_v];
    size_t end_e = (end_v < global_csr.vsize)
        ? static_cast<size_t>(global_csr.v[end_v].load())  // Explicit cast + atomic load
        : global_csr.e.size();

    e.assign(global_csr.e.begin() + start_e, global_csr.e.begin() + end_e);
    edge_ids.assign(global_csr.edge_ids.begin() + start_e, global_csr.edge_ids.begin() + end_e);

    if (global_csr.initialized_w) {
      w.assign(global_csr.w.begin() + start_e, global_csr.w.begin() + end_e);
      w_double.assign(global_csr.w_double.begin() + start_e, global_csr.w_double.begin() + end_e);
    }

    initialized_v = global_csr.initialized_v;
    initialized_e = true;
    initialized_w = global_csr.initialized_w;
    vsize = end_v - start_v;
  }

  atomic<int64_t> *global_v{};  // Pointer to global vertex array
  size_t v_offset;              // Start of the local CSR's vertex region
  size_t v_end;              // Start of the local CSR's vertex region

  vector<int64_t> e;       // Thread-specific edges
  vector<int64_t> edge_ids; // Corresponding edge IDs
  vector<int64_t> w;        // Weights (if used)
  vector<double> w_double;  // Alternative weight representation

  bool initialized_v = false;
  bool initialized_e = false;
  bool initialized_w = false;
  size_t vsize{};


  // void ComputeLocalV(const CSR &global_csr, size_t start_idx, size_t end_idx) {
  //   local_v.resize(global_csr.vsize + 1, 0);  // Initialize offsets to 0
  //
  //   size_t local_edge_index = 0;  // Tracks position in local `e`
  //   for (size_t i = 0; i < global_csr.vsize; i++) {
  //     size_t global_edge_start = global_csr.v[i];
  //     size_t global_edge_end = (i + 1 < global_csr.vsize)
  //         ? static_cast<size_t>(global_csr.v[i + 1])  // Explicit cast
  //         : global_csr.e.size();
  //     // Count edges that fall within this thread's assigned range
  //     size_t edge_count = 0;
  //     for (size_t j = global_edge_start; j < global_edge_end; j++) {
  //       if (j >= start_idx && j < end_idx) {
  //         edge_count++;
  //       }
  //     }
  //
  //     // Assign correct offset for this vertex
  //     local_v[i] = local_edge_index;
  //     local_edge_index += edge_count; // Move forward by the number of edges in this LocalCSR
  //   }
  //
  //   // Ensure last entry matches total number of edges in this LocalCSR
  //   local_v[global_csr.vsize] = local_edge_index;
  // }

  std::string ToString() const {
    std::ostringstream oss;
    oss << "LocalCSR { \n"
        << "  vsize: " << vsize << "\n"
        << "  initialized_v: " << initialized_v << "\n"
        << "  initialized_e: " << initialized_e << "\n"
        << "  initialized_w: " << initialized_w << "\n";

    // Print a limited number of edges to keep output readable
    oss << "  v: [";
    for (size_t i = v_offset; i < std::min(v_offset + vsize, v_offset + size_t(10)); i++) {
      oss << global_v[i] << (i < v_offset + vsize - 1 ? ", " : "");
    }
    if (vsize > 10) oss << "...";
    oss << "]\n";
    // Print a limited number of edges to keep output readable
    oss << "  e: [";
    for (size_t i = 0; i < std::min(e.size(), size_t(10)); i++) {
      oss << e[i] << (i < e.size() - 1 ? ", " : "");
    }
    if (e.size() > 10) oss << "...";
    oss << "]\n";

    oss << "  edge_ids: [";
    for (size_t i = 0; i < std::min(edge_ids.size(), size_t(10)); i++) {
      oss << edge_ids[i] << (i < edge_ids.size() - 1 ? ", " : "");
    }
    if (edge_ids.size() > 10) oss << "...";
    oss << "]\n";

    if (initialized_w) {
      oss << "  w: [";
      for (size_t i = 0; i < std::min(w.size(), size_t(10)); i++) {
        oss << w[i] << (i < w.size() - 1 ? ", " : "");
      }
      if (w.size() > 10) oss << "...";
      oss << "]\n";

      oss << "  w_double: [";
      for (size_t i = 0; i < std::min(w_double.size(), size_t(10)); i++) {
        oss << w_double[i] << (i < w_double.size() - 1 ? ", " : "");
      }
      if (w_double.size() > 10) oss << "...";
      oss << "]\n";
    }

    oss << "}\n";
    return oss.str();
  }
};



class BFSState : public enable_shared_from_this<BFSState> {
public:
  BFSState(shared_ptr<DataChunk> pairs_, CSR* csr_, idx_t num_threads_,
           string mode_, ClientContext &context_);

  virtual ~BFSState();

  virtual void ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op);
  void InitializeLanes();
  virtual void Clear();
  void CreateThreadLocalCSRs(); // Generates LocalCSRs


  // Common members
  shared_ptr<DataChunk> pairs;
  CSR *csr;
  vector<unique_ptr<LocalCSR>> local_csrs; // Each thread gets one LocalCSR
  string mode;
  shared_ptr<DataChunk> pf_results;
  LogicalType bfs_type;
  int64_t iter;
  int64_t v_size;
  bool change;
  idx_t started_searches;
  int64_t *src;
  Vector &src_data;
  Vector &dst_data;
  int64_t *dst;
  int64_t lane_to_num[LANE_LIMIT];

  std::vector<int64_t> thread_assignment;
  UnifiedVectorFormat vdata_src;
  UnifiedVectorFormat vdata_dst;
  idx_t active = 0;
  ClientContext &context;
  idx_t total_pairs_processed;
  idx_t num_threads;
  unique_ptr<Barrier> barrier;
  mutex change_lock;

  size_t current_batch_path_list_len;
  vector<bitset<LANE_LIMIT>> seen;
  vector<bitset<LANE_LIMIT>> visit1;
  vector<bitset<LANE_LIMIT>> visit2;

  bitset<LANE_LIMIT> lane_completed;
};

} // namespace core

} // namespace duckpgq