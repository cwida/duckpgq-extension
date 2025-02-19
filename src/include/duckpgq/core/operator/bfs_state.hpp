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
  explicit LocalCSR(std::vector<int64_t> &v_, std::vector<int64_t> &e_) :
    v(v_), e(e_) {}

  std::vector<int64_t> v;
  std::vector<int64_t> e;       // Thread-specific edges

  std::string ToString() const {
    std::ostringstream oss;
    oss << "LocalCSR { \n"
        << "  vsize: " << v.size() << "\n"
        << "  esize: " << e.size() << "\n";

    // Print a limited number of edges to keep output readable
    oss << "  v: [";
    for (size_t i = 0; i < v.size(); i++) {
      oss << v[i] << (i < v.size() - 1 ? ", " : "");
    }
    oss << "]\n";
    // Print a limited number of edges to keep output readable
    oss << "  e: [";
    for (size_t i = 0; i < e.size(); i++) {
      oss << e[i] << (i < e.size() - 1 ? ", " : "");
    }
    oss << "]\n";
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
  std::vector<std::pair<idx_t, idx_t>> partition_ranges;
  atomic<int64_t> partition_counter;
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
  mutex local_csr_lock;
  atomic<int64_t> local_csr_counter;
  size_t current_batch_path_list_len;
  vector<bitset<LANE_LIMIT>> seen;
  vector<bitset<LANE_LIMIT>> visit1;
  vector<bitset<LANE_LIMIT>> visit2;

  bitset<LANE_LIMIT> lane_completed;
};

} // namespace core

} // namespace duckpgq