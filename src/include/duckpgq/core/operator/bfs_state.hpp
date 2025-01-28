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

class BFSState : public enable_shared_from_this<BFSState> {

public:
  BFSState(shared_ptr<DataChunk> pairs_, CSR* csr_, idx_t num_threads_,
        string mode_, ClientContext &context_);

  // void InitializeBFS(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op);
  void ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op);
  void InitializeLanes();
  void Clear();

  pair<idx_t, idx_t> BoundaryCalculation(idx_t worker_id) const;
  shared_ptr<DataChunk> pairs; // (src, dst) pairs
  CSR *csr;
  string mode;
  shared_ptr<DataChunk> pf_results; // results of path-finding
  LogicalType bfs_type;
  int64_t iter;
  int64_t v_size; // Number of vertices
  idx_t v_cap;
  bool change;
  idx_t started_searches; // Number of started searches in current batch
  int64_t *src;
  Vector &src_data;
  Vector &dst_data;
  int64_t *dst;

  std::vector<int64_t> thread_assignment;
  int64_t magic_assignment;

  UnifiedVectorFormat vdata_src;
  UnifiedVectorFormat vdata_dst;
  int64_t lane_to_num[LANE_LIMIT];
  idx_t active = 0;
  size_t current_batch_path_list_len; // Length of the current batch path list
  ClientContext &context;

  vector<std::bitset<LANE_LIMIT>> seen;
  vector<std::bitset<LANE_LIMIT>> visit1;
  vector<std::bitset<LANE_LIMIT>> visit2;
  vector<std::array<ve, LANE_LIMIT>> parents_ve;

  std::bitset<LANE_LIMIT> lane_completed;

  idx_t total_pairs_processed;
  idx_t num_threads;
  unique_ptr<Barrier> barrier;

  // lock for next
  mutex change_lock;
};

} // namespace core

} // namespace duckpgq