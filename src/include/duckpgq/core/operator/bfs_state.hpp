#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/utils/duckpgq_barrier.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <thread>

namespace duckpgq {

namespace core {
class PhysicalPathFinding; // Forward declaration


class BFSState : public enable_shared_from_this<BFSState> {
public:
  BFSState(const shared_ptr<DataChunk> &pairs_, std::vector<shared_ptr<LocalCSR>> &local_csrs_, std::vector<std::pair<idx_t, idx_t>> &partition_ranges_, idx_t num_threads_,
           string mode_, ClientContext &context_, int64_t vsize_);

  virtual ~BFSState();

  virtual void ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op);
  void InitializeLanes();
  virtual void Clear();
  void CreateThreadLocalCSRs(); // Generates LocalCSRs


  // Common members
  shared_ptr<DataChunk> pairs;
  std::vector<shared_ptr<LocalCSR>> local_csrs;
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

  mutex log_mutex;
  std::vector<std::tuple<std::thread::id, int, double, idx_t>> timing_data; // (Thread ID, Core ID, Time in ms)

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

  idx_t tasks_scheduled;
  bitset<LANE_LIMIT> lane_completed;
};

} // namespace core

} // namespace duckpgq