#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/utils/duckpgq_barrier.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <duckpgq/core/utils/duckpgq_path_reconstruction.hpp>

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

  void CreateTasks();
  shared_ptr<pair<idx_t, idx_t>> FetchTask();      // Function to fetch a task
  void ResetTaskIndex();

  pair<idx_t, idx_t> BoundaryCalculation(idx_t worker_id) const;
  shared_ptr<DataChunk> pairs; // (src, dst) pairs
  CSR *csr;
  string mode;
  shared_ptr<DataChunk> pf_results; // results of path-finding
  LogicalType bfs_type;
  // const PhysicalPathFinding *op;
  int64_t iter;
  int64_t v_size; // Number of vertices
  bool change;
  idx_t started_searches; // Number of started searches in current batch
  int64_t *src;
  Vector &src_data;
  Vector &dst_data;
  int64_t *dst;

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
  idx_t scheduled_threads;

  // task_queues[workerId] = {curTaskIdx, queuedTasks}
  // queuedTasks[curTaskIx] = {start, end}
  vector<pair<idx_t, idx_t>> global_task_queue;
  std::mutex queue_mutex; // Mutex for synchronizing access
  std::condition_variable queue_cv; // Condition variable for task availability
  size_t current_task_index = 0; // Index to track the current task
  int64_t split_size = 256;

  unique_ptr<Barrier> barrier;

  // lock for next
  mutable vector<mutex> element_locks;
};

} // namespace core

} // namespace duckpgq