#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

namespace duckpgq {

namespace core {


class GlobalBFSState : public enable_shared_from_this<GlobalBFSState> {

public:
  GlobalBFSState(unique_ptr<ColumnDataCollection> &pairs_, CSR* csr_, int64_t vsize_,
                 idx_t num_threads_, string mode_, ClientContext &context_);

  void InitializeBFS(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op);
  void ScheduleBFSBatch(Pipeline &pipeline, Event &event);

  void Clear();

  void CreateTasks();
  shared_ptr<pair<idx_t, idx_t>> FetchTask();      // Function to fetch a task
  void ResetTaskIndex();

  pair<idx_t, idx_t> BoundaryCalculation(idx_t worker_id) const;
  CSR *csr;
  unique_ptr<ColumnDataCollection> &pairs; // (src, dst) pairs
  unique_ptr<DataChunk> current_pairs_batch;
  const PhysicalPathFinding *op;
  int64_t iter;
  int64_t v_size; // Number of vertices
  bool change;
  idx_t started_searches; // Number of started searches in current batch
  int64_t *src;
  int64_t *dst;
  UnifiedVectorFormat vdata_src;
  UnifiedVectorFormat vdata_dst;
  int64_t lane_to_num[LANE_LIMIT];
  idx_t active = 0;
  unique_ptr<DataChunk> path_finding_result;
  size_t current_batch_path_list_len; // Length of the current batch path list
  unique_ptr<ColumnDataCollection> results; // results of (src, dst, path-finding)
  ColumnDataScanState result_scan_state;
  ColumnDataScanState input_scan_state;
  ColumnDataAppendState append_state;
  ClientContext &context;
  vector<std::bitset<LANE_LIMIT>> seen;
  vector<std::bitset<LANE_LIMIT>> visit1;
  vector<std::bitset<LANE_LIMIT>> visit2;
  vector<std::array<ve, LANE_LIMIT>> parents_ve;

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

  string mode;
};

} // namespace core

} // namespace duckpgq