#include "duckpgq/core/operator/local_csr/local_csr_task.hpp"
#include <duckdb/parallel/event.hpp>
#include <duckpgq/core/operator/local_csr/local_csr_state.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

namespace duckpgq {
namespace core {

LocalCSRTask::LocalCSRTask(shared_ptr<Event> event_p, ClientContext &context,
                           shared_ptr<LocalCSRState> &state, idx_t worker_id_p,
                           const PhysicalOperator &op_p)
    : ExecutorTask(context, std::move(event_p), op_p), local_csr_state(state),
      worker_id(worker_id_p) {}

TaskExecutionResult LocalCSRTask::ExecuteTask(TaskExecutionMode mode) {
  auto &barrier = local_csr_state->barrier;
  CreateStatistics(); // Phase 1
  barrier->Wait(worker_id);
  if (worker_id == 0) {
    DeterminePartitions(); // Phase 2
  }
  barrier->Wait(worker_id);
  CountOutgoingEdgesPerPartition(); // Phase 3
  barrier->Wait(worker_id);
  CreateRunningSum(); // Phase 4
  barrier->Wait(worker_id);
  DistributeEdges(); // Phase 5
  barrier->Wait(worker_id);
  event->FinishTask();
  return TaskExecutionResult::TASK_FINISHED;
}

void LocalCSRTask::DistributeEdges() {
  auto &v = local_csr_state->global_csr->v;
  auto &e = local_csr_state->global_csr->e;
  idx_t total_vertices = local_csr_state->global_csr->vsize- 1;

  // One-time setup: resize edge buffers and initialize write offsets
  if (worker_id == 0) {
    for (auto &csr_ptr : local_csr_state->partition_csrs) {
      auto &csr = *csr_ptr;
      auto edge_count = csr.v[csr.v_array_size - 1].load(std::memory_order_relaxed);
      csr.e.resize(edge_count);
      csr.initialized_e = true;
    }
  }

  // Wait for all threads to finish setup
  local_csr_state->barrier->Wait(worker_id);

  // Determine per-thread vertex range
  idx_t vertices_per_worker = (total_vertices + local_csr_state->tasks_scheduled - 1) / local_csr_state->tasks_scheduled;
  idx_t src_start = worker_id * vertices_per_worker;
  idx_t src_end = std::min(src_start + vertices_per_worker, total_vertices);

  for (idx_t src = src_start; src < src_end; src++) {
    for (idx_t i = v[src]; i < v[src + 1]; i++) {
      idx_t dst = e[i];
      idx_t p = GetPartitionForVertex(dst);
      auto &csr = *local_csr_state->partition_csrs[p];
      auto &offset = csr.v[src + 1];
      idx_t pos = offset.fetch_add(1);
      csr.e[pos] = dst - csr.start_vertex;
    }
  }
}


void LocalCSRTask::CreateRunningSum() const {
  while (true) {
    idx_t i = local_csr_state->partition_index.fetch_add(1);
    if (i >= local_csr_state->partition_csrs.size()) {
      break;
    }

    auto &v = local_csr_state->partition_csrs[i]->v;
    auto v_array_size = local_csr_state->partition_csrs[i]->v_array_size;
    int64_t sum = 0;
    for (idx_t i = 0; i < v_array_size; i++) {
      auto current = v[i].load(std::memory_order_relaxed);
      v[i].store(sum, std::memory_order_relaxed);
      sum += current;
    }
  }
}

idx_t LocalCSRTask::GetPartitionForVertex(idx_t vertex) const {
  for (idx_t i = 0; i < local_csr_state->partition_csrs.size(); i++) {
    auto &csr = *local_csr_state->partition_csrs[i];
    if (vertex >= csr.start_vertex && vertex < csr.end_vertex) {
      return i;
    }
  }
  throw OutOfRangeException("Vertex %llu not found in any partition", vertex);
}

void LocalCSRTask::CountOutgoingEdgesPerPartition() {
  auto &v = local_csr_state->global_csr->v;
  auto &e = local_csr_state->global_csr->e;
  idx_t total_edges = e.size();

  // Determine work range for this worker
  idx_t edges_per_worker = (total_edges + local_csr_state->num_threads - 1) / local_csr_state->num_threads;
  idx_t start_edge = worker_id * edges_per_worker;
  idx_t end_edge = std::min(start_edge + edges_per_worker, total_edges);

  for (idx_t src = 0; src + 1 < local_csr_state->global_csr->vsize; src++) {
    idx_t start = v[src];
    idx_t end = v[src + 1];
    if (start >= end_edge || end <= start_edge) {
      continue; // skip vertices outside this worker's edge range
    }

    idx_t local_start = std::max(start, start_edge);
    idx_t local_end = std::min(end, end_edge);
    for (idx_t i = local_start; i < local_end; i++) {
      idx_t dst = e[i];
      idx_t p = GetPartitionForVertex(dst); // Map global vertex ID to partition index
      auto &csr = *local_csr_state->partition_csrs[p];
      // Map global src to local src in partition
      csr.v[src + 1]++;
    }
  }
}


void LocalCSRTask::DeterminePartitions() const {
  const idx_t max_vertex = local_csr_state->global_csr->vsize;

  // Get edge histogram across 256 chunks
  const auto &edge_histogram = local_csr_state->statistics_chunks; // e.g., vector<idx_t> of size 256
  D_ASSERT(edge_histogram.size() == BUCKET_COUNT);

  // Total edge count
  idx_t total_edges = 0;
  for (auto count : edge_histogram) {
    total_edges += count;
  }

  // Split into heavy and light partitions
  idx_t heavy_partition_count = local_csr_state->num_threads;
  idx_t light_partition_count = local_csr_state->num_threads * GetLightPartitionMultiplier(local_csr_state->context);

  idx_t heavy_edge_budget = static_cast<idx_t>(total_edges * GetHeavyPartitionFraction(local_csr_state->context));
  idx_t light_edge_budget = total_edges - heavy_edge_budget;

  idx_t heavy_target_per_partition = heavy_edge_budget / heavy_partition_count;
  idx_t light_target_per_partition = light_edge_budget / light_partition_count;

  idx_t current_chunk = 0;
  idx_t chunk_size = (max_vertex + BUCKET_COUNT - 1) / BUCKET_COUNT;

  local_csr_state->partition_csrs.clear();

  auto create_partition = [&](idx_t start_chunk, idx_t end_chunk) {
    idx_t start_vertex = start_chunk * chunk_size;
    idx_t end_vertex = std::min(end_chunk * chunk_size, max_vertex);

    // If the vertex range exceeds UINT16_MAX, split into multiple subpartitions
    while ((end_vertex - start_vertex) > UINT16_MAX) {
      idx_t mid_vertex = start_vertex + UINT16_MAX;
      auto csr = make_shared_ptr<LocalCSR>(start_vertex, mid_vertex, max_vertex);
      local_csr_state->partition_csrs.push_back(csr);
      start_vertex = mid_vertex;
    }

    // Final partition covering the remainder
    if (start_vertex < end_vertex) {
      auto csr = make_shared_ptr<LocalCSR>(start_vertex, end_vertex, max_vertex);
      local_csr_state->partition_csrs.push_back(csr);
    }
  };

  // Create heavy partitions
  for (idx_t i = 0; i < heavy_partition_count && current_chunk < BUCKET_COUNT; i++) {
    idx_t edge_sum = 0;
    idx_t start_chunk = current_chunk;

    while (current_chunk < BUCKET_COUNT && edge_sum < heavy_target_per_partition) {
      edge_sum += edge_histogram[current_chunk];
      current_chunk++;
    }

    create_partition(start_chunk, current_chunk);
  }

  // Create light partitions
  for (idx_t i = 0; i < light_partition_count && current_chunk < BUCKET_COUNT; i++) {
    idx_t edge_sum = 0;
    idx_t start_chunk = current_chunk;

    while (current_chunk < BUCKET_COUNT && edge_sum < light_target_per_partition) {
      edge_sum += edge_histogram[current_chunk];
      current_chunk++;
    }

    create_partition(start_chunk, current_chunk);
  }

  // If there are leftover chunks (in case of rounding), wrap them up
  if (current_chunk < BUCKET_COUNT) {
    create_partition(current_chunk, BUCKET_COUNT);
  }
}

void LocalCSRTask::CreateStatistics() const {
  // References to CSR data
  auto &e = local_csr_state->global_csr->e;
  idx_t total_edges = e.size();
  idx_t max_col = local_csr_state->global_csr->vsize; // max column index is #vertices

  // Determine work range for this worker
  idx_t edges_per_worker = (total_edges + local_csr_state->tasks_scheduled - 1) / local_csr_state->tasks_scheduled;
  idx_t start_edge = worker_id * edges_per_worker;
  idx_t end_edge = std::min(start_edge + edges_per_worker, total_edges);

  // Temporary local histogram to reduce contention
  std::vector<int64_t> local_chunks(BUCKET_COUNT, 0);

  // Compute shift so that max_col fits in BUCKET_COUNT buckets
  idx_t bucket_bits = __builtin_ctz(BUCKET_COUNT); // log2(BUCKET_COUNT)
  idx_t bucket_shift = 0;
  while ((1ULL << (bucket_shift + bucket_bits)) < max_col) {
    bucket_shift++;
  }

  // Bucket edges by destination vertex using bitshift and mask
  for (idx_t i = start_edge; i < end_edge; i++) {
    idx_t bucket = (e[i] >> bucket_shift) & BUCKET_MASK;
    local_chunks[bucket]++;
  }

  // Merge local result into global histogram (atomic add)
  for (idx_t i = 0; i < BUCKET_COUNT; i++) {
    __atomic_fetch_add(&local_csr_state->statistics_chunks[i], local_chunks[i], __ATOMIC_RELAXED);
  }
}

} // namespace core
} // namespace duckpgq
