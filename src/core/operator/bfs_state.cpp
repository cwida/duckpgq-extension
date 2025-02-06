#include "duckpgq/core/operator/bfs_state.hpp"

#include <duckpgq/core/operator/shortest_path/shortest_path_event.hpp>
#include <duckpgq/core/operator/iterative_length/iterative_length_event.hpp>

#include <duckpgq/core/utils/compressed_sparse_row.hpp>
#include <duckpgq/core/utils/duckpgq_barrier.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {

BFSState::BFSState(shared_ptr<DataChunk> pairs_, CSR* csr_, idx_t num_threads_,
           string mode_, ClientContext &context_)
    : pairs(std::move(pairs_)), csr(csr_), v_size(csr_->vsize - 2),
      context(context_), num_threads(num_threads_), mode(std::move(mode_)),
      src_data(pairs->data[0]), dst_data(pairs->data[1]){
  LogicalType bfs_type = mode == "iterativelength"
                             ? LogicalType::BIGINT
                             : LogicalType::LIST(LogicalType::BIGINT);
  D_ASSERT(csr_ != nullptr);
  // Only have to initialize the current batch and state once.
  total_pairs_processed = 0; // Initialize the total pairs processed
  current_batch_path_list_len = 0;
  started_searches = 0; // reset
  active = 0;
  iter = 1;
  change_atomic = false;
  pf_results = make_shared_ptr<DataChunk>();
  pf_results->Initialize(context, {bfs_type});

  visit1 = vector<std::bitset<LANE_LIMIT>>(v_size);
  visit2 = vector<std::bitset<LANE_LIMIT>>(v_size);
  seen = vector<std::bitset<LANE_LIMIT>>(v_size);

  // Initialize source and destination vectors
  src_data.ToUnifiedFormat(pairs->size(), vdata_src);
  dst_data.ToUnifiedFormat(pairs->size(), vdata_dst);
  src = FlatVector::GetData<int64_t>(src_data);
  dst = FlatVector::GetData<int64_t>(dst_data);

  // Initialize the thread assignment vector
  thread_assignment = std::vector<int64_t>(v_size, -1);

  // CreateTasks();
  barrier = make_uniq<Barrier>(num_threads);
}

BFSState::~BFSState() = default;  // Define the virtual destructor

void BFSState::Clear() {
  // Default empty implementation; override in derived classes
}

void BFSState::ScheduleBFSBatch(Pipeline &, Event &, const PhysicalPathFinding *) {
  throw NotImplementedException("ScheduleBFSBatch must be implemented in a derived class.");
}

void BFSState::InitializeLanes() {
  auto &result_validity = FlatVector::Validity(pf_results->data[0]);
  std::bitset<LANE_LIMIT> seen_mask;
  seen_mask.set();

  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    lane_to_num[lane] = -1;
    while (started_searches < pairs->size()) {
      auto search_num = started_searches++;
      int64_t src_pos = vdata_src.sel->get_index(search_num);
      if (!vdata_src.validity.RowIsValid(src_pos)) {
        result_validity.SetInvalid(search_num);
      } else {
        visit1[src[src_pos]][lane] = true;
        // bfs_state->seen[bfs_state->src[src_pos]][lane] = true;
        lane_to_num[lane] = search_num; // active lane
        active++;
        seen_mask[lane] = false;
        break;
      }
    }
  }
  for (int64_t i = 0; i < v_size; i++) {
    seen[i] = seen_mask;
  }
}



} // namespace core

} // namespace duckpgq