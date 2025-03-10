#include "duckpgq/core/operator/bfs_state.hpp"

#include <duckpgq/core/operator/shortest_path/shortest_path_event.hpp>
#include <duckpgq/core/operator/iterative_length/iterative_length_event.hpp>

#include <duckpgq/core/utils/compressed_sparse_row.hpp>
#include <duckpgq/core/utils/duckpgq_barrier.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {

BFSState::BFSState(const shared_ptr<DataChunk> &pairs_, std::vector<shared_ptr<LocalCSR>> &local_csrs_, std::vector<std::pair<idx_t, idx_t>> &partition_ranges_, idx_t num_threads_,
           string mode_, ClientContext &context_, int64_t vsize_)
    : pairs(pairs_), local_csrs(local_csrs_), partition_ranges(partition_ranges_),
      context(context_), num_threads(num_threads_), mode(std::move(mode_)), v_size(vsize_),
      src_data(pairs->data[0]), dst_data(pairs->data[1]){
  LogicalType bfs_type = mode == "iterativelength"
                             ? LogicalType::BIGINT
                             : LogicalType::LIST(LogicalType::BIGINT);
  // Only have to initialize the current batch and state once.
  total_pairs_processed = 0; // Initialize the total pairs processed
  current_batch_path_list_len = 0;
  started_searches = 0; // reset
  active = 0;
  iter = 1;
  change = false;
  pf_results = make_shared_ptr<DataChunk>();
  pf_results->Initialize(context, {bfs_type});
  visit1 = vector<std::bitset<LANE_LIMIT>>(v_size);
  visit2 = vector<std::bitset<LANE_LIMIT>>(v_size);
  seen = vector<std::bitset<LANE_LIMIT>>(v_size);

  local_csr_counter = 0;
  partition_counter = 0;

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
      int64_t dst_pos = vdata_dst.sel->get_index(search_num);
      if (!vdata_src.validity.RowIsValid(src_pos) || !vdata_dst.validity.RowIsValid(dst_pos)) {
        result_validity.SetInvalid(search_num);
      } else if (src[src_pos] == dst[dst_pos]) {
        pf_results->data[0].SetValue(search_num, 0);
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