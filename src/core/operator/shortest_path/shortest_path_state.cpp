#include "duckpgq/core/operator/shortest_path/shortest_path_state.hpp"

#include <duckpgq/core/operator/shortest_path/shortest_path_event.hpp>

//  parents_ve = std::vector<std::array<ve, LANE_LIMIT>>(
//  v_size, std::array<ve, LANE_LIMIT>{});

namespace duckpgq {

namespace core {

ShortestPathState::ShortestPathState(const shared_ptr<DataChunk> &pairs_, std::vector<shared_ptr<LocalCSR>> &local_csrs_, std::vector<std::pair<idx_t, idx_t>> &partition_ranges_, idx_t num_threads_, ClientContext &context_)
    : BFSState(pairs_, local_csrs_, partition_ranges_, num_threads_, "shortestpath", context_) {
  parents_ve = std::vector<std::array<ve, LANE_LIMIT>>(
                      v_size, std::array<ve, LANE_LIMIT>{});
}

void ShortestPathState::Clear() {
  iter = 1;
  active = 0;
  change = false;
  // empty visit vectors
  for (auto i = 0; i < v_size; i++) {
    visit1[i] = 0;
    visit2[i] = 0;
    seen[i] = 0; // reset
  }
  for (auto i = 0; i < v_size; i++) {
    for (auto j = 0; j < LANE_LIMIT; j++) {
      parents_ve[i][j] = {-1, -1};
    }
  }

  lane_completed.reset();
}


void ShortestPathState::ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op) {
  event.InsertEvent(
      make_shared_ptr<ShortestPathEvent>(shared_ptr_cast<BFSState, ShortestPathState>(shared_from_this()), pipeline, *op));
}

} // namespace core

} // namespace duckpgq