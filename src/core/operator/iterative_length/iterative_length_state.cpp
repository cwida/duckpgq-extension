#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"

#include <duckpgq/core/operator/shortest_path/shortest_path_event.hpp>
#include <duckpgq/core/operator/iterative_length/iterative_length_event.hpp>

namespace duckpgq {

namespace core {

IterativeLengthState::IterativeLengthState(const shared_ptr<DataChunk> &pairs_, std::vector<shared_ptr<LocalCSR>> &local_csrs_, std::vector<std::pair<idx_t, idx_t>> &partition_ranges_, idx_t num_threads_, ClientContext &context_)
    : BFSState(pairs_, local_csrs_, partition_ranges_, num_threads_, "iterativelength", context_) {
  // Additional IterativeLengthState-specific initialization here
}

void IterativeLengthState::Clear() {
  iter = 1;
  active = 0;
  change = false;
  // empty visit vectors
  for (auto i = 0; i < v_size; i++) {
    visit1[i] = 0;
    visit2[i] = 0;
    seen[i] = 0; // reset
  }
  lane_completed.reset();
}

void IterativeLengthState::ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op) {
  event.InsertEvent(
    make_shared_ptr<IterativeLengthEvent>(
      shared_ptr_cast<BFSState, IterativeLengthState>(shared_from_this()),
      pipeline, *op));
}

} // namespace core

} // namespace duckpgq