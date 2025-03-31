#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"

#include <duckpgq/core/operator/iterative_length/iterative_length_event.hpp>
#include <duckpgq/core/operator/shortest_path/shortest_path_event.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>
#include <fstream>

namespace duckpgq {

namespace core {

IterativeLengthState::IterativeLengthState(const shared_ptr<DataChunk> &pairs_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
  idx_t num_threads_, ClientContext &context_, int64_t vsize_)
    : BFSState(pairs_, local_csrs_, num_threads_, "iterativelength", context_, vsize_) {
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


// Function to write timing results to a file
void IterativeLengthState::WriteTimingResults(const std::string &filename) {
  std::ofstream file(filename);
  if (file.is_open()) {
    file << "ThreadID,CoreID,Time(ms),ThreadCount,vsize,esize,numPartitions,Iter\n";
    for (const auto &entry : timing_data) {
      file << std::get<0>(entry) << "," << std::get<1>(entry) << "," << std::get<2>(entry) << "," << std::get<3>(entry) << "," << std::get<4>(entry) << "," << std::get<5>(entry) << "," << std::get<6>(entry) << "," << std::get<7>(entry) << "\n";
    }
    file.close();
  }
}

} // namespace core

} // namespace duckpgq