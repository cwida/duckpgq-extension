#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/operator/bfs_state.hpp>

namespace duckpgq {

namespace core {
class PhysicalPathFinding; // Forward declaration

class IterativeLengthState : public BFSState {
public:
  IterativeLengthState(const shared_ptr<DataChunk> &pairs_, std::vector<shared_ptr<LocalCSR>> &local_csrs_, idx_t num_threads_,
    ClientContext &context_, int64_t vsize_);

  void ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op) override;

  void Clear() override;


  // Function to write timing results to a file
  void WriteTimingResults(const std::string &filename);

};

} // namespace core

} // namespace duckpgq