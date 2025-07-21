#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/utils/duckpgq_path_reconstruction.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {
class PhysicalPathFinding; // Forward declaration

class ShortestPathState : public BFSState {
public:
  ShortestPathState(const shared_ptr<DataChunk> &pairs_, std::vector<shared_ptr<LocalCSR>> &local_csrs_, idx_t num_threads_,
                      ClientContext &context_, int64_t vsize_);

  void ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op) override;

  void Clear() override;
    
  // Additional members for tracking paths
  vector<array<ve, LANE_LIMIT>> parents_ve;

};

} // namespace core

} // namespace duckpgq