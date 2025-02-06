#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/operator/bfs_state.hpp>

namespace duckpgq {

namespace core {
class PhysicalPathFinding; // Forward declaration

class IterativeLengthState : public BFSState {
public:
  IterativeLengthState(shared_ptr<DataChunk> pairs_, CSR* csr_, idx_t num_threads_,
    ClientContext &context_);

  void ScheduleBFSBatch(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op) override;

  void Clear() override;
};

} // namespace core

} // namespace duckpgq