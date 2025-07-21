#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/core/operator/shortest_path/shortest_path_event.hpp"
#include <duckpgq/core/operator/shortest_path/shortest_path_state.hpp>
#include <duckpgq/core/operator/shortest_path/shortest_path_task.hpp>

namespace duckpgq {
namespace core {

ShortestPathEvent::ShortestPathEvent(shared_ptr<ShortestPathState> gbfs_state_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
    : BasePipelineEvent(pipeline_p), gbfs_state(std::move(gbfs_state_p)), op(op_p) {

}

void ShortestPathEvent::Schedule() {}

void ShortestPathEvent::FinishEvent() {
  // std::cout << "Finished BFSEvent" << std::endl;
}

} // namespace core
} // namespace duckpgq