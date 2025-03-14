#include "duckpgq/core/operator/iterative_length/iterative_length_event.hpp"
#include <chrono>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>
#include <iomanip>

namespace duckpgq {
namespace core {

IterativeLengthEvent::IterativeLengthEvent(shared_ptr<IterativeLengthState> gbfs_state_p,
                          Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
    : BasePipelineEvent(pipeline_p), gbfs_state(std::move(gbfs_state_p)), op(op_p) {

}


void IterativeLengthEvent::Schedule() {
  auto &context = pipeline->GetClientContext();
  vector<shared_ptr<Task>> bfs_tasks;
  // idx_t local_csr_size = gbfs_state->local_csrs.size();
  for (idx_t tnum = 0; tnum < gbfs_state->num_threads; tnum++) { // todo(dtenwolde) Don't over-schedule tasks
    bfs_tasks.push_back(make_uniq<IterativeLengthTask>(
        shared_from_this(), context, gbfs_state, tnum, op));
  }
  SetTasks(std::move(bfs_tasks));
}

void IterativeLengthEvent::FinishEvent() {
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&time_t_now), "%Y-%m-%d_%H-%M-%S");
  auto timestamp = ss.str();
  string file_name = "timing_results_" + timestamp + + "_threads_" + std::to_string(gbfs_state->num_threads) + ".csv";
  gbfs_state->WriteTimingResults(file_name);
}

} // namespace core
} // namespace duckpgq

