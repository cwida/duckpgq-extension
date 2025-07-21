#include "duckpgq/core/operator/iterative_length/iterative_length_event.hpp"
#include <chrono>
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
  idx_t num_partitions = gbfs_state->local_csrs.size();
  for (idx_t tnum = 0; tnum < std::min(gbfs_state->num_threads, num_partitions); tnum++) {
    bfs_tasks.push_back(make_uniq<IterativeLengthTask>(
        shared_from_this(), context, gbfs_state, tnum, op));
    gbfs_state->tasks_scheduled++;
  }
  gbfs_state->barrier = make_uniq<Barrier>(gbfs_state->tasks_scheduled);
  SetTasks(std::move(bfs_tasks));
}

void IterativeLengthEvent::FinishEvent() {
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);

  auto heavy_partition_fraction = std::to_string(GetHeavyPartitionFraction(gbfs_state->context));
  auto light_partition_multiplier = std::to_string(GetLightPartitionMultiplier(gbfs_state->context));
  std::stringstream ss;
  ss << std::put_time(std::localtime(&time_t_now), "%Y-%m-%d_%H-%M-%S");
  auto timestamp = ss.str();
  string file_name = "timing_results_" + timestamp + + "_threads_" + std::to_string(gbfs_state->num_threads) + "_" + heavy_partition_fraction + "_" + light_partition_multiplier + ".csv";
  gbfs_state->WriteTimingResults(file_name);
}

} // namespace core
} // namespace duckpgq

