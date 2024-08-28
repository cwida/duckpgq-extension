#include "duckpgq/core/operator/task/csr_edge_creation_task.hpp"

namespace duckpgq {
namespace core {

PhysicalCSREdgeCreationTask::PhysicalCSREdgeCreationTask(shared_ptr<Event> event_p, ClientContext &context,
                              PathFindingGlobalState &state)
      : ExecutorTask(context, std::move(event_p)), context(context),
        state(state) {}

TaskExecutionResult PhysicalCSREdgeCreationTask::ExecuteTask(TaskExecutionMode mode) {
  auto &global_inputs = state.global_inputs;
  auto &global_csr = state.global_csr;
  auto &scan_state = state.scan_state;

  DataChunk input;
  global_inputs->InitializeScanChunk(input);
  auto result = Vector(LogicalTypeId::BIGINT);
  while (true) {
    {
      lock_guard<mutex> lock(global_csr->csr_lock);
      if (!global_inputs->Scan(scan_state, input)) {
        break;
      }
    }
    if (!global_csr->initialized_e) {
      const auto e_size = input.data[7].GetValue(0).GetValue<int64_t>();
      global_csr->InitializeEdge(e_size);
    }
    TernaryExecutor::Execute<int64_t, int64_t, int64_t, int32_t>(
        input.data[6], input.data[4], input.data[2], result, input.size(),
        [&](int64_t src, int64_t dst, int64_t edge_id) {
          const auto pos = ++global_csr->v[src + 1];
          global_csr->e[static_cast<int64_t>(pos) - 1] = dst;
          global_csr->edge_ids[static_cast<int64_t>(pos) - 1] = edge_id;
          return 1;
        });
  }
  event->FinishTask();
  return TaskExecutionResult::TASK_FINISHED;
}


} // namespace core
} // namespace duckpgq
