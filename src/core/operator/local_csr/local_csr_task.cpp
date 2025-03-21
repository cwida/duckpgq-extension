#include "duckpgq/core/operator/local_csr/local_csr_task.hpp"
#include <duckdb/parallel/event.hpp>
#include <duckpgq/core/operator/local_csr/local_csr_state.hpp>
#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

namespace duckpgq {
namespace core {

LocalCSRTask::LocalCSRTask(shared_ptr<Event> event_p,
                           ClientContext &context,
                           shared_ptr<LocalCSRState> &state,
                           const PhysicalOperator &op_p,
                           shared_ptr<LocalCSR> &input_csr_p,
                           idx_t start_vertex_p, idx_t end_vertex_p)
: ExecutorTask(context, std::move(event_p), op_p), local_csr_state(state), input_csr(input_csr_p) {
    output_csr = make_shared_ptr<LocalCSR>();
}

TaskExecutionResult LocalCSRTask::ExecuteTask(TaskExecutionMode mode) {
    PartitionGraph();
    return TaskExecutionResult::TASK_FINISHED;
}

void LocalCSRTask::FillLocalCSR() {
    idx_t v_offset = 0;
    output_csr->v.reserve(input_csr->v.size());

    for (idx_t j = 0; j < input_csr->v.size() - 1; j++) {
        output_csr->v.push_back(v_offset);  // Store vertex offset
        for (idx_t e_offset = input_csr->v[j]; e_offset < input_csr->v[j + 1]; e_offset++) {
            auto dst = input_csr->e[e_offset];
            if (dst >= start_vertex && dst < end_vertex) {
                v_offset++;
                output_csr->e.push_back(dst);
            }
        }
    }
    output_csr->v.push_back(v_offset);  // Final offset for last vertex
}


// Recursive function to partition graph dynamically
void LocalCSRTask::PartitionGraph() {
    // Calculate total edge count in this range
    idx_t edge_count = 0;
    for (idx_t v = start_vertex; v < end_vertex; v++) {
        edge_count += (input_csr->v[v + 1] - input_csr->v[v]);
    }

    if (edge_count <= GetPartitionSize(local_csr_state->context)) {
        // Acceptable partition size → create local CSR
        FillLocalCSR();
        if (!output_csr->e.empty()) {
            local_csr_state->local_csrs.push_back(output_csr);
            local_csr_state->partition_ranges.emplace_back(start_vertex, end_vertex);
        }
    } else {
        // Too large → Try to split into two smaller partitions
        idx_t mid_vertex = (start_vertex + end_vertex) / 2;

        // **Prevent infinite splitting**
        if (mid_vertex == start_vertex || mid_vertex == end_vertex) {
            // We cannot split further → **Keep this large partition**
            FillLocalCSR();
            if (!output_csr->e.empty()) {
              local_csr_state->local_csrs.push_back(output_csr);
              local_csr_state->partition_ranges.emplace_back(start_vertex, end_vertex);
            }
            return;
        }

        local_csr_state->local_csrs_to_partition.push_back(output_csr);
    }
}


} // namespace core
} // namespace duckpgq
