#pragma once

#include "duckpgq/common.hpp"
#include "iterative_length_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckdb {

class IterativeLengthTask : public ExecutorTask {
public:
	IterativeLengthTask(shared_ptr<Event> event_p, ClientContext &context, shared_ptr<IterativeLengthState> &state,
	                    idx_t worker_id, const PhysicalOperator &op_p);

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	ClientContext &context;
	shared_ptr<IterativeLengthState> &state;
	idx_t worker_id;
};

} // namespace duckdb
