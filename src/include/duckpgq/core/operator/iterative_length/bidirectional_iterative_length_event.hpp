#pragma once

#include <duckdb/parallel/base_pipeline_event.hpp>
#include <duckpgq/core/operator/iterative_length/bidirectional_iterative_length_task.hpp>

namespace duckdb {

class BidirectionalIterativeLengthEvent : public BasePipelineEvent {
public:
	BidirectionalIterativeLengthEvent(shared_ptr<BidirectionalIterativeLengthState> gbfs_state_p, Pipeline &pipeline_p,
	                                  const PhysicalPathFinding &op_p);

	void Schedule() override;
	void FinishEvent() override;

private:
	shared_ptr<BidirectionalIterativeLengthState> gbfs_state;
	const PhysicalPathFinding &op;
};

} // namespace duckdb
