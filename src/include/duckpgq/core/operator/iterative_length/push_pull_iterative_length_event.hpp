#pragma once

#include "duckpgq/common.hpp"
#include "push_pull_iterative_length_state.hpp"

#include <duckdb/parallel/base_pipeline_event.hpp>

namespace duckdb {

class PushPullIterativeLengthEvent : public BasePipelineEvent {
public:
	PushPullIterativeLengthEvent(shared_ptr<PushPullIterativeLengthState> gbfs_state_p, Pipeline &pipeline_p,
	                             const PhysicalPathFinding &op_p);

	void Schedule() override;
	void FinishEvent() override;

private:
	shared_ptr<PushPullIterativeLengthState> gbfs_state;
	const PhysicalPathFinding &op;
};

} // namespace duckdb
