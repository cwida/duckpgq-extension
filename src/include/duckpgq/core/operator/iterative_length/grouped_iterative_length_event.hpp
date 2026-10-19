#pragma once

#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/executor_task.hpp"
#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"
#include "duckpgq/core/utils/duckpgq_barrier.hpp"

namespace duckdb {

class PhysicalPathFinding;
class GroupedIterativeLengthEvent;

struct GroupedIterativeLengthGroupState {
	GroupedIterativeLengthGroupState(idx_t worker_count, idx_t v_size, ClientContext &context);

	unique_ptr<Barrier> barrier;
	//! Lane bitsets reused by every batch this group runs
	shared_ptr<BFSWorkingSet> working_set;
	shared_ptr<IterativeLengthState> current_state;
};

class GroupedIterativeLengthTask : public ExecutorTask {
public:
	GroupedIterativeLengthTask(shared_ptr<Event> event_p, ClientContext &context,
	                           GroupedIterativeLengthEvent &grouped_event_p, idx_t group_id_p, idx_t worker_id_p,
	                           const PhysicalOperator &op_p);

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	ClientContext &context;
	GroupedIterativeLengthEvent &grouped_event;
	idx_t group_id;
	idx_t worker_id;
};

class GroupedIterativeLengthEvent : public BasePipelineEvent {
public:
	GroupedIterativeLengthEvent(vector<shared_ptr<IterativeLengthState>> states_p, idx_t workers_per_group_p,
	                            idx_t group_count_p, Pipeline &pipeline_p, const PhysicalPathFinding &op_p);

	void Schedule() override;
	void FinishEvent() override;

	shared_ptr<IterativeLengthState> ClaimNextBatch(GroupedIterativeLengthGroupState &group);
	void FinishBatch(IterativeLengthState &state);
	idx_t WorkersPerGroup() const;
	GroupedIterativeLengthGroupState &Group(idx_t group_id);

private:
	vector<shared_ptr<IterativeLengthState>> states;
	vector<unique_ptr<GroupedIterativeLengthGroupState>> groups;
	atomic<idx_t> next_batch;
	idx_t workers_per_group;
	idx_t group_count;
	const PhysicalPathFinding &op;
};

} // namespace duckdb
