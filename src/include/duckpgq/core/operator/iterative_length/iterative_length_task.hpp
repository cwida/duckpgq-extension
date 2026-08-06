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
	void IterativeLength();
	void ReachDetect() const;
	bool CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen, std::vector<std::bitset<LANE_LIMIT>> &next,
	                 shared_ptr<LocalCSR> &local_csr) const;
	void UnReachableSet() const;

	void Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit, std::vector<std::bitset<LANE_LIMIT>> &next,
	             const LocalCSR &local_csr);

	void RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit, std::vector<std::bitset<LANE_LIMIT>> &next,
	                const LocalCSR &local_csr);

private:
	ClientContext &context;
	shared_ptr<IterativeLengthState> &state;
	idx_t worker_id;
	bool explore_done; // Flag to indicate task completion
};

} // namespace duckdb
