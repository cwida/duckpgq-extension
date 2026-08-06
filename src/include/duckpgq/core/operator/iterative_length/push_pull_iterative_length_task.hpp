#pragma once

#include "duckpgq/common.hpp"
#include "push_pull_iterative_length_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckdb {

class PushPullIterativeLengthTask : public ExecutorTask {
public:
	PushPullIterativeLengthTask(shared_ptr<Event> event_p, ClientContext &context,
	                            shared_ptr<PushPullIterativeLengthState> &state, idx_t worker_id,
	                            const PhysicalOperator &op_p);

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	void PushPullIterativeLength();
	void Push(idx_t iteration);
	void Pull(idx_t iteration);
	void ReachDetect() const;
	idx_t CheckChange(std::vector<std::bitset<LANE_LIMIT>> &seen, std::vector<std::bitset<LANE_LIMIT>> &next,
	                  shared_ptr<LocalCSR> &local_csr, idx_t &candidate_vertices) const;
	void UnReachableSet() const;
	idx_t CountFrontierVertices(const std::vector<std::bitset<LANE_LIMIT>> &visit, idx_t start_vertex,
	                            idx_t end_vertex) const;
	void RecordPhaseTiming(const char *phase, const char *mode, idx_t iteration, idx_t partition_count, idx_t vertices,
	                       idx_t edges, idx_t candidates, idx_t changed_vertices, double time_ms) const;
	void TimedBarrier(const char *phase, const char *mode, idx_t iteration);

	idx_t Explore(const std::vector<std::bitset<LANE_LIMIT>> &visit, std::vector<std::bitset<LANE_LIMIT>> &next,
	              const std::atomic<uint32_t> *v, const std::vector<uint16_t> &e, size_t v_size, idx_t start_vertex);

	idx_t RunExplore(const std::vector<std::bitset<LANE_LIMIT>> &visit, std::vector<std::bitset<LANE_LIMIT>> &next,
	                 const std::atomic<uint32_t> *v, const std::vector<uint16_t> &e, size_t v_size, idx_t start_vertex);

private:
	ClientContext &context;
	shared_ptr<PushPullIterativeLengthState> &state;
	idx_t worker_id;
};

} // namespace duckdb
