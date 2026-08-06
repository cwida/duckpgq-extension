#pragma once

#include "duckpgq/common.hpp"
#include "local_csr_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckdb {

class LocalCSRTask : public ExecutorTask {
public:
	LocalCSRTask(shared_ptr<Event> event_p, ClientContext &context, shared_ptr<LocalCSRState> &state, idx_t worker_id,
	             const PhysicalOperator &op_p);

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

	void BuildLocalCSRs(bool reverse);
	void BuildPullCSRs();
	void CreateStatistics(bool reverse, std::vector<int64_t> &statistics_chunks) const;
	void DeterminePartitions(std::vector<int64_t> &statistics_chunks,
	                         std::vector<shared_ptr<LocalCSR>> &partition_csrs) const;
	void DeterminePullPartitions(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) const;
	void CountOutgoingEdgesPerPartition(bool reverse, std::vector<shared_ptr<LocalCSR>> &partition_csrs);
	void CountIncomingEdgesPerPullPartition(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs);
	idx_t GetPartitionForVertex(idx_t vertex, std::vector<shared_ptr<LocalCSR>> &partition_csrs) const;
	idx_t GetPullPartitionForVertex(idx_t vertex, std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) const;
	void CreateRunningSum(std::vector<shared_ptr<LocalCSR>> &partition_csrs) const;
	void CreatePullRunningSum(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) const;
	void DistributeEdges(bool reverse, std::vector<shared_ptr<LocalCSR>> &partition_csrs);
	void DistributePullEdges(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs);

	shared_ptr<LocalCSRState> &local_csr_state;
	idx_t worker_id;
};

} // namespace duckdb
