#pragma once

#include "duckpgq/common.hpp"
#include "local_csr_state.hpp"

namespace duckdb {

//! The per-worker steps of the partitioned CSR build. The build kernel (local_csr_event.hpp) runs them phase by
//! phase; each method is one parallel phase or a serial step the kernel runs through worker 0.
class LocalCSRWorker {
public:
	LocalCSRWorker(shared_ptr<LocalCSRState> state_p, idx_t worker_id_p);

	void PromoteStreamingEndpointBuffers(std::vector<shared_ptr<LocalCSR>> &partition_csrs);
	void BufferForwardEdges(std::vector<shared_ptr<LocalCSR>> &partition_csrs);
	void MergeForwardBuffers(std::vector<shared_ptr<LocalCSR>> &partition_csrs);
	void CreateStatistics(bool reverse, std::vector<int64_t> &statistics_chunks) const;
	void DeterminePartitions(std::vector<int64_t> &statistics_chunks, std::vector<shared_ptr<LocalCSR>> &partition_csrs,
	                         bool initialize_vertex_arrays = true) const;
	void DeterminePullPartitions(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) const;
	void CountOutgoingEdgesPerPartition(bool reverse, std::vector<shared_ptr<LocalCSR>> &partition_csrs);
	void CountIncomingEdgesPerPullPartition(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs);
	idx_t GetPartitionForVertex(idx_t vertex, std::vector<shared_ptr<LocalCSR>> &partition_csrs) const;
	idx_t GetPullPartitionForVertex(idx_t vertex, std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) const;
	void CreateRunningSum(std::vector<shared_ptr<LocalCSR>> &partition_csrs) const;
	void CreatePullRunningSum(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) const;
	void PrepareDistributeEdges(std::vector<shared_ptr<LocalCSR>> &partition_csrs) const;
	void DistributeEdges(bool reverse, std::vector<shared_ptr<LocalCSR>> &partition_csrs);
	void PrepareDistributePullEdges(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs) const;
	void DistributePullEdges(std::vector<shared_ptr<PullCSR>> &pull_partition_csrs);
	void FinalizeSparseRows(std::vector<shared_ptr<LocalCSR>> &partition_csrs) const;

	shared_ptr<LocalCSRState> local_csr_state;
	idx_t worker_id;
};

} // namespace duckdb
