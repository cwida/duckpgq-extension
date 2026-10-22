#pragma once
#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/bfs_state.hpp>
#include <duckpgq/core/operator/phased_kernel_event.hpp>

namespace duckdb {

class PhysicalPathFinding; // Forward declaration

//! MS-BFS over one batch of up to LANE_LIMIT pairs, written as a phased kernel: every BFS iteration is an EXPLORE
//! phase (partitions expanded in parallel) followed by a CHECK phase (newly reached lanes folded into `seen` per
//! partition); the serial step after CHECK detects reached destinations and starts the next iteration or wave.
class IterativeLengthState : public BFSState, public PhasedKernel {
public:
	IterativeLengthState(const shared_ptr<PathFindingBatch> &batch_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
	                     idx_t num_threads_, ClientContext &context_, int64_t vsize_);

	shared_ptr<Event> CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op) override;
	void Clear() override;
	// Function to write timing results to a file
	void WriteTimingResults(const std::string &filename);

	//! ---- PhasedKernel
	bool Start() override;
	idx_t WorkerCount() const override;
	void RunPhase(idx_t worker_id) override;
	bool AdvancePhase() override;
	void Finished() override;

	//! Workers the kernel runs with; set before Start when a group shares its workers and working set
	void SetWorkerCount(idx_t workers);
	//! Label of the batch timing row ("bfs_batch" or "bfs_batch_grouped")
	void SetTimingLabel(const string &label);

public:
	vector<uint8_t> worker_changed;

private:
	enum class Phase : uint8_t { EXPLORE, CHECK };
	void BeginIteration();

	Phase phase = Phase::EXPLORE;
	idx_t worker_count = 1;
	string timing_label = "bfs_batch";
};

} // namespace duckdb
