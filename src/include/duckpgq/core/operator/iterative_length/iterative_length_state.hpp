#pragma once
#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/bfs_state.hpp>
#include <duckpgq/core/operator/phased_kernel_event.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

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
	//! Shared with the scheduler: edges swept by the explore phases of this batch (the throughput clock of the
	//! adaptive batch scheduler); null when nobody listens
	atomic<idx_t> *edges_scanned = nullptr;
	//! experimental_path_finding_operator_frontier_bitmap: skip inactive sources through the summary bitmap
	bool use_frontier_bitmap = true;
	//! Test the bitmap per source on dense levels too: set by the scheduler from the regime (memory bound: on,
	//! SF100 1.2-1.6x; cache resident: off, SF10 10% overhead) unless the option fixes it
	bool bitmap_filter = false;
	//! Applies the option: -1 decides from the size of one working set (on when it exceeds a sixteenth of the grouped
	//! batches budget, 32 MB by default: SF10's 14 MB stays off, SF100's 86 MB goes on; measured 27 Oct: the test
	//! also pays for a single SF100 batch, so the total of the working sets is not the right measure)
	void SetBitmapFilter() {
		auto configured = GetPathFindingFrontierBitmapFilter(context);
		if (configured >= 0) {
			bitmap_filter = configured > 0;
			return;
		}
		auto budget = GetPathFindingGroupedBatchesBudget(context);
		auto threshold = budget > 0 ? static_cast<double>(budget) / 16 : 32.0 * 1024 * 1024;
		bitmap_filter = static_cast<double>(BFSWorkingSet::BytesFor(static_cast<idx_t>(v_size))) > threshold;
	}
	//! experimental_path_finding_operator_visit_prefetch: visit lines prefetched this many sources ahead (0 off)
	idx_t visit_prefetch = 8;
	//! Sparse levels (W31 step 3): when few vertices have any lane active (count x divisor < V), the explore phase
	//! expands the active vertices by row lookup in each partition instead of sweeping the partition's source list;
	//! the tail of a wave, kept alive by a few long-distance lanes, runs ten or more such levels on LDBC knows
	idx_t sparse_level_divisor = 16;
	bool sparse_level = false;
	bool row_lookup_available = true;
	vector<uint32_t> frontier_list;
	vector<idx_t> worker_active_counts;

private:
	enum class Phase : uint8_t { EXPLORE, CHECK };
	void BeginIteration();
	void PrepareLevel(idx_t active_total);

	Phase phase = Phase::EXPLORE;
	idx_t worker_count = 1;
	string timing_label = "bfs_batch";
};

} // namespace duckdb
