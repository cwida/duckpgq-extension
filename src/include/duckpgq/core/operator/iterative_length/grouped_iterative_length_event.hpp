#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"
#include "duckpgq/core/operator/phased_kernel_event.hpp"

namespace duckdb {

//! The batches of one query, claimed by worker groups in order
struct GroupedIterativeLengthBatches {
	explicit GroupedIterativeLengthBatches(vector<shared_ptr<IterativeLengthState>> states_p)
	    : states(std::move(states_p)) {
	}
	//! The next unclaimed batch, or nullptr
	shared_ptr<IterativeLengthState> Claim() {
		auto batch_idx = next_batch.fetch_add(1);
		return batch_idx < states.size() ? states[batch_idx] : nullptr;
	}
	vector<shared_ptr<IterativeLengthState>> states;
	atomic<idx_t> next_batch {0};
};

//! One worker group: runs the batches it claims one after the other with a fixed number of workers, sharing one
//! working set (the lane bitsets) across them, so the footprint scales with the number of groups rather than with
//! the number of batches. A group is a phased kernel that delegates its phases to the batch it currently runs.
class GroupedIterativeLengthKernel : public PhasedKernel {
public:
	GroupedIterativeLengthKernel(shared_ptr<GroupedIterativeLengthBatches> batches_p, idx_t workers_p, idx_t v_size,
	                             ClientContext &context);

	bool Start() override;
	idx_t WorkerCount() const override;
	void RunPhase(idx_t worker_id) override;
	bool AdvancePhase() override;

private:
	//! Claims batches until one has work to do; false when none is left
	bool StartNextBatch();

	shared_ptr<GroupedIterativeLengthBatches> batches;
	idx_t workers;
	shared_ptr<BFSWorkingSet> working_set;
	shared_ptr<IterativeLengthState> current;
};

} // namespace duckdb
