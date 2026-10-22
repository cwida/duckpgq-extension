#include "duckpgq/core/operator/iterative_length/grouped_iterative_length_event.hpp"

namespace duckdb {

GroupedIterativeLengthKernel::GroupedIterativeLengthKernel(shared_ptr<GroupedIterativeLengthBatches> batches_p,
                                                           idx_t workers_p, idx_t v_size, ClientContext &context)
    : batches(std::move(batches_p)), workers(std::max<idx_t>(1, workers_p)),
      working_set(make_shared_ptr<BFSWorkingSet>(context, v_size)) {
}

bool GroupedIterativeLengthKernel::StartNextBatch() {
	while (true) {
		current = batches->Claim();
		if (!current) {
			return false;
		}
		current->AttachWorkingSet(working_set);
		current->SetWorkerCount(workers);
		current->SetTimingLabel("bfs_batch_grouped");
		if (current->Start()) {
			return true;
		}
		current->Finished();
	}
}

bool GroupedIterativeLengthKernel::Start() {
	return StartNextBatch();
}

idx_t GroupedIterativeLengthKernel::WorkerCount() const {
	return workers;
}

void GroupedIterativeLengthKernel::RunPhase(idx_t worker_id) {
	current->RunPhase(worker_id);
}

bool GroupedIterativeLengthKernel::AdvancePhase() {
	if (current->AdvancePhase()) {
		return true;
	}
	current->Finished();
	return StartNextBatch();
}

} // namespace duckdb
