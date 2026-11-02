#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckdb {

IterativeLengthState::IterativeLengthState(const shared_ptr<PathFindingBatch> &batch_,
                                           std::vector<shared_ptr<LocalCSR>> &local_csrs_, idx_t num_threads_,
                                           ClientContext &context_, int64_t vsize_,
                                           const PathFindingSettings &settings)
    : BFSState(batch_, local_csrs_, num_threads_, PathFindingOperatorMode::ITERATIVE_LENGTH, context_, vsize_,
               settings) {
	worker_changed.resize(num_threads_, 0);
	worker_count = std::max<idx_t>(1, num_threads_);
	use_frontier_bitmap = settings.frontier_bitmap;
	SetBitmapFilter(settings);
	visit_prefetch = settings.visit_prefetch;
	sub_range_tasks = settings.sub_range_tasks;
	sparse_level_divisor = settings.sparse_level_divisor;
	// Row lookup needs the sparse-row form of every partition; partitions assembled from streaming segments are swept
	for (auto &local_csr : local_csrs) {
		if (local_csr && (!local_csr->segments.empty() || !local_csr->HasSparseRows())) {
			row_lookup_available = false;
		}
	}
}

void IterativeLengthState::Clear() {
	// The working set keeps the last wave's bits: the PREPARE phase of the next wave clears them on the workers
	iter = 1;
	active = 0;
	change = false;
	std::fill(worker_changed.begin(), worker_changed.end(), 0);
}

shared_ptr<Event> IterativeLengthState::CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op) {
	auto self = shared_ptr_cast<BFSState, IterativeLengthState>(shared_from_this());
	return make_shared_ptr<PhasedKernelEvent>(shared_ptr<PhasedKernel>(self), pipeline, *op);
}

} // namespace duckdb
