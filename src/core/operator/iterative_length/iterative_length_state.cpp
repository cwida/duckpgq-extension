#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

namespace duckdb {

IterativeLengthState::IterativeLengthState(const shared_ptr<PathFindingBatch> &batch_,
                                           std::vector<shared_ptr<LocalCSR>> &local_csrs_, idx_t num_threads_,
                                           ClientContext &context_, int64_t vsize_)
    : BFSState(batch_, local_csrs_, num_threads_, "iterativelength", context_, vsize_) {
	worker_changed.resize(num_threads_, 0);
	worker_count = std::max<idx_t>(1, num_threads_);
	use_frontier_bitmap = GetPathFindingFrontierBitmap(context_);
	SetBitmapFilter();
	auto prefetch = GetPathFindingVisitPrefetch(context_);
	visit_prefetch = prefetch > 0 ? NumericCast<idx_t>(prefetch) : 0;
	sub_range_tasks = GetPathFindingSubRangeTasks(context_);
	auto divisor = GetPathFindingSparseLevelDivisor(context_);
	sparse_level_divisor = divisor > 0 ? NumericCast<idx_t>(divisor) : 0;
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
