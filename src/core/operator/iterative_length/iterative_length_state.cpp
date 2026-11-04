#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

#include <cstring>

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

//===--------------------------------------------------------------------===//
// ReachResultBudget
//===--------------------------------------------------------------------===//
void ReachResultBudget::Charge(idx_t entries) {
	auto total = charged_entries.fetch_add(entries) + entries;
	if (cap_bytes != 0 && total * BYTES_PER_ENTRY > cap_bytes) {
		throw OutOfMemoryException(
		    "the reach result exceeds %s: the sources reach more than %llu (vertex, distance) entries, accounted at %llu "
		    "bytes each (DuckDB's memory_limit minus the path-finding working-set cap). Run fewer sources per query, "
		    "list the destinations, or raise memory_limit",
		    StringUtil::BytesToHumanReadableString(cap_bytes), total, BYTES_PER_ENTRY);
	}
}

//===--------------------------------------------------------------------===//
// LaneReachRecorder
//===--------------------------------------------------------------------===//
void LaneReachRecorder::Record(idx_t worker, idx_t vertex, int32_t level, const std::bitset<LANE_LIMIT> &lanes) {
	static_assert(sizeof(std::bitset<LANE_LIMIT>) == LANE_LIMIT / 8, "lane bitsets are read as 64-bit words");
	static constexpr idx_t WORDS = LANE_LIMIT / 64;
	touched[worker] |= lanes;
	auto &lane_buffers = buffers[worker];
	uint64_t words[WORDS];
	memcpy(words, &lanes, sizeof(words));
	ReachedEntry entry {NumericCast<uint32_t>(vertex), level};
	idx_t recorded = 0;
	for (idx_t word_idx = 0; word_idx < WORDS; word_idx++) {
		auto word = words[word_idx];
		while (word) {
			auto lane = (word_idx << 6) + static_cast<idx_t>(__builtin_ctzll(word));
			lane_buffers[lane].push_back(entry);
			recorded++;
			word &= word - 1;
		}
	}
	auto &pending_entries = pending[worker];
	pending_entries += recorded;
	if (budget && pending_entries >= CHARGE_ENTRIES) {
		budget->Charge(pending_entries);
		pending_entries = 0;
	}
}

void LaneReachRecorder::ChargePending() {
	for (auto &pending_entries : pending) {
		if (budget && pending_entries > 0) {
			budget->Charge(pending_entries);
		}
		pending_entries = 0;
	}
}

std::bitset<LANE_LIMIT> LaneReachRecorder::TakeTouched() {
	std::bitset<LANE_LIMIT> result;
	for (auto &lanes : touched) {
		result |= lanes;
		lanes.reset();
	}
	return result;
}

idx_t LaneReachRecorder::CountOf(idx_t lane) const {
	idx_t count = 0;
	for (auto &lane_buffers : buffers) {
		count += lane_buffers[lane].size();
	}
	return count;
}

void LaneReachRecorder::Drain(idx_t lane, vector<ReachedEntry> &out) {
	for (auto &lane_buffers : buffers) {
		auto &entries = lane_buffers[lane];
		out.insert(out.end(), entries.begin(), entries.end());
		vector<ReachedEntry>().swap(entries);
	}
}

shared_ptr<Event> IterativeLengthState::CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op) {
	auto self = shared_ptr_cast<BFSState, IterativeLengthState>(shared_from_this());
	return make_shared_ptr<PhasedKernelEvent>(shared_ptr<PhasedKernel>(self), pipeline, *op);
}

} // namespace duckdb
