#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

#include <cstdlib>
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
	reach_dense_divisor = settings.reach_dense_divisor;
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
// ZeroedBytes, ReachResult
//===--------------------------------------------------------------------===//
ZeroedBytes::ZeroedBytes(idx_t size_p) : size(size_p) {
	data = static_cast<uint8_t *>(calloc(std::max<idx_t>(1, size_p), 1));
	if (!data) {
		throw OutOfMemoryException("could not allocate %llu bytes for a reach result", size_p);
	}
}

ZeroedBytes::~ZeroedBytes() {
	free(data);
}

ZeroedBytes &ZeroedBytes::operator=(ZeroedBytes &&other) noexcept {
	if (this != &other) {
		free(data);
		data = other.data;
		size = other.size;
		other.data = nullptr;
		other.size = 0;
	}
	return *this;
}

void ReachResult::ToEntries() {
	if (!IsDense()) {
		return;
	}
	entries.reserve(entries.size() + count);
	for (idx_t v = 0; v < distances.size; v++) {
		auto byte = distances.data[v];
		if (byte) {
			entries.push_back({NumericCast<uint32_t>(v), static_cast<int32_t>(byte) - 1});
		}
	}
	distances = ZeroedBytes();
}

void ReachResult::Release() {
	vector<ReachedEntry>().swap(entries);
	distances = ZeroedBytes();
	count = 0;
}

//===--------------------------------------------------------------------===//
// ReachResultBudget
//===--------------------------------------------------------------------===//
void ReachResultBudget::Charge(idx_t bytes) {
	auto total = charged_bytes.fetch_add(bytes) + bytes;
	if (cap_bytes != 0 && total > cap_bytes) {
		throw OutOfMemoryException(
		    "the reach result exceeds %s: the sources' reached sets hold more than %s (DuckDB's memory_limit minus "
		    "the path-finding working-set cap). Run fewer sources per query, list the destinations, or raise "
		    "memory_limit",
		    StringUtil::BytesToHumanReadableString(cap_bytes), StringUtil::BytesToHumanReadableString(total));
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
	auto byte = static_cast<uint8_t>(level + 1); // SwitchForms keeps a lane dense only while level + 1 fits
	idx_t recorded = 0;
	for (idx_t word_idx = 0; word_idx < WORDS; word_idx++) {
		auto word = words[word_idx];
		while (word) {
			auto lane = (word_idx << 6) + static_cast<idx_t>(__builtin_ctzll(word));
			auto dense_lane = dense[lane].data;
			if (dense_lane) {
				dense_lane[vertex] = byte;
			} else {
				lane_buffers[lane].push_back(entry);
				recorded++;
			}
			word &= word - 1;
		}
	}
	auto &pending_entries = pending[worker];
	pending_entries += recorded;
	if (budget && pending_entries >= CHARGE_ENTRIES) {
		budget->Charge(pending_entries * ReachResultBudget::ENTRY_GROWTH_BYTES);
		pending_entries = 0;
	}
}

void LaneReachRecorder::ChargePending() {
	for (auto &pending_entries : pending) {
		if (budget && pending_entries > 0) {
			budget->Charge(pending_entries * ReachResultBudget::ENTRY_GROWTH_BYTES);
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

void LaneReachRecorder::SwitchForms(idx_t next_level) {
	bool byte_fits = next_level <= ReachResult::MAX_DENSE_DISTANCE;
	if (!byte_fits) {
		// The next round would store a distance no byte holds: dense lanes go back to entries (a lane this deep on
		// a graph it covers densely is a long path; rare, and the entries are what the budget then charges)
		for (idx_t lane = 0; lane < LANE_LIMIT; lane++) {
			if (!dense[lane].data) {
				continue;
			}
			ReachResult result;
			result.distances = std::move(dense[lane]);
			result.count = 0;
			for (idx_t v = 0; v < result.distances.size; v++) {
				result.count += result.distances.data[v] != 0;
			}
			if (budget) {
				budget->Charge(result.count * ReachResultBudget::ENTRY_GROWTH_BYTES);
			}
			result.ToEntries();
			if (budget) {
				budget->Release(vertex_count);
			}
			buffers[0][lane] = std::move(result.entries);
		}
		return;
	}
	if (dense_divisor == 0) {
		return;
	}
	auto threshold = vertex_count / dense_divisor;
	for (idx_t lane = 0; lane < LANE_LIMIT; lane++) {
		if (dense[lane].data || CountOf(lane) <= threshold) {
			continue;
		}
		if (budget) {
			budget->Charge(vertex_count);
		}
		ZeroedBytes bytes(vertex_count);
		idx_t replayed = 0;
		for (auto &lane_buffers : buffers) {
			auto &entries = lane_buffers[lane];
			for (auto &entry : entries) {
				bytes.data[entry.vertex] = static_cast<uint8_t>(entry.distance + 1);
			}
			replayed += entries.size();
			vector<ReachedEntry>().swap(entries);
		}
		if (budget) {
			budget->Release(replayed * ReachResultBudget::ENTRY_GROWTH_BYTES);
		}
		dense[lane] = std::move(bytes);
	}
}

ReachResult LaneReachRecorder::Take(idx_t lane, idx_t source, bool source_in_range) {
	ReachResult result;
	if (!source_in_range) {
		return result; // reaches nothing, as the per-source search (Initialize returns before the source)
	}
	if (dense[lane].data) {
		result.distances = std::move(dense[lane]);
		result.distances.data[source] = 1; // distance 0
		result.count = 1;
		for (idx_t v = 0; v < result.distances.size; v++) {
			result.count += v != source && result.distances.data[v] != 0;
		}
		return result;
	}
	result.entries.reserve(CountOf(lane) + 1);
	result.entries.push_back({NumericCast<uint32_t>(source), 0});
	for (auto &lane_buffers : buffers) {
		auto &entries = lane_buffers[lane];
		result.entries.insert(result.entries.end(), entries.begin(), entries.end());
		vector<ReachedEntry>().swap(entries);
	}
	result.count = result.entries.size();
	return result;
}

shared_ptr<Event> IterativeLengthState::CreateBFSEvent(Pipeline &pipeline, const PhysicalPathFinding *op) {
	auto self = shared_ptr_cast<BFSState, IterativeLengthState>(shared_from_this());
	return make_shared_ptr<PhasedKernelEvent>(shared_ptr<PhasedKernel>(self), pipeline, *op);
}

} // namespace duckdb
