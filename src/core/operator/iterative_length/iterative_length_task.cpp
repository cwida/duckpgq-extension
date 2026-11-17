#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <mutex>

namespace duckdb {

namespace {

//! Sets or clears the summary bits of [begin, end) within one 64-vertex word from `bits`; the word may be shared with
//! the neighbouring partition at a range boundary, so it is updated atomically
inline void StoreActiveWord(uint64_t *active, idx_t word_idx, uint64_t range_mask, uint64_t bits) {
	auto &word = *reinterpret_cast<std::atomic<uint64_t> *>(active + word_idx);
	if (range_mask == ~uint64_t(0)) {
		word.store(bits, std::memory_order_relaxed);
		return;
	}
	word.fetch_and(~range_mask, std::memory_order_relaxed);
	if (bits) {
		word.fetch_or(bits, std::memory_order_relaxed);
	}
}

//! ON_REACHED(vertex, lanes) is called for every vertex some lane newly reached, with exactly those lanes (the reach
//! mode records them; the destination check of lengths passes a no-op)
template <class ON_REACHED>
bool CheckChange(std::bitset<LANE_LIMIT> *seen, std::bitset<LANE_LIMIT> *next, std::bitset<LANE_LIMIT> *visit,
                 uint64_t *active, idx_t range_begin, idx_t range_end, idx_t &active_count,
                 const std::bitset<LANE_LIMIT> &active_lanes, ON_REACHED &&on_reached) {
	bool local_change = false;
	idx_t word_idx = range_begin >> 6;
	uint64_t word_bits = 0;
	uint64_t word_mask = 0;
	for (auto i = range_begin; i < range_end; i++) {
		bool now_active = false;
		if (next[i].any()) {
			// A lane that found everything it looked for stops propagating (Then et al.)
			next[i] &= active_lanes;
			next[i] &= ~seen[i];
			seen[i] |= next[i];
			now_active = next[i].any();
			local_change |= now_active;
			active_count += now_active;
			if (now_active) {
				on_reached(i, next[i]);
			}
		}
		// Every EXPLORE of this iteration has read `visit`; cleared here it is the empty `next` of the next iteration
		visit[i] = 0;
		if (active) {
			// The frontier of the coming iteration, summarised one bit per vertex
			auto bit = uint64_t(1) << (i & 63);
			word_mask |= bit;
			if (now_active) {
				word_bits |= bit;
			}
			if ((i & 63) == 63 || i + 1 == range_end) {
				StoreActiveWord(active, word_idx, word_mask, word_bits);
				word_idx++;
				word_bits = 0;
				word_mask = 0;
			}
		}
	}
	return local_change;
}

inline bool IsActive(const uint64_t *active, idx_t vertex) {
	return !active || ((active[vertex >> 6] >> (vertex & 63)) & 1);
}

//! Narrows [start_edges, end_edges) of a row whose destinations are sorted to those in [dst_begin, dst_end)
inline void RowSubRange(const LocalCSR &local_csr, uint32_t &start_edges, uint32_t &end_edges, idx_t dst_begin,
                        idx_t dst_end) {
	auto lo = static_cast<uint16_t>(dst_begin - local_csr.start_vertex);
	auto hi_global = dst_end - local_csr.start_vertex;
	auto base = local_csr.e.data();
	auto first = base + start_edges;
	auto last = base + end_edges;
	auto begin_it = std::lower_bound(first, last, lo);
	auto end_it = hi_global >= 65536 ? last : std::lower_bound(begin_it, last, static_cast<uint16_t>(hi_global));
	start_edges = NumericCast<uint32_t>(begin_it - base);
	end_edges = NumericCast<uint32_t>(end_it - base);
}

inline bool IsSuppressed(const uint64_t *suppressed, idx_t position) {
	return suppressed && ((suppressed[position >> 6] >> (position & 63)) & 1);
}

//! The explore phase of one partition: for every source with edges into the partition whose lanes are active, OR
//! its visit line into the lines of its targets. `active` (may be null) skips inactive sources without reading their
//! 64-byte visit line; `prefetch` sources ahead the visit line of the next active source is requested early, the
//! source list being sequential while the visit lines are scattered (W31 step 3).
//! FILTER: only edges whose destination lies in [dst_begin, dst_end) are applied, so several tasks can share one
//! partition with disjoint writes (W32: coarse partitions keep the parallelism of a single batch)
template <bool FILTER>
void Explore(const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next, const LocalCSR &local_csr,
             const uint64_t *suppressed, const uint64_t *active, idx_t prefetch, idx_t dst_begin, idx_t dst_end) {
	for (const auto &segment : local_csr.segments) {
		auto rows = segment.source_vertices.size();
		for (idx_t row_idx = 0; row_idx < rows; row_idx++) {
			auto source_vertex = segment.source_vertices[row_idx];
			if (prefetch && row_idx + prefetch < rows) {
				auto ahead = segment.source_vertices[row_idx + prefetch];
				if (IsActive(active, ahead)) {
					__builtin_prefetch(visit + ahead);
				}
			}
			if (!IsActive(active, source_vertex) || !visit[source_vertex].any()) {
				continue;
			}
			for (auto offset = segment.row_offsets[row_idx]; offset < segment.row_offsets[row_idx + 1]; offset++) {
				auto target = segment.edges[offset] + local_csr.start_vertex;
				if (FILTER && (target < dst_begin || target >= dst_end)) {
					continue;
				}
				next[target] |= visit[source_vertex];
			}
		}
	}
	if (!local_csr.segments.empty()) {
		return;
	}
	if (local_csr.HasSparseRows()) {
		auto rows = local_csr.source_vertices.size();
		for (idx_t row_idx = 0; row_idx < rows; row_idx++) {
			auto source_vertex = local_csr.source_vertices[row_idx];
			if (prefetch && row_idx + prefetch < rows) {
				auto ahead = local_csr.source_vertices[row_idx + prefetch];
				if (IsActive(active, ahead)) {
					__builtin_prefetch(visit + ahead);
				}
			}
			if (IsActive(active, source_vertex) && visit[source_vertex].any()) {
				auto start_edges = local_csr.row_offsets[row_idx];
				auto end_edges = local_csr.row_offsets[row_idx + 1];
				if (FILTER) {
					// Rows are sorted by destination: only this task's slice of the row is read
					RowSubRange(local_csr, start_edges, end_edges, dst_begin, dst_end);
				}
				for (auto offset = start_edges; offset < end_edges; offset++) {
					if (IsSuppressed(suppressed, offset)) {
						continue;
					}
					auto n = local_csr.e[offset] + local_csr.start_vertex;
					next[n] |= visit[source_vertex];
				}
			}
		}
		return;
	}
	for (idx_t i = 0; i < local_csr.GetVertexSize(); i++) {
		if (IsActive(active, i) && visit[i].any()) {
			auto start_edges = local_csr.v[i].load(std::memory_order_relaxed);
			auto end_edges = local_csr.v[i + 1].load(std::memory_order_relaxed);
			for (auto offset = start_edges; offset < end_edges; offset++) {
				auto n = local_csr.e[offset] + local_csr.start_vertex;
				if (FILTER && (n < dst_begin || n >= dst_end)) {
					continue;
				}
				next[n] |= visit[i];
			}
		}
	}
}

//! The explore phase of one partition on a sparse level: every active vertex is looked up in the partition (row
//! buckets, about two rows per bucket) and its edges into the partition's destination range are relaxed. Writes stay
//! within the partition, as in the sweep, so partitions remain the parallel unit.
template <bool FILTER>
void ExploreSparse(const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next, const LocalCSR &local_csr,
                   const uint64_t *suppressed, const vector<uint32_t> &frontier, idx_t dst_begin, idx_t dst_end) {
	for (auto source_vertex : frontier) {
		uint32_t start_edges;
		uint32_t end_edges;
		if (!local_csr.GetRowEdges(source_vertex, start_edges, end_edges)) {
			continue;
		}
		const auto &lanes = visit[source_vertex];
		if (FILTER) {
			RowSubRange(local_csr, start_edges, end_edges, dst_begin, dst_end);
		}
		for (auto offset = start_edges; offset < end_edges; offset++) {
			if (IsSuppressed(suppressed, offset)) {
				continue;
			}
			next[local_csr.e[offset] + local_csr.start_vertex] |= lanes;
		}
	}
}

void RunExplore(IterativeLengthState &state, const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next,
                const LocalCSR &local_csr, const uint64_t *suppressed, idx_t dst_begin, idx_t dst_end,
                bool whole_partition) {
	// Dense levels sweep the partition's rows without consulting the bitmap: when most vertices have an active lane
	// the per-source bit test only costs (SF10, 65536 pairs: 10% slower with it); the bitmap's job is the frontier
	// list of a sparse level
	const uint64_t *active = state.use_frontier_bitmap && state.bitmap_filter ? state.active_bitmap : nullptr;
	if (state.sparse_level) {
		if (whole_partition) {
			ExploreSparse<false>(visit, next, local_csr, suppressed, state.frontier_list, dst_begin, dst_end);
		} else {
			ExploreSparse<true>(visit, next, local_csr, suppressed, state.frontier_list, dst_begin, dst_end);
		}
		return;
	}
	auto explore = [&]() {
		if (whole_partition) {
			Explore<false>(visit, next, local_csr, suppressed, active, state.visit_prefetch, dst_begin, dst_end);
		} else {
			Explore<true>(visit, next, local_csr, suppressed, active, state.visit_prefetch, dst_begin, dst_end);
		}
	};
	PhaseTimer timer(state.benchmark_enabled);
	explore();
	if (!state.benchmark_enabled) {
		return;
	}
	std::lock_guard<std::mutex> guard(state.log_mutex);
	RecordExploreTiming(state.timing_data, timer.ElapsedMs(), state.num_threads, local_csr.GetVertexSize(),
	                    local_csr.GetEdgeSize(), state.local_csrs.size(), state.iter);
}

//===--------------------------------------------------------------------===//
// Flat-CSR explore variants: the ablation of the partitioned CSR (ExploreVariant)
//===--------------------------------------------------------------------===//
static constexpr idx_t LANE_WORDS = LANE_LIMIT / 64;

//! The owner of the destination writes it: no synchronisation (FLAT_SERIAL, FLAT_RANGES)
struct OwnedWrite {
	inline void operator()(std::bitset<LANE_LIMIT> *next, uint32_t target, const std::bitset<LANE_LIMIT> &lanes) const {
		next[target] |= lanes;
	}
};

//! Any worker may write any destination: each 64-lane word is updated with fetch_or. The word is read first, since
//! most relaxations of a dense level add no lane and an unconditional fetch_or would take the line exclusive each time
struct AtomicWrite {
	inline void operator()(std::bitset<LANE_LIMIT> *next, uint32_t target, const std::bitset<LANE_LIMIT> &lanes) const {
		static_assert(sizeof(std::bitset<LANE_LIMIT>) == LANE_LIMIT / 8, "lane bitsets are updated as 64-bit words");
		uint64_t add[LANE_WORDS];
		memcpy(add, &lanes, sizeof(add));
		auto words = reinterpret_cast<std::atomic<uint64_t> *>(next + target);
		for (idx_t word_idx = 0; word_idx < LANE_WORDS; word_idx++) {
			if (add[word_idx] & ~words[word_idx].load(std::memory_order_relaxed)) {
				words[word_idx].fetch_or(add[word_idx], std::memory_order_relaxed);
			}
		}
	}
};

//! Any worker may write any destination under the spinlock of the destination's stripe
struct LockedWrite {
	LaneStripeLock *locks;
	inline void operator()(std::bitset<LANE_LIMIT> *next, uint32_t target, const std::bitset<LANE_LIMIT> &lanes) const {
		auto &lock = locks[(target >> IterativeLengthState::STRIPE_SHIFT) & (IterativeLengthState::STRIPE_COUNT - 1)];
		lock.Lock();
		next[target] |= lanes;
		lock.Unlock();
	}
};

//! Relaxes the edges of `source`, or those whose destination lies in [dst_begin, dst_end) (rows are sorted by
//! destination)
template <class WRITE>
inline void RelaxFlatRow(const FlatCSR &csr, idx_t source, const std::bitset<LANE_LIMIT> *visit,
                         std::bitset<LANE_LIMIT> *next, bool whole_row, uint32_t dst_begin, uint32_t dst_end,
                         const WRITE &write) {
	auto first = csr.destinations.data() + csr.offsets[source];
	auto last = csr.destinations.data() + csr.offsets[source + 1];
	if (!whole_row) {
		first = std::lower_bound(first, last, dst_begin);
		last = std::lower_bound(first, last, dst_end);
	}
	const auto &lanes = visit[source];
	for (auto edge = first; edge != last; edge++) {
		write(next, *edge, lanes);
	}
}

//! Sources [begin, end) of the level: positions of the frontier list on a sparse level, vertices on a dense one
//! (skipping those without edges or without an active lane, the bitmap test first when `active` is given)
template <class WRITE>
void ExploreFlat(const FlatCSR &csr, const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next,
                 const vector<uint32_t> *frontier, const uint64_t *active, idx_t begin, idx_t end, bool whole_row,
                 uint32_t dst_begin, uint32_t dst_end, const WRITE &write) {
	if (frontier) {
		for (idx_t i = begin; i < end; i++) {
			RelaxFlatRow(csr, (*frontier)[i], visit, next, whole_row, dst_begin, dst_end, write);
		}
		return;
	}
	for (idx_t source = begin; source < end; source++) {
		if (csr.offsets[source] == csr.offsets[source + 1] || !IsActive(active, source) || !visit[source].any()) {
			continue;
		}
		RelaxFlatRow(csr, source, visit, next, whole_row, dst_begin, dst_end, write);
	}
}

uint64_t GetWord(const std::bitset<LANE_LIMIT> &bitset, idx_t word_idx) {
	uint64_t word = 0;
	for (idx_t i = 0; i < 64; ++i) {
		auto bit_idx = word_idx * 64 + i;
		if (bit_idx < LANE_LIMIT && bitset.test(bit_idx)) {
			word |= (1ULL << i);
		}
	}
	return word;
}

void WriteLaneActivity(IterativeLengthState &state, const std::bitset<LANE_LIMIT> *visit) {
	auto file_name = state.benchmark_output_prefix + "_lane_activity_" + state.benchmark_run_id + ".csv";
	std::ofstream outfile(file_name, std::ios_base::app);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding lane activity benchmark file \"%s\"", file_name);
	}
	if (outfile.tellp() == 0) {
		outfile << "Iter,Lane,ActiveVertices\n";
	}
	std::vector<int64_t> lane_activity(LANE_LIMIT, 0);
	static constexpr idx_t WORD_COUNT = (LANE_LIMIT + 63) / 64;
	for (idx_t v = 0; v < state.v_size; ++v) {
		for (idx_t word_idx = 0; word_idx < WORD_COUNT; word_idx++) {
			auto word = GetWord(visit[v], word_idx);
			for (idx_t bit = 0; bit < 64; ++bit) {
				auto lane = word_idx * 64 + bit;
				if (lane < LANE_LIMIT) {
					lane_activity[lane] += (word >> bit) & 1;
				}
			}
		}
	}
	for (idx_t lane = 0; lane < LANE_LIMIT; ++lane) {
		outfile << state.iter << "," << lane << "," << lane_activity[lane] << "\n";
	}
	// Lane -1: vertices with any lane active, the sources the explore phase has to read
	int64_t any_active = 0;
	for (idx_t v = 0; v < state.v_size; ++v) {
		any_active += visit[v].any();
	}
	outfile << state.iter << ",-1," << any_active << "\n";
}

} // namespace

//===--------------------------------------------------------------------===//
// PhasedKernel
//===--------------------------------------------------------------------===//
void IterativeLengthState::SetWorkerCount(idx_t workers) {
	worker_count = std::max<idx_t>(1, workers);
}

void IterativeLengthState::SetTimingLabel(const string &label) {
	timing_label = label;
}

idx_t IterativeLengthState::WorkerCount() const {
	return worker_count;
}

void IterativeLengthState::BeginIteration() {
	phase = Phase::EXPLORE;
	partition_counter = 0;
	local_csr_counter = 0;
	change = false;
	std::fill(worker_changed.begin(), worker_changed.end(), 0);
	worker_active_counts.assign(worker_count, 0);
	sparse_level = false;
}

//! Chooses the sweep or the row-lookup expansion for the coming level from the number of active vertices, and
//! lists them from the summary bitmap when looking up
void IterativeLengthState::PrepareLevel(idx_t active_total) {
	sparse_level = row_lookup_available && use_frontier_bitmap && active_bitmap && sparse_level_divisor > 0 &&
	               active_total * sparse_level_divisor < static_cast<idx_t>(v_size);
	if (!sparse_level) {
		return;
	}
	frontier_list.clear();
	frontier_list.reserve(active_total);
	auto words = BFSWorkingSet::ActiveWordsFor(static_cast<idx_t>(v_size));
	for (idx_t word_idx = 0; word_idx < words; word_idx++) {
		auto word = active_bitmap[word_idx];
		while (word) {
			auto bit = static_cast<idx_t>(__builtin_ctzll(word));
			auto vertex = (word_idx << 6) + bit;
			if (vertex < static_cast<idx_t>(v_size)) {
				frontier_list.push_back(NumericCast<uint32_t>(vertex));
			}
			word &= word - 1;
		}
	}
}

//! The destination range of task sub_idx within a partition: the partition's vertex range split into sub_ranges
//! pieces (whole partition when sub_ranges is one)
void IterativeLengthState::SubRange(const LocalCSR &local_csr, idx_t sub_idx, idx_t &begin, idx_t &end) const {
	auto span = local_csr.end_vertex - local_csr.start_vertex;
	begin = local_csr.start_vertex + span * sub_idx / sub_ranges;
	end = sub_idx + 1 == sub_ranges ? local_csr.end_vertex : local_csr.start_vertex + span * (sub_idx + 1) / sub_ranges;
}

bool IterativeLengthState::Start() {
	phase_start_time = std::chrono::steady_clock::now();
	// Fewer partitions than workers: split every partition's destination range so each worker has a task per phase
	// (W32: coarse partitions cost a single batch its parallelism otherwise)
	auto partitions = std::max<idx_t>(1, local_csrs.size());
	sub_ranges = 1;
	if (sub_range_tasks && worker_count > partitions) {
		// A sub-range task re-reads the partition's rows, so split only as far as the query's batches leave
		// threads idle (the scheduler's hint), not up to this batch's own worker count
		auto wanted = sub_range_hint > 0 ? sub_range_hint : (worker_count + partitions - 1) / partitions;
		sub_ranges = std::max<idx_t>(1, std::min<idx_t>(16, wanted));
	}
	worker_count = std::max<idx_t>(1, std::min<idx_t>(worker_count, partitions * sub_ranges));
	worker_changed.assign(worker_count, 0);
	if (reach_recorder) {
		reach_recorder->Resize(worker_count, NumericCast<idx_t>(v_size), reach_dense_divisor);
	}
	PrepareFlatExplore();
	// The lane bitsets are only allocated once the batch actually runs, and handed back when it is done
	EnsureWorkingSet();
	if (started_searches >= pairs->size()) {
		return false;
	}
	BeginWave();
	return true;
}

void IterativeLengthState::BeginWave() {
	AssignLanes();
	phase = Phase::PREPARE;
}

void IterativeLengthState::RunPhase(idx_t worker_id) {
	if (phase == Phase::PREPARE) {
		PrepareWorkingSetSlice(worker_id, worker_count);
		return;
	}
	if (phase == Phase::COLLECT) {
		RunCollect(worker_id);
		return;
	}
	auto &visit = iter & 1 ? visit1 : visit2;
	auto &next = iter & 1 ? visit2 : visit1;
	if (phase == Phase::EXPLORE && explore_variant != ExploreVariant::PARTITIONED) {
		RunFlatExplore(visit, next);
		return;
	}
	if (phase == Phase::EXPLORE) {
		auto task_count = static_cast<int64_t>(local_csrs.size() * sub_ranges);
		while (true) {
			auto task_idx = local_csr_counter.fetch_add(1);
			if (task_idx >= task_count) {
				break;
			}
			auto partition_idx = task_idx / static_cast<int64_t>(sub_ranges);
			auto sub_idx = static_cast<idx_t>(task_idx % static_cast<int64_t>(sub_ranges));
			auto local_csr = local_csrs[partition_idx].get();
			if (!local_csr) {
				throw InternalException("Tried to reference nullptr for LocalCSR");
			}
			idx_t dst_begin;
			idx_t dst_end;
			SubRange(*local_csr, sub_idx, dst_begin, dst_end);
			bool whole = sub_ranges == 1;
			auto suppressed_bits = partition_idx < static_cast<int64_t>(suppressed.size()) && suppressed[partition_idx]
			                           ? suppressed[partition_idx]->data()
			                           : nullptr;
			RunExplore(*this, visit, next, *local_csr, suppressed_bits, dst_begin, dst_end, whole);
			// Edges committed after the epoch, for the same destination range
			if (partition_idx < static_cast<int64_t>(delta_csrs.size()) && delta_csrs[partition_idx]) {
				RunExplore(*this, visit, next, *delta_csrs[partition_idx], nullptr, dst_begin, dst_end, whole);
			}
		}
		return;
	}
	bool local_change = false;
	idx_t active_count = 0;
	auto task_count = static_cast<int64_t>(local_csrs.size() * sub_ranges);
	while (true) {
		auto task_idx = partition_counter.fetch_add(1);
		if (task_idx >= task_count) {
			break;
		}
		auto partition_idx = task_idx / static_cast<int64_t>(sub_ranges);
		idx_t range_begin;
		idx_t range_end;
		SubRange(*local_csrs[partition_idx], static_cast<idx_t>(task_idx % static_cast<int64_t>(sub_ranges)),
		         range_begin, range_end);
		if (reach_recorder) {
			// Reach lanes: every newly reached (vertex, lane) of this range goes to this worker's buffers at `iter`
			auto level = NumericCast<int32_t>(iter);
			local_change |= CheckChange(seen, next, visit, active_bitmap, range_begin, range_end, active_count,
			                            lane_active, [&](idx_t vertex, const std::bitset<LANE_LIMIT> &lanes) {
				                            reach_recorder->Record(worker_id, vertex, level, lanes);
			                            });
		} else {
			local_change |= CheckChange(seen, next, visit, active_bitmap, range_begin, range_end, active_count,
			                            lane_active, [](idx_t, const std::bitset<LANE_LIMIT> &) {});
		}
	}
	worker_changed[worker_id] = local_change ? 1 : 0;
	worker_active_counts[worker_id] = active_count;
}

void IterativeLengthState::PrepareFlatExplore() {
	if (explore_variant == ExploreVariant::PARTITIONED) {
		return;
	}
	// The flat CSR holds the epoch's edges only: a reader with delta edges or tombstones needs the partitioned explore
	for (auto &delta : delta_csrs) {
		if (delta && delta->GetEdgeSize() > 0) {
			throw NotImplementedException("experimental_path_finding_operator_explore_variant: the flat variants do "
			                              "not read edges committed after the index epoch");
		}
	}
	for (auto &bits : suppressed) {
		if (bits) {
			throw NotImplementedException("experimental_path_finding_operator_explore_variant: the flat variants do "
			                              "not read the tombstones of an index snapshot");
		}
	}
	flat_csr = FlatCSR::Get(local_csrs, static_cast<idx_t>(v_size));
	if (explore_variant == ExploreVariant::FLAT_LOCKED && !stripe_locks) {
		stripe_locks = unique_ptr<LaneStripeLock[]>(new LaneStripeLock[STRIPE_COUNT]);
	}
}

void IterativeLengthState::RunFlatExplore(const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next) {
	// Sources per morsel of FLAT_ATOMIC and FLAT_LOCKED: vertices of a dense level, frontier entries of a sparse one
	static constexpr idx_t DENSE_MORSEL = 2048;
	static constexpr idx_t SPARSE_MORSEL = 256;
	auto &csr = *flat_csr;
	auto vertices = static_cast<idx_t>(v_size);
	const vector<uint32_t> *frontier = sparse_level ? &frontier_list : nullptr;
	const uint64_t *active = use_frontier_bitmap && bitmap_filter ? active_bitmap : nullptr;
	auto sources = frontier ? frontier->size() : vertices;
	switch (explore_variant) {
	case ExploreVariant::FLAT_SERIAL:
		// One partition has one owner: the first worker to arrive expands the level
		if (local_csr_counter.fetch_add(1) == 0) {
			ExploreFlat(csr, visit, next, frontier, active, 0, sources, true, 0, 0, OwnedWrite());
		}
		return;
	case ExploreVariant::FLAT_RANGES:
		// A destination range per worker; every task reads every active row for its slice of it
		while (true) {
			auto task_idx = static_cast<idx_t>(local_csr_counter.fetch_add(1));
			if (task_idx >= worker_count) {
				return;
			}
			auto dst_begin = NumericCast<uint32_t>(vertices * task_idx / worker_count);
			auto dst_end = NumericCast<uint32_t>(vertices * (task_idx + 1) / worker_count);
			ExploreFlat(csr, visit, next, frontier, active, 0, sources, false, dst_begin, dst_end, OwnedWrite());
		}
	case ExploreVariant::FLAT_ATOMIC:
	case ExploreVariant::FLAT_LOCKED: {
		auto morsel = frontier ? SPARSE_MORSEL : DENSE_MORSEL;
		while (true) {
			auto begin = static_cast<idx_t>(local_csr_counter.fetch_add(1)) * morsel;
			if (begin >= sources) {
				return;
			}
			auto end = std::min(sources, begin + morsel);
			if (explore_variant == ExploreVariant::FLAT_ATOMIC) {
				ExploreFlat(csr, visit, next, frontier, active, begin, end, true, 0, 0, AtomicWrite());
			} else {
				ExploreFlat(csr, visit, next, frontier, active, begin, end, true, 0, 0,
				            LockedWrite {stripe_locks.get()});
			}
		}
	}
	default:
		throw InternalException("RunFlatExplore called for the partitioned explore");
	}
}

bool IterativeLengthState::AdvancePhase() {
	if (phase == Phase::COLLECT) {
		return false;
	}
	if (phase == Phase::PREPARE) {
		MarkLaneSources();
		BeginIteration();
		// The first level has at most one active vertex per lane
		PrepareLevel(active);
		return true;
	}
	if (phase == Phase::EXPLORE) {
		if (benchmark_lane_activity_enabled) {
			WriteLaneActivity(*this, iter & 1 ? visit1 : visit2);
		}
		phase = Phase::CHECK;
		partition_counter = 0;
		return true;
	}
	bool any_change = false;
	idx_t active_total = 0;
	for (idx_t i = 0; i < worker_count; i++) {
		any_change |= worker_changed[i] != 0;
		active_total += worker_active_counts[i];
	}
	change = any_change;
	DetectReached();
	if (change) {
		BeginIteration();
		PrepareLevel(active_total);
		return true;
	}
	MarkUnreachable();
	if (HasNextWave()) {
		Clear();
		BeginWave();
		return true;
	}
	// The last wave leaves its bits behind: the next batch to attach this working set clears them in its PREPARE phase
	if (StartCollect()) {
		phase = Phase::COLLECT;
		return true;
	}
	return false;
}

bool IterativeLengthState::HasNextWave() {
	return started_searches < pairs->size();
}

void IterativeLengthState::DetectReached() {
	ReachDetect();
}

void IterativeLengthState::MarkUnreachable() {
	UnReachableSet();
}

void IterativeLengthState::Finished() {
	ReleaseWorkingSet();
	if (!benchmark_enabled) {
		return;
	}
	AppendBatchPhaseTiming(timing_label, ElapsedMs(phase_start_time));
	auto heavy_partition_fraction = std::to_string(GetHeavyPartitionFraction(context));
	auto light_partition_multiplier = std::to_string(GetLightPartitionMultiplier(context));
	auto file_name = benchmark_output_prefix + "_explore_timing_" + benchmark_run_id + "_threads_" +
	                 std::to_string(num_threads) + "_" + heavy_partition_fraction + "_" + light_partition_multiplier +
	                 ".csv";
	WriteExploreTimings(file_name, timing_data);
}

} // namespace duckdb
