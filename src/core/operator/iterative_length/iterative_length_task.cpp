#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <thread>
#ifdef __linux__
#include <sched.h>
#elif defined(__APPLE__)
#include <pthread.h>
#endif

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

bool CheckChange(std::bitset<LANE_LIMIT> *seen, std::bitset<LANE_LIMIT> *next, std::bitset<LANE_LIMIT> *visit,
                 uint64_t *active, idx_t range_begin, idx_t range_end, idx_t &active_count) {
	bool local_change = false;
	idx_t word_idx = range_begin >> 6;
	uint64_t word_bits = 0;
	uint64_t word_mask = 0;
	for (auto i = range_begin; i < range_end; i++) {
		bool now_active = false;
		if (next[i].any()) {
			next[i] &= ~seen[i];
			seen[i] |= next[i];
			now_active = next[i].any();
			local_change |= now_active;
			active_count += now_active;
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
	if (!state.benchmark_enabled) {
		explore();
		return;
	}
	auto start_time = std::chrono::high_resolution_clock::now();
	explore();
	auto end_time = std::chrono::high_resolution_clock::now();
	auto duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
	std::thread::id thread_id = std::this_thread::get_id();
	int core_id = -1;
#ifdef __linux__
	core_id = sched_getcpu();
#elif defined(__APPLE__)
	uint64_t tid;
	pthread_threadid_np(nullptr, &tid);
	auto hardware_threads = std::thread::hardware_concurrency();
	if (hardware_threads > 0) {
		core_id = static_cast<int>(tid % hardware_threads);
	}
#endif
	std::lock_guard<std::mutex> guard(state.log_mutex);
	state.timing_data.emplace_back(thread_id, core_id, duration_ms, state.num_threads, local_csr.GetVertexSize(),
	                               local_csr.GetEdgeSize(), state.local_csrs.size(), state.iter);
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

void ReachDetect(IterativeLengthState &state) {
	auto result_data = FlatVector::GetDataMutable<int64_t>(state.pf_results->data[0]);
	for (idx_t lane = 0; lane < LANE_LIMIT; lane++) {
		int64_t search_num = state.lane_to_num[lane];
		if (search_num >= 0) {
			int64_t dst_pos = state.vdata_dst.sel->get_index(search_num);
			if (state.seen[state.dst[dst_pos]][lane]) {
				result_data[search_num] = state.iter;
				state.lane_to_num[lane] = -1;
				state.active--;
				state.lane_active[lane] = false;
			}
		}
	}
	if (state.active == 0) {
		state.change = false;
	}
	state.iter++;
	// Lanes still active after the bound have no path within it; UnReachableSet marks them NULL
	if (state.max_depth > 0 && static_cast<idx_t>(state.iter) > state.max_depth) {
		state.change = false;
	}
}

void UnReachableSet(IterativeLengthState &state) {
	auto result_data = FlatVector::GetDataMutable<int64_t>(state.pf_results->data[0]);
	auto &result_validity = FlatVector::ValidityMutable(state.pf_results->data[0]);
	for (idx_t lane = 0; lane < LANE_LIMIT; lane++) {
		int64_t search_num = state.lane_to_num[lane];
		if (search_num >= 0) {
			result_validity.SetInvalid(search_num);
			result_data[search_num] = (int64_t)-1;
			state.lane_to_num[lane] = -1;
			state.lane_active[lane] = false;
		}
	}
}

mutex bfs_phase_timing_lock;

size_t GetLocalCSREdgeCount(const std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	size_t edge_count = 0;
	for (const auto &local_csr : partition_csrs) {
		edge_count += local_csr->GetEdgeSize();
	}
	return edge_count;
}

void AppendBFSPhaseTiming(const IterativeLengthState &state, const string &label, double time_ms) {
	auto file_name = state.benchmark_output_prefix + "_phase_timing.csv";
	lock_guard<mutex> lock(bfs_phase_timing_lock);
	bool write_header = !std::filesystem::exists(file_name);
	std::ofstream outfile(file_name, std::ios::app);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding phase benchmark file \"%s\"", file_name);
	}
	if (write_header) {
		outfile << "Phase,RunID,ThreadCount,PairCount,VertexCount,EdgeCount,PartitionCount,Time_ms,MemoryBytes\n";
	}
	outfile << label << "," << state.benchmark_run_id << "," << state.num_threads << "," << state.pairs->size() << ","
	        << state.v_size - 2 << "," << GetLocalCSREdgeCount(state.local_csrs) << "," << state.local_csrs.size()
	        << "," << time_ms << ",0\n";
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
	tasks_scheduled = worker_count;
	worker_changed.assign(worker_count, 0);
	// The lane bitsets are only allocated once the batch actually runs, and handed back when it is done
	EnsureWorkingSet();
	if (started_searches >= pairs->size()) {
		return false;
	}
	InitializeLanes();
	BeginIteration();
	// The first level has at most one active vertex per lane
	PrepareLevel(active);
	return true;
}

void IterativeLengthState::RunPhase(idx_t worker_id) {
	auto &visit = iter & 1 ? visit1 : visit2;
	auto &next = iter & 1 ? visit2 : visit1;
	if (phase == Phase::EXPLORE) {
		idx_t swept_total = 0;
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
			idx_t swept = sub_idx == 0 ? local_csr->GetEdgeSize() : 0;
			// Edges committed after the epoch, for the same destination range
			if (partition_idx < static_cast<int64_t>(delta_csrs.size()) && delta_csrs[partition_idx]) {
				RunExplore(*this, visit, next, *delta_csrs[partition_idx], nullptr, dst_begin, dst_end, whole);
				swept += sub_idx == 0 ? delta_csrs[partition_idx]->GetEdgeSize() : 0;
			}
			swept_total += swept;
		}
		if (edges_scanned && swept_total) {
			// One shared increment per worker per phase, not one per partition
			edges_scanned->fetch_add(swept_total, std::memory_order_relaxed);
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
		local_change |= CheckChange(seen, next, visit, active_bitmap, range_begin, range_end, active_count);
	}
	worker_changed[worker_id] = local_change ? 1 : 0;
	worker_active_counts[worker_id] = active_count;
}

bool IterativeLengthState::AdvancePhase() {
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
	ReachDetect(*this);
	if (change) {
		BeginIteration();
		PrepareLevel(active_total);
		return true;
	}
	UnReachableSet(*this);
	Clear();
	if (started_searches < pairs->size()) {
		InitializeLanes();
		BeginIteration();
		PrepareLevel(active);
		return true;
	}
	return false;
}

void IterativeLengthState::Finished() {
	ReleaseWorkingSet();
	if (!benchmark_enabled) {
		return;
	}
	auto phase_end_time = std::chrono::steady_clock::now();
	auto time_ms = std::chrono::duration<double, std::milli>(phase_end_time - phase_start_time).count();
	AppendBFSPhaseTiming(*this, timing_label, time_ms);
	auto heavy_partition_fraction = std::to_string(GetHeavyPartitionFraction(context));
	auto light_partition_multiplier = std::to_string(GetLightPartitionMultiplier(context));
	auto file_name = benchmark_output_prefix + "_explore_timing_" + benchmark_run_id + "_threads_" +
	                 std::to_string(num_threads) + "_" + heavy_partition_fraction + "_" + light_partition_multiplier +
	                 ".csv";
	WriteTimingResults(file_name);
}

} // namespace duckdb
