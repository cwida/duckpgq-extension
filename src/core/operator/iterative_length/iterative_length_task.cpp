#include "duckpgq/core/operator/iterative_length/iterative_length_state.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>

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

bool CheckChange(std::bitset<LANE_LIMIT> *seen, std::bitset<LANE_LIMIT> *next, std::bitset<LANE_LIMIT> *visit,
                 const LocalCSR &local_csr) {
	bool local_change = false;
	for (auto i = local_csr.start_vertex; i < local_csr.end_vertex; i++) {
		if (next[i].any()) {
			next[i] &= ~seen[i];
			seen[i] |= next[i];
			local_change |= next[i].any();
		}
		// Every EXPLORE of this iteration has read `visit`; cleared here it is the empty `next` of the next iteration
		visit[i] = 0;
	}
	return local_change;
}

inline bool IsSuppressed(const uint64_t *suppressed, idx_t position) {
	return suppressed && ((suppressed[position >> 6] >> (position & 63)) & 1);
}

void Explore(const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next, const LocalCSR &local_csr,
             const uint64_t *suppressed) {
	for (const auto &segment : local_csr.segments) {
		for (idx_t row_idx = 0; row_idx < segment.source_vertices.size(); row_idx++) {
			auto source_vertex = segment.source_vertices[row_idx];
			if (!visit[source_vertex].any()) {
				continue;
			}
			for (auto offset = segment.row_offsets[row_idx]; offset < segment.row_offsets[row_idx + 1]; offset++) {
				auto target = segment.edges[offset] + local_csr.start_vertex;
				next[target] |= visit[source_vertex];
			}
		}
	}
	if (!local_csr.segments.empty()) {
		return;
	}
	if (local_csr.HasSparseRows()) {
		for (idx_t row_idx = 0; row_idx < local_csr.source_vertices.size(); row_idx++) {
			auto source_vertex = local_csr.source_vertices[row_idx];
			if (visit[source_vertex].any()) {
				auto start_edges = local_csr.row_offsets[row_idx];
				auto end_edges = local_csr.row_offsets[row_idx + 1];
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
		if (visit[i].any()) {
			auto start_edges = local_csr.v[i].load(std::memory_order_relaxed);
			auto end_edges = local_csr.v[i + 1].load(std::memory_order_relaxed);
			for (auto offset = start_edges; offset < end_edges; offset++) {
				auto n = local_csr.e[offset] + local_csr.start_vertex;
				next[n] |= visit[i];
			}
		}
	}
}

void RunExplore(IterativeLengthState &state, const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next,
                const LocalCSR &local_csr, const uint64_t *suppressed) {
	if (!state.benchmark_enabled) {
		Explore(visit, next, local_csr, suppressed);
		return;
	}
	auto start_time = std::chrono::high_resolution_clock::now();
	Explore(visit, next, local_csr, suppressed);
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
}

bool IterativeLengthState::Start() {
	phase_start_time = std::chrono::steady_clock::now();
	// Never more workers than partitions: the parallel phases hand out partitions
	worker_count = std::max<idx_t>(1, std::min<idx_t>(worker_count, std::max<idx_t>(1, local_csrs.size())));
	tasks_scheduled = worker_count;
	worker_changed.assign(worker_count, 0);
	// The lane bitsets are only allocated once the batch actually runs, and handed back when it is done
	EnsureWorkingSet();
	if (started_searches >= pairs->size()) {
		return false;
	}
	InitializeLanes();
	BeginIteration();
	return true;
}

void IterativeLengthState::RunPhase(idx_t worker_id) {
	auto &visit = iter & 1 ? visit1 : visit2;
	auto &next = iter & 1 ? visit2 : visit1;
	if (phase == Phase::EXPLORE) {
		while (true) {
			auto partition_idx = local_csr_counter.fetch_add(1);
			if (partition_idx >= static_cast<int64_t>(local_csrs.size())) {
				break;
			}
			auto local_csr = local_csrs[partition_idx].get();
			if (!local_csr) {
				throw InternalException("Tried to reference nullptr for LocalCSR");
			}
			auto suppressed_bits = partition_idx < static_cast<int64_t>(suppressed.size()) && suppressed[partition_idx]
			                           ? suppressed[partition_idx]->data()
			                           : nullptr;
			RunExplore(*this, visit, next, *local_csr, suppressed_bits);
			// Edges committed after the epoch, for the same destination range
			if (partition_idx < static_cast<int64_t>(delta_csrs.size()) && delta_csrs[partition_idx]) {
				RunExplore(*this, visit, next, *delta_csrs[partition_idx], nullptr);
			}
		}
		return;
	}
	bool local_change = false;
	while (true) {
		auto partition_idx = partition_counter.fetch_add(1);
		if (partition_idx >= static_cast<int64_t>(local_csrs.size())) {
			break;
		}
		local_change |= CheckChange(seen, next, visit, *local_csrs[partition_idx]);
	}
	worker_changed[worker_id] = local_change ? 1 : 0;
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
	for (idx_t i = 0; i < worker_count; i++) {
		any_change |= worker_changed[i] != 0;
	}
	change = any_change;
	ReachDetect(*this);
	if (change) {
		BeginIteration();
		return true;
	}
	UnReachableSet(*this);
	Clear();
	if (started_searches < pairs->size()) {
		InitializeLanes();
		BeginIteration();
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
