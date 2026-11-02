#include "duckpgq/core/operator/bfs_state.hpp"

#include "duckdb/storage/buffer_manager.hpp"

#include <cstring>

#include <duckpgq/core/option/duckpgq_option.hpp>
#include <duckpgq/core/utils/compressed_sparse_row.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckdb {

BFSState::BFSState(const shared_ptr<PathFindingBatch> &batch_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
                   idx_t num_threads_, string mode_, ClientContext &context_, int64_t vsize_)
    : batch(batch_), pairs(batch->search_pairs), local_csrs(local_csrs_), context(context_), num_threads(num_threads_),
      mode(std::move(mode_)), v_size(vsize_), src_data(pairs->data[0]), dst_data(pairs->data[1]) {
	bfs_type = mode == "shortestpath" ? LogicalType::LIST(LogicalType::BIGINT) : LogicalType::BIGINT;
	started_searches = 0;
	active = 0;
	iter = 1;
	change = false;
	pf_results = make_shared_ptr<DataChunk>();
	pf_results->Initialize(context, {bfs_type});

	local_csr_counter = 0;
	partition_counter = 0;

	// Initialize source and destination vectors
	src_data.ToUnifiedFormat(vdata_src);
	dst_data.ToUnifiedFormat(vdata_dst);
	src = FlatVector::GetData<int64_t>(src_data);
	dst = FlatVector::GetData<int64_t>(dst_data);

	benchmark_enabled = GetPathFindingBenchmarkOption(context);
	benchmark_lane_activity_enabled = benchmark_enabled && GetPathFindingBenchmarkLaneActivityOption(context);
	benchmark_output_prefix = GetPathFindingBenchmarkPrefix(context);
	benchmark_run_id = CreateBenchmarkRunId();
}

BFSState::~BFSState() = default; // Define the virtual destructor

BFSWorkingSet::BFSWorkingSet(ClientContext &context, idx_t v_size_p) : v_size(v_size_p) {
	if (v_size == 0) {
		return;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	handle = buffer_manager.Allocate(MemoryTag::EXTENSION, BytesFor(v_size), true);
	auto base = reinterpret_cast<std::bitset<LANE_LIMIT> *>(handle.GetDataMutable());
	visit1 = base;
	visit2 = base + v_size;
	seen = base + 2 * v_size;
	active_bitmap = reinterpret_cast<uint64_t *>(base + 3 * v_size);
}

void BFSWorkingSet::Slice(idx_t worker, idx_t workers, idx_t &begin, idx_t &end) const {
	auto words = (v_size + 63) / 64;
	auto words_per_worker = (words + workers - 1) / workers;
	begin = std::min(v_size, worker * words_per_worker * 64);
	end = std::min(v_size, (worker + 1) * words_per_worker * 64);
}

void BFSWorkingSet::Prepare(idx_t begin, idx_t end, const std::bitset<LANE_LIMIT> &seen_mask) {
	if (begin >= end) {
		return;
	}
	auto bytes = (end - begin) * sizeof(std::bitset<LANE_LIMIT>);
	memset(visit1 + begin, 0, bytes);
	memset(visit2 + begin, 0, bytes);
	for (idx_t i = begin; i < end; i++) {
		seen[i] = seen_mask;
	}
	// The slice starts on a word; the padding word of the bitmap goes with the last slice
	auto word_end = end == v_size ? ActiveWordsFor(v_size) : end >> 6;
	memset(active_bitmap + (begin >> 6), 0, (word_end - (begin >> 6)) * sizeof(uint64_t));
}

void BFSState::AttachWorkingSet(shared_ptr<BFSWorkingSet> set) {
	D_ASSERT(set && set->v_size == static_cast<idx_t>(v_size));
	working_set = std::move(set);
	visit1 = working_set->visit1;
	visit2 = working_set->visit2;
	seen = working_set->seen;
	active_bitmap = working_set->active_bitmap;
}

void BFSState::EnsureWorkingSet() {
	if (working_set) {
		return;
	}
	AttachWorkingSet(make_shared_ptr<BFSWorkingSet>(context, static_cast<idx_t>(v_size)));
}

void BFSState::ReleaseWorkingSet() {
	visit1 = nullptr;
	visit2 = nullptr;
	seen = nullptr;
	active_bitmap = nullptr;
	working_set.reset();
}

void BFSState::Clear() {
	// Default empty implementation; override in derived classes
}

shared_ptr<Event> BFSState::CreateBFSEvent(Pipeline &, const PhysicalPathFinding *) {
	throw NotImplementedException("CreateBFSEvent must be implemented in a derived class.");
}

void BFSState::AssignLanes() {
	auto &result_validity = FlatVector::ValidityMutable(pf_results->data[0]);
	seen_mask.set();

	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		lane_to_num[lane] = -1;
		while (started_searches < pairs->size()) {
			auto search_num = started_searches++;
			int64_t src_pos = vdata_src.sel->get_index(search_num);
			int64_t dst_pos = vdata_dst.sel->get_index(search_num);
			if (!vdata_src.validity.RowIsValid(src_pos) || !vdata_dst.validity.RowIsValid(dst_pos)) {
				result_validity.SetInvalid(search_num);
			} else if (!IsVertexInRange(src[src_pos]) || !IsVertexInRange(dst[dst_pos])) {
				// A pair endpoint outside the CSR cannot be reached; never index the lane arrays with it
				result_validity.SetInvalid(search_num);
			} else if (src[src_pos] == dst[dst_pos]) {
				pf_results->data[0].SetValue(search_num, 0);
			} else {
				lane_to_num[lane] = search_num; // active lane
				lane_active[lane] = true;
				active++;
				seen_mask[lane] = false;
				break;
			}
		}
	}
}

void BFSState::PrepareWorkingSetSlice(idx_t worker, idx_t workers) {
	idx_t begin;
	idx_t end;
	working_set->Slice(worker, workers, begin, end);
	working_set->Prepare(begin, end, seen_mask);
}

void BFSState::MarkLaneSources() {
	for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
		auto search_num = lane_to_num[lane];
		if (search_num < 0) {
			continue;
		}
		auto source = src[vdata_src.sel->get_index(search_num)];
		visit1[source][lane] = true;
		if (active_bitmap) {
			active_bitmap[source >> 6] |= uint64_t(1) << (source & 63);
		}
	}
}

void BFSState::ReachDetect() {
	auto result_data = FlatVector::GetDataMutable<int64_t>(pf_results->data[0]);
	for (idx_t lane = 0; lane < LANE_LIMIT; lane++) {
		int64_t search_num = lane_to_num[lane];
		if (search_num >= 0) {
			int64_t dst_pos = vdata_dst.sel->get_index(search_num);
			if (seen[dst[dst_pos]][lane]) {
				result_data[search_num] = iter;
				lane_to_num[lane] = -1;
				active--;
				lane_active[lane] = false;
			}
		}
	}
	// Lanes still active after the bound have no path within it; UnReachableSet marks them NULL
	if (active == 0 || DepthBoundReached(iter)) {
		change = false;
	}
	iter++;
}

void BFSState::UnReachableSet() {
	auto result_data = FlatVector::GetDataMutable<int64_t>(pf_results->data[0]);
	auto &result_validity = FlatVector::ValidityMutable(pf_results->data[0]);
	for (idx_t lane = 0; lane < LANE_LIMIT; lane++) {
		int64_t search_num = lane_to_num[lane];
		if (search_num >= 0) {
			result_validity.SetInvalid(search_num);
			result_data[search_num] = (int64_t)-1;
			lane_to_num[lane] = -1;
			lane_active[lane] = false;
		}
	}
}

void BFSState::AppendBatchPhaseTiming(const string &label, double time_ms) const {
	AppendPhaseTimingRow(benchmark_output_prefix, label, benchmark_run_id, num_threads, pairs->size(), v_size - 2,
	                     GetLocalCSREdgeCount(local_csrs), local_csrs.size(), time_ms, 0);
}

} // namespace duckdb
