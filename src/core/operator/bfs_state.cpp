#include "duckpgq/core/operator/bfs_state.hpp"

#include "duckdb/storage/buffer_manager.hpp"

#include <cstring>
#include <duckpgq/core/utils/parallel_for.hpp>

#include <duckpgq/core/operator/shortest_path/shortest_path_event.hpp>
#include <duckpgq/core/operator/iterative_length/iterative_length_state.hpp>

#include <duckpgq/core/option/duckpgq_option.hpp>
#include <duckpgq/core/utils/compressed_sparse_row.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

#include <chrono>
#include <iomanip>
#include <sstream>

namespace duckdb {

static string CreateBenchmarkRunId() {
	auto now = std::chrono::system_clock::now();
	auto time_t_now = std::chrono::system_clock::to_time_t(now);
	auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count() % 1000000;
	std::stringstream ss;
	ss << std::put_time(std::localtime(&time_t_now), "%Y-%m-%d_%H-%M-%S") << "_" << std::setfill('0') << std::setw(6)
	   << micros;
	return ss.str();
}

BFSState::BFSState(const shared_ptr<PathFindingBatch> &batch_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
                   idx_t num_threads_, string mode_, ClientContext &context_, int64_t vsize_)
    : batch(batch_), pairs(batch->search_pairs), local_csrs(local_csrs_), context(context_), num_threads(num_threads_),
      mode(std::move(mode_)), v_size(vsize_), src_data(pairs->data[0]), dst_data(pairs->data[1]) {
	bfs_type = mode == "shortestpath" ? LogicalType::LIST(LogicalType::BIGINT) : LogicalType::BIGINT;
	// Only have to initialize the current batch and state once.
	total_pairs_processed = 0; // Initialize the total pairs processed
	current_batch_path_list_len = 0;
	started_searches = 0; // reset
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

	tasks_scheduled = 0;
	benchmark_enabled = GetPathFindingBenchmarkOption(context);
	benchmark_lane_activity_enabled = benchmark_enabled && GetPathFindingBenchmarkLaneActivityOption(context);
	benchmark_output_prefix = GetPathFindingBenchmarkPrefix(context);
	benchmark_run_id = CreateBenchmarkRunId();

	// CreateTasks();
}

BFSState::~BFSState() = default; // Define the virtual destructor

BFSWorkingSet::BFSWorkingSet(ClientContext &context, idx_t v_size_p, idx_t threads) : v_size(v_size_p) {
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
	Zero(threads);
}

void BFSWorkingSet::Zero(idx_t threads) {
	if (v_size == 0) {
		return;
	}
	auto bytes = BytesFor(v_size);
	auto base = handle.GetDataMutable();
	// One thread per 8 MB at most: small sets are not worth the thread start-up
	threads = std::max<idx_t>(1, std::min<idx_t>(threads, bytes / (8 * 1024 * 1024) + 1));
	ParallelFor(bytes, threads, [&](idx_t begin, idx_t end, idx_t) { memset(base + begin, 0, end - begin); });
	dirty = false;
}

void BFSState::AttachWorkingSet(shared_ptr<BFSWorkingSet> set) {
	D_ASSERT(set && set->v_size == static_cast<idx_t>(v_size));
	if (set->dirty) {
		set->Zero(num_threads);
	}
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
	AttachWorkingSet(make_shared_ptr<BFSWorkingSet>(context, static_cast<idx_t>(v_size), num_threads));
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

void BFSState::InitializeLanes() {
	auto &result_validity = FlatVector::ValidityMutable(pf_results->data[0]);
	std::bitset<LANE_LIMIT> seen_mask;
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
				visit1[src[src_pos]][lane] = true;
				if (active_bitmap) {
					active_bitmap[src[src_pos] >> 6] |= uint64_t(1) << (src[src_pos] & 63);
				}
				// bfs_state->seen[bfs_state->src[src_pos]][lane] = true;
				lane_to_num[lane] = search_num; // active lane
				lane_active[lane] = true;
				active++;
				seen_mask[lane] = false;
				break;
			}
		}
	}
	FillSeen(seen_mask);
}

void BFSState::FillSeen(const std::bitset<LANE_LIMIT> &seen_mask) {
	// 64 B per vertex: 76 MB at SF300, 241 MB on cit-Patents, so in parallel like the zeroing
	auto threads = std::max<idx_t>(1, std::min<idx_t>(num_threads, static_cast<idx_t>(v_size) / 131072 + 1));
	ParallelFor(static_cast<idx_t>(v_size), threads, [&](idx_t begin, idx_t end, idx_t) {
		for (idx_t i = begin; i < end; i++) {
			seen[i] = seen_mask;
		}
	});
}

} // namespace duckdb
