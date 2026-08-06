#include "duckpgq/core/operator/bfs_state.hpp"

#include <duckpgq/core/operator/shortest_path/shortest_path_event.hpp>
#include <duckpgq/core/operator/iterative_length/iterative_length_event.hpp>

#include <duckpgq/core/option/duckpgq_option.hpp>
#include <duckpgq/core/utils/compressed_sparse_row.hpp>
#include <duckpgq/core/utils/duckpgq_barrier.hpp>
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
	ss << std::put_time(std::localtime(&time_t_now), "%Y-%m-%d_%H-%M-%S") << "_" << std::setfill('0')
	   << std::setw(6) << micros;
	return ss.str();
}

BFSState::BFSState(const shared_ptr<PathFindingBatch> &batch_, std::vector<shared_ptr<LocalCSR>> &local_csrs_,
                   idx_t num_threads_, string mode_, ClientContext &context_, int64_t vsize_)
    : batch(batch_), pairs(batch->search_pairs), local_csrs(local_csrs_), context(context_), num_threads(num_threads_),
      mode(std::move(mode_)),
      v_size(vsize_), src_data(pairs->data[0]), dst_data(pairs->data[1]) {
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
	visit1 = vector<std::bitset<LANE_LIMIT>>(v_size);
	visit2 = vector<std::bitset<LANE_LIMIT>>(v_size);
	seen = vector<std::bitset<LANE_LIMIT>>(v_size);

	local_csr_counter = 0;
	partition_counter = 0;

	// Initialize source and destination vectors
	src_data.ToUnifiedFormat(vdata_src);
	dst_data.ToUnifiedFormat(vdata_dst);
	src = FlatVector::GetData<int64_t>(src_data);
	dst = FlatVector::GetData<int64_t>(dst_data);

	// Initialize the thread assignment vector
	thread_assignment = std::vector<int64_t>(v_size, -1);
	tasks_scheduled = 0;
	benchmark_enabled = GetPathFindingBenchmarkOption(context);
	benchmark_lane_activity_enabled = benchmark_enabled && GetPathFindingBenchmarkLaneActivityOption(context);
	benchmark_output_prefix = GetPathFindingBenchmarkPrefix(context);
	benchmark_run_id = CreateBenchmarkRunId();

	// CreateTasks();
	barrier = make_uniq<Barrier>(num_threads);
}

BFSState::~BFSState() = default; // Define the virtual destructor

void BFSState::Clear() {
	// Default empty implementation; override in derived classes
}

void BFSState::ScheduleBFSBatch(Pipeline &, Event &, const PhysicalPathFinding *) {
	throw NotImplementedException("ScheduleBFSBatch must be implemented in a derived class.");
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
			} else if (src[src_pos] == dst[dst_pos]) {
				pf_results->data[0].SetValue(search_num, 0);
			} else {
				visit1[src[src_pos]][lane] = true;
				// bfs_state->seen[bfs_state->src[src_pos]][lane] = true;
				lane_to_num[lane] = search_num; // active lane
				lane_active[lane] = true;
				active++;
				seen_mask[lane] = false;
				break;
			}
		}
	}
	for (int64_t i = 0; i < v_size; i++) {
		seen[i] = seen_mask;
	}
}

} // namespace duckdb
