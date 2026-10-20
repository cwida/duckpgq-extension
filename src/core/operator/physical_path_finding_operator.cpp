#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/core/utils/csr_build.hpp"
#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>
#include <duckpgq/core/operator/partitioned_csr_cache.hpp>

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/hyperloglog.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckpgq/core/operator/bfs_state.hpp"
#include <duckpgq/core/operator/iterative_length/bidirectional_iterative_length_state.hpp>
#include <duckpgq/core/operator/iterative_length/grouped_iterative_length_event.hpp>
#include <duckpgq/core/operator/iterative_length/iterative_length_state.hpp>
#include <duckpgq/core/operator/iterative_length/push_pull_iterative_length_state.hpp>
#include <duckpgq/core/operator/local_csr/local_csr_event.hpp>
#include <duckpgq/core/option/duckpgq_option.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <duckpgq_state.hpp>
#include <atomic>
#include <chrono>
#include <unordered_set>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <numeric>
#include <sstream>
#include <thread>
#include <unordered_map>

namespace duckdb {

namespace {

static mutex path_finding_operator_phase_timing_lock;

static void UpdatePeak(std::atomic<idx_t> &peak, idx_t value) {
	auto previous = peak.load(std::memory_order_relaxed);
	while (previous < value &&
	       !peak.compare_exchange_weak(previous, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
	}
}

static void SampleCSRBuildMemory(PathFindingGlobalSinkState &gstate) {
	if (!GetPathFindingBenchmarkOption(gstate.context_)) {
		return;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(gstate.context_);
	UpdatePeak(gstate.csr_build_buffer_peak_bytes, buffer_manager.GetUsedMemory());
	UpdatePeak(gstate.csr_build_swap_peak_bytes, buffer_manager.GetUsedSwap());
}

static uint32_t UnpackPartitionedEndpointSource(hash_t endpoint, idx_t radix_bits);

static hash_t PackPartitionedEndpoint(uint32_t source, idx_t partition_idx, uint16_t local_destination,
                                      idx_t radix_bits) {
	D_ASSERT(radix_bits <= RadixPartitioning::MAX_RADIX_BITS);
	auto source_low_bits = 32 - radix_bits;
	auto source_low_mask = (hash_t(1) << source_low_bits) - 1;
	hash_t result = local_destination;
	result |= (static_cast<hash_t>(source) & source_low_mask) << 16;
	result |= static_cast<hash_t>(partition_idx) << RadixPartitioning::Shift(radix_bits);
	if (radix_bits > 0) {
		result |= static_cast<hash_t>(source >> source_low_bits) << 48;
	}
	D_ASSERT(UnpackPartitionedEndpointSource(result, radix_bits) == source);
	D_ASSERT(RadixPartitioning::ApplyMask(result, radix_bits) == partition_idx);
	return result;
}

static uint32_t UnpackPartitionedEndpointSource(hash_t endpoint, idx_t radix_bits) {
	auto source_low_bits = 32 - radix_bits;
	auto source_low_mask = (hash_t(1) << source_low_bits) - 1;
	auto source = static_cast<uint32_t>((endpoint >> 16) & source_low_mask);
	if (radix_bits > 0) {
		source |= static_cast<uint32_t>(endpoint >> 48) << source_low_bits;
	}
	return source;
}

} // namespace

void AppendOperatorPhaseTiming(ClientContext &context, const string &phase, idx_t thread_count, idx_t pair_count,
                               idx_t unique_count, idx_t duplicate_count, double time_ms, idx_t memory_bytes) {
	if (!GetPathFindingBenchmarkOption(context)) {
		return;
	}

	auto file_name = GetPathFindingBenchmarkPrefix(context) + "_phase_timing.csv";
	lock_guard<mutex> lock(path_finding_operator_phase_timing_lock);
	bool write_header = !std::filesystem::exists(file_name);
	std::ofstream outfile(file_name, std::ios::app);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding phase benchmark file \"%s\"", file_name);
	}
	if (write_header) {
		outfile << "Phase,RunID,ThreadCount,PairCount,VertexCount,EdgeCount,PartitionCount,Time_ms,MemoryBytes\n";
	}
	outfile << phase << ",operator," << thread_count << "," << pair_count << ",0," << unique_count << ","
	        << duplicate_count << "," << time_ms << "," << memory_bytes << "\n";
}

namespace {

size_t GetLocalCSREdgeCount(const std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	size_t edge_count = 0;
	for (const auto &local_csr : partition_csrs) {
		edge_count += local_csr->GetEdgeSize();
	}
	return edge_count;
}

idx_t GetPathFindingVSize(const PathFindingGlobalSinkState &gstate) {
	return gstate.edge_input || gstate.cached_partitioned_csr_input ? gstate.vertex_count + 2 : gstate.csr->vsize;
}

idx_t GetPathFindingEdgeCount(const PathFindingGlobalSinkState &gstate) {
	if (gstate.cached_partitioned_csr_input) {
		return gstate.expected_edge_count;
	}
	return gstate.edge_input ? gstate.endpoint_count : gstate.csr->e.size();
}

void RecordStrategy(ClientContext &context, const string &key, const string &value) {
	GetDuckPGQState(context)->RecordPathFindingStrategy(key, value);
}

const char *OrientationName(PathFindingSearchOrientation orientation) {
	return orientation == PathFindingSearchOrientation::REVERSE ? "reverse" : "forward";
}

void MarkTransientGlobalCSRForDeletion(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	if (!gstate.edge_input && !gstate.cached_partitioned_csr_input) {
		GetDuckPGQState(context)->csr_to_delete.insert(gstate.csr_id);
	}
}

} // namespace

//! Single-source BFS over the partitioned CSR for one group of pairs that share a source.
//!
//! The frontier is a list of vertex ids. Small frontiers are expanded vertex by vertex, looking each vertex up in
//! every partition (binary search in the sparse rows), so an iteration costs O(frontier + edges) instead of a sweep
//! over all vertices; large frontiers switch to the partition sweep, which streams the CSR once. Discovery is a
//! compare-and-swap on `seen`, so there is no separate check phase and the next frontier is collected per worker.
class SourceGroupedIterativeLengthState {
public:
	SourceGroupedIterativeLengthState(ClientContext &context_p, int64_t source_p,
	                                  std::vector<shared_ptr<LocalCSR>> &local_csrs_p, idx_t num_threads_p,
	                                  int64_t v_size_p)
	    : context(context_p), source(source_p), local_csrs(local_csrs_p), num_threads(num_threads_p), v_size(v_size_p),
	      distances(v_size, -1), seen(v_size), frontier(v_size), next_lists(num_threads_p), iter(1), change(false),
	      tasks_scheduled(0) {
		partition_counter = 0;
		local_csr_counter = 0;
		frontier_cursor = 0;
		benchmark_enabled = GetPathFindingBenchmarkOption(context);
		benchmark_output_prefix = GetPathFindingBenchmarkPrefix(context);
	}

	void Initialize() {
		if (source < 0 || source >= v_size) {
			return;
		}
		CheckRowLookupAvailable();
		seen[source].store(1, std::memory_order_relaxed);
		distances[source] = 0;
		frontier_list.push_back(NumericCast<uint32_t>(source));
		if (!targets.empty() && targets.count(NumericCast<uint32_t>(source))) {
			remaining_targets.fetch_sub(1, std::memory_order_relaxed);
		}
		change = !Exhausted();
	}

	int64_t Distance(int64_t target) const {
		if (target < 0 || target >= v_size) {
			return -1;
		}
		return distances[target];
	}

	//! A frontier this small is cheaper to expand by vertex lookups than by a sweep over the partitions.
	//! Partitions assembled from streaming segments have no row lookup, so they are always swept.
	bool UseSparseExpansion() const {
		if (!row_lookup_available) {
			return false;
		}
		return frontier_list.size() * SPARSE_FRONTIER_DIVISOR < static_cast<idx_t>(v_size);
	}

	void CheckRowLookupAvailable() {
		row_lookup_available = true;
		for (auto &local_csr : local_csrs) {
			if (local_csr && !local_csr->segments.empty()) {
				row_lookup_available = false;
			}
		}
		for (auto &delta : delta_csrs) {
			if (delta && !delta->segments.empty()) {
				row_lookup_available = false;
			}
		}
	}
	bool row_lookup_available = true;

	static constexpr idx_t SPARSE_FRONTIER_DIVISOR = 64;
	static constexpr idx_t FRONTIER_SLICE = 256;

	ClientContext &context;
	int64_t source;
	std::vector<shared_ptr<LocalCSR>> local_csrs;
	//! Index snapshot overlay aligned with local_csrs (empty for a plain CSR), see BFSState
	std::vector<shared_ptr<LocalCSR>> delta_csrs;
	std::vector<shared_ptr<vector<uint64_t>>> suppressed;
	idx_t num_threads;
	int64_t v_size;
	vector<int64_t> distances;
	//! Visited flags; set with compare-and-swap at discovery
	vector<atomic<uint8_t>> seen;
	//! Membership bytes of the current frontier, only maintained while the dense sweep is used
	vector<atomic<uint8_t>> frontier;
	vector<uint32_t> frontier_list;
	vector<vector<uint32_t>> next_lists;
	bool sparse_expansion = true;
	//! Stop after this many iterations (0 = unbounded) or once every destination of the group has been reached
	idx_t max_depth = 0;
	std::unordered_set<uint32_t> targets;
	atomic<int64_t> remaining_targets {0};

	//! Registers the destinations of this group so the search can stop when all of them are found
	void AddTarget(int64_t target) {
		if (target < 0 || target >= v_size) {
			return;
		}
		if (targets.insert(NumericCast<uint32_t>(target)).second) {
			remaining_targets.fetch_add(1, std::memory_order_relaxed);
		}
	}

	bool Exhausted() const {
		return (max_depth > 0 && static_cast<idx_t>(iter) > max_depth) ||
		       (!targets.empty() && remaining_targets.load(std::memory_order_relaxed) <= 0);
	}
	atomic<int64_t> partition_counter;
	atomic<int64_t> local_csr_counter;
	atomic<idx_t> frontier_cursor;
	unique_ptr<Barrier> barrier;
	int64_t iter;
	bool change;
	idx_t tasks_scheduled;
	bool benchmark_enabled;
	string benchmark_output_prefix;
	std::chrono::steady_clock::time_point phase_start_time;
	vector<shared_ptr<DataChunk>> output_chunks;
};

namespace {

class SourceGroupedIterativeLengthTask : public ExecutorTask {
public:
	SourceGroupedIterativeLengthTask(shared_ptr<Event> event_p, ClientContext &context,
	                                 shared_ptr<SourceGroupedIterativeLengthState> state_p, idx_t worker_id_p,
	                                 const PhysicalOperator &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), state(std::move(state_p)), worker_id(worker_id_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		BarrierAbortOnUnwind abort_barrier(*state->barrier);
		Execute();
		abort_barrier.Disarm();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	//! Discovers `target` at the current distance if no other worker did; the first to flip `seen` owns it
	inline void Relax(idx_t target) {
		if (state->seen[target].load(std::memory_order_relaxed)) {
			return;
		}
		uint8_t expected = 0;
		if (state->seen[target].compare_exchange_strong(expected, 1, std::memory_order_relaxed)) {
			state->distances[target] = state->iter;
			state->next_lists[worker_id].push_back(NumericCast<uint32_t>(target));
			if (!state->targets.empty() && state->targets.count(NumericCast<uint32_t>(target))) {
				state->remaining_targets.fetch_sub(1, std::memory_order_relaxed);
			}
		}
	}

	inline static bool IsSuppressed(const uint64_t *suppressed, idx_t offset) {
		return suppressed && ((suppressed[offset >> 6] >> (offset & 63)) & 1);
	}

	//! Relaxes the edges of one row of a partition
	inline void RelaxRow(const LocalCSR &local_csr, uint32_t start_edges, uint32_t end_edges,
	                     const uint64_t *suppressed) {
		for (auto offset = start_edges; offset < end_edges; offset++) {
			if (IsSuppressed(suppressed, offset)) {
				continue;
			}
			Relax(local_csr.e[offset] + local_csr.start_vertex);
		}
	}

	const uint64_t *SuppressedOf(idx_t partition_idx) const {
		return partition_idx < state->suppressed.size() && state->suppressed[partition_idx]
		           ? state->suppressed[partition_idx]->data()
		           : nullptr;
	}

	//! Sparse expansion: one frontier vertex, looked up in every partition and its delta companion
	void ExpandVertex(idx_t vertex) {
		for (idx_t partition_idx = 0; partition_idx < state->local_csrs.size(); partition_idx++) {
			auto &local_csr = *state->local_csrs[partition_idx];
			uint32_t start_edges;
			uint32_t end_edges;
			if (local_csr.GetRowEdges(vertex, start_edges, end_edges)) {
				RelaxRow(local_csr, start_edges, end_edges, SuppressedOf(partition_idx));
			}
			if (partition_idx < state->delta_csrs.size() && state->delta_csrs[partition_idx]) {
				auto &delta = *state->delta_csrs[partition_idx];
				if (delta.GetRowEdges(vertex, start_edges, end_edges)) {
					RelaxRow(delta, start_edges, end_edges, nullptr);
				}
			}
		}
	}

	//! Dense expansion: every row of one partition whose source is in the frontier
	void ExplorePartition(const LocalCSR &local_csr, const uint64_t *suppressed) {
		for (const auto &segment : local_csr.segments) {
			for (idx_t row_idx = 0; row_idx < segment.source_vertices.size(); row_idx++) {
				auto source_vertex = segment.source_vertices[row_idx];
				if (!state->frontier[source_vertex].load(std::memory_order_relaxed)) {
					continue;
				}
				for (auto offset = segment.row_offsets[row_idx]; offset < segment.row_offsets[row_idx + 1]; offset++) {
					Relax(segment.edges[offset] + local_csr.start_vertex);
				}
			}
		}
		if (!local_csr.segments.empty()) {
			return;
		}
		if (local_csr.HasSparseRows()) {
			for (idx_t row_idx = 0; row_idx < local_csr.source_vertices.size(); row_idx++) {
				auto source_vertex = local_csr.source_vertices[row_idx];
				if (!state->frontier[source_vertex].load(std::memory_order_relaxed)) {
					continue;
				}
				RelaxRow(local_csr, local_csr.row_offsets[row_idx], local_csr.row_offsets[row_idx + 1], suppressed);
			}
			return;
		}
		for (idx_t vertex = 0; vertex < local_csr.GetVertexSize(); vertex++) {
			if (!state->frontier[vertex].load(std::memory_order_relaxed)) {
				continue;
			}
			RelaxRow(local_csr, local_csr.v[vertex].load(std::memory_order_relaxed),
			         local_csr.v[vertex + 1].load(std::memory_order_relaxed), nullptr);
		}
	}

	//! Work over slices of the frontier list, shared between workers through the cursor
	template <class FUNC>
	void ForEachFrontierSlice(FUNC &&fn) {
		auto &frontier = state->frontier_list;
		while (true) {
			auto begin = state->frontier_cursor.fetch_add(SourceGroupedIterativeLengthState::FRONTIER_SLICE);
			if (begin >= frontier.size()) {
				break;
			}
			auto end = std::min<idx_t>(frontier.size(), begin + SourceGroupedIterativeLengthState::FRONTIER_SLICE);
			for (idx_t i = begin; i < end; i++) {
				fn(frontier[i]);
			}
		}
	}

	void Execute() {
		auto &barrier = state->barrier;
		if (worker_id == 0) {
			state->Initialize();
		}
		barrier->Wait(worker_id);

		while (state->change) {
			// Worker 0 picks the expansion mode for this iteration and resets the shared cursors
			if (worker_id == 0) {
				state->sparse_expansion = state->UseSparseExpansion();
				state->frontier_cursor = 0;
				state->local_csr_counter = 0;
			}
			barrier->Wait(worker_id);

			if (state->sparse_expansion) {
				ForEachFrontierSlice([&](uint32_t vertex) { ExpandVertex(vertex); });
			} else {
				// Mark the frontier, sweep the partitions, unmark; all O(frontier) except the sweep itself
				ForEachFrontierSlice(
				    [&](uint32_t vertex) { state->frontier[vertex].store(1, std::memory_order_relaxed); });
				barrier->Wait(worker_id);
				while (true) {
					auto partition_idx = state->local_csr_counter.fetch_add(1);
					if (partition_idx >= static_cast<int64_t>(state->local_csrs.size())) {
						break;
					}
					ExplorePartition(*state->local_csrs[partition_idx], SuppressedOf(partition_idx));
					if (partition_idx < static_cast<int64_t>(state->delta_csrs.size()) &&
					    state->delta_csrs[partition_idx]) {
						ExplorePartition(*state->delta_csrs[partition_idx], nullptr);
					}
				}
				barrier->Wait(worker_id);
				if (worker_id == 0) {
					state->frontier_cursor = 0;
				}
				barrier->Wait(worker_id);
				ForEachFrontierSlice(
				    [&](uint32_t vertex) { state->frontier[vertex].store(0, std::memory_order_relaxed); });
			}
			barrier->Wait(worker_id);

			// The next frontier is the union of what every worker discovered
			if (worker_id == 0) {
				state->frontier_list.clear();
				for (auto &next : state->next_lists) {
					state->frontier_list.insert(state->frontier_list.end(), next.begin(), next.end());
					next.clear();
				}
				state->iter++;
				state->change = !state->frontier_list.empty() && !state->Exhausted();
			}
			barrier->Wait(worker_id);
		}
	}

	shared_ptr<SourceGroupedIterativeLengthState> state;
	idx_t worker_id;
};

class SourceGroupedIterativeLengthEvent : public BasePipelineEvent {
public:
	SourceGroupedIterativeLengthEvent(shared_ptr<SourceGroupedIterativeLengthState> state_p, Pipeline &pipeline_p,
	                                  const PhysicalPathFinding &op_p)
	    : BasePipelineEvent(pipeline_p), state(std::move(state_p)), op(op_p) {
	}

	void Schedule() override {
		state->phase_start_time = std::chrono::steady_clock::now();
		auto &context = pipeline->GetClientContext();
		vector<shared_ptr<Task>> tasks;
		auto worker_count = ChooseWorkerCount(context);
		for (idx_t worker = 0; worker < worker_count; worker++) {
			tasks.push_back(
			    make_uniq<SourceGroupedIterativeLengthTask>(shared_from_this(), context, state, worker, op));
			state->tasks_scheduled++;
		}
		state->barrier = MakeQueryBarrier(state->tasks_scheduled, context);
		SetTasks(std::move(tasks));
	}

	//! Workers for one source: never more than the configured per-batch limit (the eager CSR build relies on a
	//! single worker to avoid starving the outer query) or than there are partitions, and roughly one per
	//! EDGES_PER_WORKER edges so small graphs do not pay for barrier rounds across idle threads
	idx_t ChooseWorkerCount(ClientContext &context) const {
		static constexpr idx_t EDGES_PER_WORKER = 1 << 16;
		idx_t workers = std::max<idx_t>(1, state->num_threads);
		auto configured = GetPathFindingThreadsPerBatch(context);
		if (configured > 0) {
			workers = std::min<idx_t>(workers, static_cast<idx_t>(configured));
		}
		workers = std::min<idx_t>(workers, std::max<idx_t>(1, state->local_csrs.size()));
		idx_t edge_count = 0;
		for (auto &local_csr : state->local_csrs) {
			edge_count += local_csr->GetEdgeSize();
		}
		return std::max<idx_t>(1, std::min<idx_t>(workers, (edge_count + EDGES_PER_WORKER - 1) / EDGES_PER_WORKER));
	}

	void FinishEvent() override {
		if (!state->benchmark_enabled) {
			return;
		}
		auto phase_end_time = std::chrono::steady_clock::now();
		auto time_ms = std::chrono::duration<double, std::milli>(phase_end_time - state->phase_start_time).count();
		idx_t output_count = 0;
		for (auto &chunk : state->output_chunks) {
			output_count += chunk->size();
		}
		// Report the workers that actually ran: small graphs have fewer partitions than configured threads
		AppendOperatorPhaseTiming(state->context, "source_group_bfs", state->tasks_scheduled, output_count,
		                          state->v_size - 2, state->local_csrs.size(), time_ms, 0);
	}

private:
	shared_ptr<SourceGroupedIterativeLengthState> state;
	const PhysicalPathFinding &op;
};

struct PairKey {
	bool src_valid;
	bool dst_valid;
	int64_t src;
	int64_t dst;

	bool operator==(const PairKey &other) const {
		return src_valid == other.src_valid && dst_valid == other.dst_valid && src == other.src && dst == other.dst;
	}
};

struct PairKeyHash {
	size_t operator()(const PairKey &key) const {
		auto result = std::hash<int64_t> {}(key.src);
		result ^= std::hash<int64_t> {}(key.dst) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<bool> {}(key.src_valid) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		result ^= std::hash<bool> {}(key.dst_valid) + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
		return result;
	}
};

PairKey GetPairKey(const UnifiedVectorFormat &src_format, const UnifiedVectorFormat &dst_format,
                   const int64_t *src_data, const int64_t *dst_data, idx_t row) {
	auto src_idx = src_format.sel->get_index(row);
	auto dst_idx = dst_format.sel->get_index(row);
	bool src_valid = src_format.validity.RowIsValid(src_idx);
	bool dst_valid = dst_format.validity.RowIsValid(dst_idx);
	return {src_valid, dst_valid, src_valid ? src_data[src_idx] : 0, dst_valid ? dst_data[dst_idx] : 0};
}

shared_ptr<PathFindingBatch> CreatePathFindingBatch(const shared_ptr<DataChunk> &output_pairs, idx_t output_index) {
	return make_shared_ptr<PathFindingBatch>(output_pairs, output_pairs, output_index);
}

shared_ptr<PathFindingBatch> CreatePathFindingBatch(const shared_ptr<DataChunk> &output_pairs,
                                                    const shared_ptr<DataChunk> &search_pairs, idx_t output_index) {
	return make_shared_ptr<PathFindingBatch>(output_pairs, search_pairs, output_index);
}

shared_ptr<DataChunk> CreateReversedSearchPairs(ClientContext &context, DataChunk &output_pairs) {
	auto search_pairs = make_shared_ptr<DataChunk>();
	search_pairs->Initialize(context, output_pairs.GetTypes());
	for (idx_t row = 0; row < output_pairs.size(); row++) {
		search_pairs->data[0].SetValue(row, output_pairs.GetValue(1, row));
		search_pairs->data[1].SetValue(row, output_pairs.GetValue(0, row));
	}
	search_pairs->SetChildCardinality(output_pairs.size());
	return search_pairs;
}

bool ShouldUseReverseSearch(const PathFindingPairStats &stats, PathFindingOperatorMode mode, ClientContext &context) {
	auto ratio = GetPathFindingReverseOrientationRatio(context);
	if (mode != PathFindingOperatorMode::ITERATIVE_LENGTH || ratio <= 0 || stats.distinct_dst_count == 0) {
		return false;
	}
	return static_cast<double>(stats.distinct_src_count) >=
	       static_cast<double>(ratio) * static_cast<double>(stats.distinct_dst_count);
}

PathFindingSearchOrientation ChooseSearchOrientation(const PathFindingPairStats &stats, PathFindingOperatorMode mode,
                                                     ClientContext &context) {
	return ShouldUseReverseSearch(stats, mode, context) ? PathFindingSearchOrientation::REVERSE
	                                                    : PathFindingSearchOrientation::FORWARD;
}

//! Only the legacy CSR inputs build reverse partitions; endpoint inputs and cached indexes are forward-only, so a
//! reverse search would have nothing to traverse and the forward orientation is used instead
static bool CanSearchInReverse(const PathFindingGlobalSinkState &gstate) {
	return !(gstate.precounted_edge_input || gstate.buffered_edge_input || gstate.cached_partitioned_csr_input);
}

static PathFindingSearchOrientation ChooseSupportedSearchOrientation(const PathFindingGlobalSinkState &gstate,
                                                                     ClientContext &context) {
	if (!CanSearchInReverse(gstate)) {
		return PathFindingSearchOrientation::FORWARD;
	}
	return ChooseSearchOrientation(gstate.pair_stats, gstate.path_finding_mode, context);
}

//! Maps the pair keys of an index-backed query to the epoch's dense vertex ids; unknown keys become NULL (unreachable)
//! Maps the pair keys of an index-backed query to the epoch's dense vertex ids in place; unknown keys become NULL
//! (unreachable). The hidden pair columns are never returned to the user, so overwriting them is safe.
void TranslatePairKeysInPlace(const CSRIndexSnapshot &snapshot, DataChunk &pairs) {
	for (idx_t column_idx = 0; column_idx < 2; column_idx++) {
		auto &column = pairs.data[column_idx];
		column.Flatten(pairs.size());
		auto values = FlatVector::GetDataMutable<int64_t>(column);
		auto &validity = FlatVector::ValidityMutable(column);
		for (idx_t row = 0; row < pairs.size(); row++) {
			if (!validity.RowIsValid(row)) {
				continue;
			}
			auto id = snapshot.Lookup(values[row]);
			if (id == DConstants::INVALID_INDEX) {
				validity.SetInvalid(row);
			} else {
				values[row] = NumericCast<int64_t>(id);
			}
		}
	}
}

shared_ptr<PathFindingBatch> CreateOrientedPathFindingBatch(ClientContext &context,
                                                            PathFindingSearchOrientation orientation,
                                                            const shared_ptr<DataChunk> &output_pairs,
                                                            const shared_ptr<DataChunk> &search_pairs,
                                                            idx_t output_index) {
	if (orientation == PathFindingSearchOrientation::REVERSE) {
		return CreatePathFindingBatch(output_pairs, CreateReversedSearchPairs(context, *search_pairs), output_index);
	}
	return CreatePathFindingBatch(output_pairs, search_pairs, output_index);
}

shared_ptr<PathFindingBatch> CreateOrientedPathFindingBatch(ClientContext &context,
                                                            PathFindingSearchOrientation orientation,
                                                            const shared_ptr<DataChunk> &output_pairs,
                                                            idx_t output_index) {
	if (orientation == PathFindingSearchOrientation::REVERSE) {
		return CreatePathFindingBatch(output_pairs, CreateReversedSearchPairs(context, *output_pairs), output_index);
	}
	return CreatePathFindingBatch(output_pairs, output_index);
}

void AccumulatePairStats(DataChunk &chunk, PathFindingPairStats &stats, HyperLogLog &distinct_srcs,
                         HyperLogLog &distinct_dsts) {
	UnifiedVectorFormat src_format;
	UnifiedVectorFormat dst_format;
	chunk.data[0].ToUnifiedFormat(src_format);
	chunk.data[1].ToUnifiedFormat(dst_format);
	auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
	auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);

	for (idx_t row = 0; row < chunk.size(); row++) {
		auto src_idx = src_format.sel->get_index(row);
		auto dst_idx = dst_format.sel->get_index(row);
		bool src_valid = src_format.validity.RowIsValid(src_idx);
		bool dst_valid = dst_format.validity.RowIsValid(dst_idx);
		stats.pair_count++;
		if (!src_valid || !dst_valid) {
			stats.null_pair_count++;
			continue;
		}
		auto src = src_data[src_idx];
		auto dst = dst_data[dst_idx];
		if (!stats.src_value_initialized) {
			stats.src_value_initialized = true;
			stats.single_src = src;
		} else if (stats.single_src != src) {
			stats.all_src_equal = false;
		}
		if (!stats.dst_value_initialized) {
			stats.dst_value_initialized = true;
			stats.single_dst = dst;
		} else if (stats.single_dst != dst) {
			stats.all_dst_equal = false;
		}
		distinct_srcs.InsertElement(Hash(src));
		distinct_dsts.InsertElement(Hash(dst));
		if (src == dst) {
			stats.self_pair_count++;
		}
	}
}

void FinalizePairStats(PathFindingGlobalSinkState &gstate, PathFindingPairStats stats, const HyperLogLog &distinct_srcs,
                       const HyperLogLog &distinct_dsts) {
	stats.distinct_src_count = distinct_srcs.Count();
	stats.distinct_dst_count = distinct_dsts.Count();
	gstate.pair_stats = stats;
}

PathFindingOperatorMode ParsePathFindingOperatorMode(const string &mode) {
	if (mode == "iterativelength") {
		return PathFindingOperatorMode::ITERATIVE_LENGTH;
	}
	if (mode == "pushpulliterativelength") {
		return PathFindingOperatorMode::PUSH_PULL_ITERATIVE_LENGTH;
	}
	if (mode == "bidirectionaliterativelength") {
		return PathFindingOperatorMode::BIDIRECTIONAL_ITERATIVE_LENGTH;
	}
	if (mode == "shortestpath") {
		return PathFindingOperatorMode::SHORTEST_PATH;
	}
	throw InvalidInputException("Unknown mode specified %s", mode);
}

void ConfigureLocalCSRStateForMode(LocalCSRState &local_csr_state, PathFindingOperatorMode mode) {
	switch (mode) {
	case PathFindingOperatorMode::BIDIRECTIONAL_ITERATIVE_LENGTH:
		local_csr_state.build_reverse_csr = true;
		local_csr_state.finalize_sparse_rows = false;
		break;
	case PathFindingOperatorMode::PUSH_PULL_ITERATIVE_LENGTH:
		local_csr_state.build_pull_csr = true;
		break;
	default:
		break;
	}
}

//! Uses the CSR index snapshot when the query has one; otherwise the CSR is built for this query
bool TryLoadPartitionedCSR(PathFindingGlobalSinkState &gstate, LocalCSRState &local_csr_state, ClientContext &context) {
	if (gstate.index_snapshot) {
		local_csr_state.partition_csrs = gstate.index_snapshot->partitions;
		local_csr_state.partition_delta_csrs = gstate.index_snapshot->delta_partitions;
		local_csr_state.partition_suppressed = gstate.index_snapshot->suppressed;
		local_csr_state.loaded_from_cache = true;
		RecordStrategy(context, "csr", "index");
		return true;
	}
	local_csr_state.build_timing_started = true;
	local_csr_state.build_start_time = std::chrono::steady_clock::now();
	RecordStrategy(context, "csr", "built");
	return false;
}

//! Records how long this query spent building its CSR, once the build events have finished
void RecordPartitionedCSRBuild(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	auto &local_csr_state = *gstate.local_csr_state;
	if (local_csr_state.loaded_from_cache || !local_csr_state.build_timing_started) {
		return;
	}
	local_csr_state.build_timing_started = false;
	auto build_end_time = std::chrono::steady_clock::now();
	auto build_ms =
	    std::chrono::duration<double, std::milli>(build_end_time - local_csr_state.build_start_time).count();
	AppendOperatorPhaseTiming(context, "partitioned_csr_build", gstate.num_threads, gstate.pair_stats.pair_count,
	                          GetPathFindingEdgeCount(gstate), local_csr_state.partition_csrs.size(), build_ms, 0);
}

shared_ptr<BFSState> CreateBFSStateForMode(PathFindingOperatorMode mode, const shared_ptr<PathFindingBatch> &batch,
                                           PathFindingSearchOrientation orientation, LocalCSRState &local_csr_state,
                                           idx_t num_threads, ClientContext &context, int64_t vsize) {
	switch (mode) {
	case PathFindingOperatorMode::ITERATIVE_LENGTH:
		if (orientation == PathFindingSearchOrientation::REVERSE) {
			return make_shared_ptr<IterativeLengthState>(batch, local_csr_state.reverse_partition_csrs, num_threads,
			                                             context, vsize);
		}
		return make_shared_ptr<IterativeLengthState>(batch, local_csr_state.partition_csrs, num_threads, context,
		                                             vsize);
	case PathFindingOperatorMode::PUSH_PULL_ITERATIVE_LENGTH:
		return make_shared_ptr<PushPullIterativeLengthState>(
		    batch, local_csr_state.partition_csrs, local_csr_state.pull_partition_csrs, num_threads, context, vsize);
	case PathFindingOperatorMode::BIDIRECTIONAL_ITERATIVE_LENGTH:
		return make_shared_ptr<BidirectionalIterativeLengthState>(
		    batch, local_csr_state.partition_csrs, local_csr_state.reverse_partition_csrs, num_threads, context, vsize);
	case PathFindingOperatorMode::SHORTEST_PATH:
		// TODO(dtenwolde) implement also for shortest path
		throw NotImplementedException("Shortest path operator has not been implemented yet.");
	}
	throw InternalException("Unhandled path-finding operator mode");
}

shared_ptr<Event> CreateBFSEventForMode(PathFindingGlobalSinkState &gstate, const shared_ptr<PathFindingBatch> &batch,
                                        Pipeline &pipeline, const PhysicalPathFinding *op, ClientContext &context) {
	auto bfs_state =
	    CreateBFSStateForMode(gstate.path_finding_mode, batch, gstate.search_orientation, *gstate.local_csr_state,
	                          gstate.num_threads, context, GetPathFindingVSize(gstate));
	bfs_state->delta_csrs = gstate.local_csr_state->partition_delta_csrs;
	bfs_state->suppressed = gstate.local_csr_state->partition_suppressed;
	bfs_state->max_depth = gstate.max_depth;
	auto bfs_event = bfs_state->CreateBFSEvent(pipeline, op);
	gstate.bfs_states.push_back(std::move(bfs_state));
	return bfs_event;
}

//! Finishes once every BFS batch event it depends on has finished. It takes over the parents of the scheduling
//! event, so the pipeline-complete event waits for all batches rather than for the first one only.
class PathFindingBatchesCompleteEvent : public BasePipelineEvent {
public:
	explicit PathFindingBatchesCompleteEvent(Pipeline &pipeline_p) : BasePipelineEvent(pipeline_p) {
	}

	void Schedule() override {
	}
};

//! Base for events whose FinishEvent schedules the BFS batch events. Event::InsertEvent moves the parents of this
//! event into the inserted event, so calling it once per batch would detach every batch after the first from the
//! pipeline-complete event. Instead a single completion event is inserted and made dependent on all batch events.
class PathFindingBatchSchedulerEvent : public BasePipelineEvent {
public:
	explicit PathFindingBatchSchedulerEvent(Pipeline &pipeline_p) : BasePipelineEvent(pipeline_p) {
	}

	void Schedule() override {
	}

protected:
	//! Must be called from FinishEvent: the batch events run after this event, the completion event after them.
	void ScheduleBatchEvents(vector<shared_ptr<Event>> batch_events) {
		auto complete_event = make_shared_ptr<PathFindingBatchesCompleteEvent>(*pipeline);
		for (auto &batch_event : batch_events) {
			complete_event->AddDependency(*batch_event);
		}
		InsertEvent(complete_event);
		for (auto &batch_event : batch_events) {
			batch_event->AddDependency(*this);
			executor.AddEvent(std::move(batch_event));
		}
	}
};

idx_t GetGroupedWorkersPerBatch(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	auto total_threads = std::max<idx_t>(1, gstate.num_threads);
	auto configured_workers = GetPathFindingThreadsPerBatch(context);
	auto workers_per_batch = configured_workers <= 0
	                             ? total_threads
	                             : std::min<idx_t>(static_cast<idx_t>(configured_workers), total_threads);
	auto &partition_csrs = gstate.search_orientation == PathFindingSearchOrientation::REVERSE
	                           ? gstate.local_csr_state->reverse_partition_csrs
	                           : gstate.local_csr_state->partition_csrs;
	auto partition_count = std::max<idx_t>(1, partition_csrs.size());
	return std::max<idx_t>(1, std::min(workers_per_batch, partition_count));
}

idx_t GetGroupedBatchCount(PathFindingGlobalSinkState &gstate, ClientContext &context, idx_t workers_per_batch,
                           idx_t batch_count) {
	auto total_threads = std::max<idx_t>(1, gstate.num_threads);
	auto max_groups_from_threads = std::max<idx_t>(1, total_threads / workers_per_batch);
	auto configured_groups = GetPathFindingMaxConcurrentBatches(context);
	auto group_count = configured_groups <= 0
	                       ? max_groups_from_threads
	                       : std::min<idx_t>(static_cast<idx_t>(configured_groups), max_groups_from_threads);
	return std::max<idx_t>(1, std::min(group_count, batch_count));
}

bool ShouldUseGroupedIterativeLengthBatches(PathFindingGlobalSinkState &gstate, ClientContext &context,
                                            idx_t batch_count) {
	return batch_count > 0 && gstate.path_finding_mode == PathFindingOperatorMode::ITERATIVE_LENGTH &&
	       GetPathFindingGroupedBatches(context);
}

static constexpr idx_t FEW_SOURCES_LIMIT = 16;

bool ShouldUseSourceGroupedIterativeLength(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	auto ratio = GetPathFindingSourceGroupRatio(context);
	// Pairs with a NULL endpoint (a key absent from the graph) are answered NULL without joining a group
	if (gstate.path_finding_mode != PathFindingOperatorMode::ITERATIVE_LENGTH || ratio <= 0) {
		return false;
	}
	auto distinct_search_sources = gstate.search_orientation == PathFindingSearchOrientation::REVERSE
	                                   ? gstate.pair_stats.distinct_dst_count
	                                   : gstate.pair_stats.distinct_src_count;
	if (distinct_search_sources == 0) {
		return false;
	}
	// A handful of sources is always cheaper to search one at a time with the frontier kernel than with 512-lane
	// bitsets over every vertex
	if (distinct_search_sources <= FEW_SOURCES_LIMIT) {
		return true;
	}
	return static_cast<double>(gstate.pair_stats.pair_count) >=
	       static_cast<double>(ratio) * static_cast<double>(distinct_search_sources);
}

bool TryGetExactSingleSearchSource(const PathFindingGlobalSinkState &gstate, int64_t &source) {
	if (gstate.pair_stats.pair_count == 0 || gstate.pair_stats.null_pair_count != 0) {
		return false;
	}
	if (gstate.search_orientation == PathFindingSearchOrientation::REVERSE) {
		if (!gstate.pair_stats.dst_value_initialized || !gstate.pair_stats.all_dst_equal) {
			return false;
		}
		source = gstate.pair_stats.single_dst;
		return true;
	}
	if (!gstate.pair_stats.src_value_initialized || !gstate.pair_stats.all_src_equal) {
		return false;
	}
	source = gstate.pair_stats.single_src;
	return true;
}

//! Returns the batch events to register, or an empty vector when a single grouped event was inserted directly.
vector<shared_ptr<Event>> CreatePathFindingBatchEvents(PathFindingGlobalSinkState &gstate,
                                                       vector<shared_ptr<PathFindingBatch>> &batches,
                                                       Pipeline &pipeline, Event &event, const PhysicalPathFinding &op,
                                                       ClientContext &context) {
	vector<shared_ptr<Event>> batch_events;
	RecordStrategy(context, "batches", std::to_string(batches.size()));
	if (!ShouldUseGroupedIterativeLengthBatches(gstate, context, batches.size())) {
		RecordStrategy(context, "scheduling", "per_batch");
		batch_events.reserve(batches.size());
		for (auto &batch : batches) {
			batch_events.push_back(CreateBFSEventForMode(gstate, batch, pipeline, &op, context));
		}
		return batch_events;
	}

	auto workers_per_batch = GetGroupedWorkersPerBatch(gstate, context);
	auto group_count = GetGroupedBatchCount(gstate, context, workers_per_batch, batches.size());
	RecordStrategy(context, "scheduling", "grouped");
	RecordStrategy(context, "groups", std::to_string(group_count));
	RecordStrategy(context, "workers_per_batch", std::to_string(workers_per_batch));
	vector<shared_ptr<IterativeLengthState>> iterative_states;
	iterative_states.reserve(batches.size());

	for (auto &batch : batches) {
		auto &partition_csrs = gstate.search_orientation == PathFindingSearchOrientation::REVERSE
		                           ? gstate.local_csr_state->reverse_partition_csrs
		                           : gstate.local_csr_state->partition_csrs;
		auto state = make_shared_ptr<IterativeLengthState>(batch, partition_csrs, workers_per_batch, context,
		                                                   GetPathFindingVSize(gstate));
		state->delta_csrs = gstate.local_csr_state->partition_delta_csrs;
		state->suppressed = gstate.local_csr_state->partition_suppressed;
		state->max_depth = gstate.max_depth;
		iterative_states.push_back(state);
		gstate.bfs_states.push_back(state);
	}

	event.InsertEvent(make_shared_ptr<GroupedIterativeLengthEvent>(std::move(iterative_states), workers_per_batch,
	                                                               group_count, pipeline, op));
	return batch_events;
}

class SourceGroupedScheduleEvent : public PathFindingBatchSchedulerEvent {
public:
	SourceGroupedScheduleEvent(PathFindingGlobalSinkState &gstate_p, Pipeline &pipeline_p,
	                           const PhysicalPathFinding &op_p, ClientContext &context_p)
	    : PathFindingBatchSchedulerEvent(pipeline_p), gstate(gstate_p), op(op_p), context(context_p) {
	}

	void FinishEvent() override {
		RecordPartitionedCSRBuild(gstate, context);
		vector<shared_ptr<Event>> batch_events;
		batch_events.reserve(gstate.source_group_sources.size());
		auto &partition_csrs = gstate.search_orientation == PathFindingSearchOrientation::REVERSE
		                           ? gstate.local_csr_state->reverse_partition_csrs
		                           : gstate.local_csr_state->partition_csrs;
		RecordStrategy(context, "source_groups", std::to_string(gstate.source_group_sources.size()));
		for (idx_t group_idx = 0; group_idx < gstate.source_group_sources.size(); group_idx++) {
			auto state = make_shared_ptr<SourceGroupedIterativeLengthState>(
			    context, gstate.source_group_sources[group_idx], partition_csrs, gstate.num_threads,
			    GetPathFindingVSize(gstate));
			state->delta_csrs = gstate.local_csr_state->partition_delta_csrs;
			state->suppressed = gstate.local_csr_state->partition_suppressed;
			state->max_depth = gstate.max_depth;
			state->output_chunks = std::move(gstate.source_group_output_chunks[group_idx]);
			if (!gstate.source_group_preserve_output_order) {
				// Single group: every row of its chunks is a destination of this source
				for (auto &chunk : state->output_chunks) {
					RegisterTargets(*chunk, [&](idx_t) { return state.get(); });
				}
			}
			auto state_idx = gstate.source_group_states.size();
			if (!gstate.source_group_preserve_output_order) {
				for (idx_t chunk_idx = 0; chunk_idx < state->output_chunks.size(); chunk_idx++) {
					gstate.source_group_output_refs.emplace_back(state_idx, chunk_idx);
				}
			}
			gstate.source_group_states.push_back(state);
			batch_events.push_back(make_shared_ptr<SourceGroupedIterativeLengthEvent>(state, *pipeline, op));
		}
		if (gstate.source_group_preserve_output_order) {
			// Several groups: the output rows stay in input order and are mapped to their group per row
			for (idx_t chunk_idx = 0; chunk_idx < gstate.global_output_batches.size(); chunk_idx++) {
				auto &row_groups = gstate.source_group_output_states[chunk_idx];
				RegisterTargets(
				    *gstate.global_output_batches[chunk_idx], [&](idx_t row) -> SourceGroupedIterativeLengthState * {
					    auto group_idx = row_groups[row];
					    return group_idx == DConstants::INVALID_INDEX ? nullptr
					                                                  : gstate.source_group_states[group_idx].get();
				    });
				gstate.source_group_output_refs.emplace_back(DConstants::INVALID_INDEX, chunk_idx);
			}
		}
		ScheduleBatchEvents(std::move(batch_events));
	}

private:
	//! Registers the search endpoint of every row of an output chunk with the group state that searches it, so
	//! that group can stop once all of its destinations are found
	template <class GROUP_OF_ROW>
	void RegisterTargets(DataChunk &chunk, GROUP_OF_ROW group_of_row) const {
		auto &target_column = chunk.data[gstate.search_orientation == PathFindingSearchOrientation::REVERSE ? 0 : 1];
		UnifiedVectorFormat target_format;
		target_column.ToUnifiedFormat(chunk.size(), target_format);
		auto target_data = UnifiedVectorFormat::GetData<int64_t>(target_format);
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto target_idx = target_format.sel->get_index(row);
			auto group = target_format.validity.RowIsValid(target_idx) ? group_of_row(row) : nullptr;
			if (group) {
				group->AddTarget(target_data[target_idx]);
			}
		}
	}

	PathFindingGlobalSinkState &gstate;
	const PhysicalPathFinding &op;
	ClientContext &context;
};

class PathFindingScheduleEvent : public PathFindingBatchSchedulerEvent {
public:
	PathFindingScheduleEvent(vector<shared_ptr<PathFindingBatch>> batches_p, PathFindingGlobalSinkState &gstate_p,
	                         Pipeline &pipeline_p, const PhysicalPathFinding &op_p, ClientContext &context_p)
	    : PathFindingBatchSchedulerEvent(pipeline_p), batches(std::move(batches_p)), gstate(gstate_p), op(op_p),
	      context(context_p) {
	}

	void FinishEvent() override {
		RecordPartitionedCSRBuild(gstate, context);
		auto batch_events = CreatePathFindingBatchEvents(gstate, batches, *pipeline, *this, op, context);
		if (!batch_events.empty()) {
			ScheduleBatchEvents(std::move(batch_events));
		}
	}

private:
	vector<shared_ptr<PathFindingBatch>> batches;
	PathFindingGlobalSinkState &gstate;
	const PhysicalPathFinding &op;
	ClientContext &context;
};

void SchedulePartitionedEndpointBuildThenPathFinding(PathFindingGlobalSinkState &gstate,
                                                     vector<shared_ptr<PathFindingBatch>> batches, Pipeline &pipeline,
                                                     Event &event, const PhysicalPathFinding &op,
                                                     ClientContext &context);
void SchedulePartitionedEndpointBuildThenSourceGrouped(PathFindingGlobalSinkState &gstate, Pipeline &pipeline,
                                                       Event &event, const PhysicalPathFinding &op,
                                                       ClientContext &context);

void ScheduleLocalCSRBuildThenPathFinding(PathFindingGlobalSinkState &gstate,
                                          vector<shared_ptr<PathFindingBatch>> batches, Pipeline &pipeline,
                                          Event &event, const PhysicalPathFinding &op, ClientContext &context) {
	if (gstate.search_orientation == PathFindingSearchOrientation::REVERSE &&
	    (gstate.precounted_edge_input || gstate.buffered_edge_input)) {
		throw NotImplementedException(
		    "Reverse search requires a reverse PartitionCSR; direct endpoint construction currently builds only the "
		    "forward orientation");
	}
	if (gstate.cached_partitioned_csr_input && gstate.endpoint_partition_width == 0) {
		gstate.endpoint_partition_width =
		    GetBufferedPartitionedCSRWidth(gstate.vertex_count, gstate.num_threads, context);
	}
	shared_ptr<LocalCSRState> local_csr_state;
	if (gstate.cached_partitioned_csr_input || gstate.precounted_edge_input || gstate.buffered_edge_input) {
		std::vector<std::vector<LocalCSRBuildPartition>> empty_buffers;
		local_csr_state = make_shared_ptr<LocalCSRState>(context, std::move(empty_buffers), gstate.vertex_count,
		                                                 GetPathFindingEdgeCount(gstate),
		                                                 gstate.endpoint_partition_width, gstate.num_threads);
		local_csr_state->partition_csrs = gstate.endpoint_partition_csrs;
	} else if (gstate.edge_input) {
		local_csr_state =
		    make_shared_ptr<LocalCSRState>(context, std::move(gstate.endpoint_build_runs), gstate.vertex_count,
		                                   gstate.endpoint_count, gstate.endpoint_partition_width, gstate.num_threads);
	} else {
		local_csr_state = make_shared_ptr<LocalCSRState>(context, gstate.csr, gstate.num_threads);
	}
	ConfigureLocalCSRStateForMode(*local_csr_state, gstate.path_finding_mode);
	if (gstate.search_orientation == PathFindingSearchOrientation::REVERSE) {
		local_csr_state->build_forward_csr = false;
		local_csr_state->build_reverse_csr = true;
	}
	gstate.local_csr_state = local_csr_state;
	if (TryLoadPartitionedCSR(gstate, *local_csr_state, context)) {
		event.InsertEvent(make_shared_ptr<PathFindingScheduleEvent>(std::move(batches), gstate, pipeline, op, context));
		return;
	}
	if (gstate.cached_partitioned_csr_input) {
		throw ConstraintException("The cache-only path-finding form needs a CSR index; no index snapshot was found for "
		                          "key '%s'",
		                          op.cache_key);
	}
	if (gstate.buffered_edge_input) {
		SchedulePartitionedEndpointBuildThenPathFinding(gstate, std::move(batches), pipeline, event, op, context);
		return;
	}
	if (gstate.precounted_edge_input) {
		event.InsertEvent(make_shared_ptr<PathFindingScheduleEvent>(std::move(batches), gstate, pipeline, op, context));
		return;
	}

	auto local_csr_event = make_shared_ptr<LocalCSREvent>(local_csr_state, pipeline, op, context);
	event.InsertEvent(local_csr_event);
	local_csr_event->InsertEvent(
	    make_shared_ptr<PathFindingScheduleEvent>(std::move(batches), gstate, pipeline, op, context));
}

void ScheduleLocalCSRBuildThenSourceGrouped(PathFindingGlobalSinkState &gstate, Pipeline &pipeline, Event &event,
                                            const PhysicalPathFinding &op, ClientContext &context) {
	if (gstate.search_orientation == PathFindingSearchOrientation::REVERSE &&
	    (gstate.precounted_edge_input || gstate.buffered_edge_input)) {
		throw NotImplementedException(
		    "Reverse search requires a reverse PartitionCSR; direct endpoint construction currently builds only the "
		    "forward orientation");
	}
	if (gstate.cached_partitioned_csr_input && gstate.endpoint_partition_width == 0) {
		gstate.endpoint_partition_width =
		    GetBufferedPartitionedCSRWidth(gstate.vertex_count, gstate.num_threads, context);
	}
	shared_ptr<LocalCSRState> local_csr_state;
	if (gstate.cached_partitioned_csr_input || gstate.precounted_edge_input || gstate.buffered_edge_input) {
		std::vector<std::vector<LocalCSRBuildPartition>> empty_buffers;
		local_csr_state = make_shared_ptr<LocalCSRState>(context, std::move(empty_buffers), gstate.vertex_count,
		                                                 GetPathFindingEdgeCount(gstate),
		                                                 gstate.endpoint_partition_width, gstate.num_threads);
		local_csr_state->partition_csrs = gstate.endpoint_partition_csrs;
	} else if (gstate.edge_input) {
		local_csr_state =
		    make_shared_ptr<LocalCSRState>(context, std::move(gstate.endpoint_build_runs), gstate.vertex_count,
		                                   gstate.endpoint_count, gstate.endpoint_partition_width, gstate.num_threads);
	} else {
		local_csr_state = make_shared_ptr<LocalCSRState>(context, gstate.csr, gstate.num_threads);
	}
	ConfigureLocalCSRStateForMode(*local_csr_state, gstate.path_finding_mode);
	if (gstate.search_orientation == PathFindingSearchOrientation::REVERSE) {
		local_csr_state->build_forward_csr = false;
		local_csr_state->build_reverse_csr = true;
	}
	gstate.local_csr_state = local_csr_state;
	if (TryLoadPartitionedCSR(gstate, *local_csr_state, context)) {
		event.InsertEvent(make_shared_ptr<SourceGroupedScheduleEvent>(gstate, pipeline, op, context));
		return;
	}
	if (gstate.cached_partitioned_csr_input) {
		throw ConstraintException("The cache-only path-finding form needs a CSR index; no index snapshot was found for "
		                          "key '%s'",
		                          op.cache_key);
	}
	if (gstate.buffered_edge_input) {
		SchedulePartitionedEndpointBuildThenSourceGrouped(gstate, pipeline, event, op, context);
		return;
	}
	if (gstate.precounted_edge_input) {
		event.InsertEvent(make_shared_ptr<SourceGroupedScheduleEvent>(gstate, pipeline, op, context));
		return;
	}

	auto local_csr_event = make_shared_ptr<LocalCSREvent>(local_csr_state, pipeline, op, context);
	event.InsertEvent(local_csr_event);
	local_csr_event->InsertEvent(make_shared_ptr<SourceGroupedScheduleEvent>(gstate, pipeline, op, context));
}

struct EndpointBuildEntry {
	uint32_t src;
	uint16_t local_dst;
};

//! Builds one destination-range partition from its radix-partitioned endpoints: one scan into an edge array,
//! a radix sort by source in cache, and the sparse rows emitted in one sweep. No per-vertex counter array.
static idx_t BuildCSRFromEndpointPartition(PathFindingGlobalSinkState &gstate, idx_t partition_idx) {
	auto &endpoint_partitions = gstate.endpoint_partition_data->GetPartitions();
	auto &endpoint_partition = endpoint_partitions[partition_idx];
	auto start_vertex = partition_idx * gstate.endpoint_partition_width;
	auto end_vertex = std::min(start_vertex + gstate.endpoint_partition_width, gstate.vertex_count + 2);

	vector<EndpointBuildEntry> edges;
	if (endpoint_partition) {
		edges.reserve(endpoint_partition->Count());
		ColumnDataScanState scan;
		DataChunk input;
		endpoint_partition->InitializeScan(scan);
		endpoint_partition->InitializeScanChunk(scan, input);
		while (endpoint_partition->Scan(scan, input)) {
			UnifiedVectorFormat endpoint_format;
			input.data[0].ToUnifiedFormat(endpoint_format);
			auto endpoint_data = UnifiedVectorFormat::GetData<hash_t>(endpoint_format);
			for (idx_t row = 0; row < input.size(); row++) {
				auto endpoint = endpoint_data[endpoint_format.sel->get_index(row)];
				auto source = UnpackPartitionedEndpointSource(endpoint, gstate.endpoint_radix_bits);
				auto local_destination = endpoint & UINT16_MAX;
				if (static_cast<idx_t>(source) >= gstate.vertex_count ||
				    RadixPartitioning::ApplyMask(endpoint, gstate.endpoint_radix_bits) != partition_idx ||
				    local_destination >= gstate.endpoint_partition_width) {
					throw InternalException(
					    "Packed partitioned endpoint does not match its source or destination partition");
				}
				edges.push_back({source, NumericCast<uint16_t>(local_destination)});
			}
		}
	}
	auto endpoint_count = edges.size();
	SampleCSRBuildMemory(gstate);

	vector<EndpointBuildEntry> scratch;
	vector<idx_t> histogram;
	CSRRadixSortBySource(edges, scratch, histogram, CSRSourceBits(gstate.vertex_count));
	gstate.endpoint_partition_csrs[partition_idx] = CSREmitSparsePartition(
	    start_vertex, end_vertex, gstate.vertex_count, edges, [](idx_t, const EndpointBuildEntry &) {});
	endpoint_partition.reset();
	SampleCSRBuildMemory(gstate);
	return endpoint_count;
}

class PartitionedEndpointBuildTask : public ExecutorTask {
public:
	PartitionedEndpointBuildTask(shared_ptr<Event> event_p, ClientContext &context,
	                             PathFindingGlobalSinkState &gstate_p, const PhysicalPathFinding &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), gstate(gstate_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		idx_t built_count = 0;
		while (true) {
			auto build_order_idx = gstate.next_endpoint_partition.fetch_add(1, std::memory_order_relaxed);
			if (build_order_idx >= gstate.endpoint_partition_build_order.size()) {
				break;
			}
			auto partition_idx = gstate.endpoint_partition_build_order[build_order_idx];
			built_count += BuildCSRFromEndpointPartition(gstate, partition_idx);
		}
		gstate.built_endpoint_count.fetch_add(built_count, std::memory_order_relaxed);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	PathFindingGlobalSinkState &gstate;
};

class PartitionedEndpointBuildEvent : public BasePipelineEvent {
public:
	PartitionedEndpointBuildEvent(PathFindingGlobalSinkState &gstate_p, Pipeline &pipeline_p,
	                              const PhysicalPathFinding &op_p, ClientContext &context_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p), op(op_p), context(context_p) {
	}

	void Schedule() override {
		if (!gstate.endpoint_partition_data) {
			throw InternalException("Partitioned endpoint build has no DuckDB partitioned collection");
		}
		vector<shared_ptr<Task>> tasks;
		auto task_count =
		    std::min<idx_t>(std::max<idx_t>(1, gstate.num_threads), gstate.endpoint_partition_csrs.size());
		for (idx_t task_idx = 0; task_idx < task_count; task_idx++) {
			tasks.push_back(make_uniq<PartitionedEndpointBuildTask>(shared_from_this(), context, gstate, op));
		}
		SetTasks(std::move(tasks));
	}

	void FinishEvent() override {
		auto built_count = gstate.built_endpoint_count.load(std::memory_order_relaxed);
		// Edge rows with NULL keys or without a matching vertex are dropped by the endpoint join; more rows than the
		// edge table has means a vertex key is not unique
		if (built_count > gstate.expected_edge_count) {
			throw ConstraintException("Partitioned endpoint build expected at most %llu graph edges but received %llu; "
			                          "check vertex uniqueness",
			                          gstate.expected_edge_count, built_count);
		}
		gstate.endpoint_count = built_count;
		idx_t memory_bytes = 0;
		for (auto &partition : gstate.endpoint_partition_csrs) {
			if (!partition) {
				throw InternalException("Partitioned endpoint build did not create all CSR partitions");
			}
			memory_bytes += partition->source_vertices.capacity() * sizeof(uint32_t);
			memory_bytes += partition->row_offsets.capacity() * sizeof(uint32_t);
			memory_bytes += partition->e.capacity() * sizeof(uint16_t);
		}
		auto end_time = std::chrono::steady_clock::now();
		AppendOperatorPhaseTiming(
		    gstate.context_, "endpoint_partition_build", gstate.num_threads, gstate.pair_stats.pair_count,
		    gstate.endpoint_count, gstate.endpoint_partition_csrs.size(),
		    std::chrono::duration<double, std::milli>(end_time - gstate.endpoint_fill_start).count(), memory_bytes);
		AppendOperatorPhaseTiming(
		    gstate.context_, "local_csr_forward", gstate.num_threads, gstate.pair_stats.pair_count,
		    gstate.endpoint_count, gstate.endpoint_partition_csrs.size(),
		    std::chrono::duration<double, std::milli>(end_time - gstate.endpoint_build_start).count(), memory_bytes);
		gstate.local_csr_state->partition_csrs = gstate.endpoint_partition_csrs;
		gstate.endpoint_partition_data.reset();
		SampleCSRBuildMemory(gstate);
		auto buffer_peak = gstate.csr_build_buffer_peak_bytes.load(std::memory_order_relaxed);
		auto swap_peak = gstate.csr_build_swap_peak_bytes.load(std::memory_order_relaxed);
		AppendOperatorPhaseTiming(gstate.context_, "csr_build_buffer_manager_baseline", gstate.num_threads,
		                          gstate.pair_stats.pair_count, gstate.endpoint_count,
		                          gstate.endpoint_partition_csrs.size(), 0, gstate.csr_build_buffer_baseline_bytes);
		AppendOperatorPhaseTiming(gstate.context_, "csr_build_buffer_manager_peak", gstate.num_threads,
		                          gstate.pair_stats.pair_count, gstate.endpoint_count,
		                          gstate.endpoint_partition_csrs.size(), 0, buffer_peak);
		AppendOperatorPhaseTiming(
		    gstate.context_, "csr_build_buffer_manager_peak_delta", gstate.num_threads, gstate.pair_stats.pair_count,
		    gstate.endpoint_count, gstate.endpoint_partition_csrs.size(), 0,
		    buffer_peak > gstate.csr_build_buffer_baseline_bytes ? buffer_peak - gstate.csr_build_buffer_baseline_bytes
		                                                         : 0);
		AppendOperatorPhaseTiming(gstate.context_, "csr_build_buffer_manager_swap_peak", gstate.num_threads,
		                          gstate.pair_stats.pair_count, gstate.endpoint_count,
		                          gstate.endpoint_partition_csrs.size(), 0, swap_peak);
		AppendOperatorPhaseTiming(gstate.context_, "csr_build_buffer_manager_swap_baseline", gstate.num_threads,
		                          gstate.pair_stats.pair_count, gstate.endpoint_count,
		                          gstate.endpoint_partition_csrs.size(), 0, gstate.csr_build_swap_baseline_bytes);
		AppendOperatorPhaseTiming(
		    gstate.context_, "csr_build_buffer_manager_swap_peak_delta", gstate.num_threads,
		    gstate.pair_stats.pair_count, gstate.endpoint_count, gstate.endpoint_partition_csrs.size(), 0,
		    swap_peak > gstate.csr_build_swap_baseline_bytes ? swap_peak - gstate.csr_build_swap_baseline_bytes : 0);
	}

private:
	PathFindingGlobalSinkState &gstate;
	const PhysicalPathFinding &op;
	ClientContext &context;
};

void SchedulePartitionedEndpointBuildThenPathFinding(PathFindingGlobalSinkState &gstate,
                                                     vector<shared_ptr<PathFindingBatch>> batches, Pipeline &pipeline,
                                                     Event &event, const PhysicalPathFinding &op,
                                                     ClientContext &context) {
	auto build_event = make_shared_ptr<PartitionedEndpointBuildEvent>(gstate, pipeline, op, context);
	event.InsertEvent(build_event);
	build_event->InsertEvent(
	    make_shared_ptr<PathFindingScheduleEvent>(std::move(batches), gstate, pipeline, op, context));
}

void SchedulePartitionedEndpointBuildThenSourceGrouped(PathFindingGlobalSinkState &gstate, Pipeline &pipeline,
                                                       Event &event, const PhysicalPathFinding &op,
                                                       ClientContext &context) {
	auto build_event = make_shared_ptr<PartitionedEndpointBuildEvent>(gstate, pipeline, op, context);
	event.InsertEvent(build_event);
	build_event->InsertEvent(make_shared_ptr<SourceGroupedScheduleEvent>(gstate, pipeline, op, context));
}

struct SourceGroupBuildState {
	int64_t source;
};

SinkFinalizeType FinalizeSourceGroupedPathFindingPhase(PathFindingGlobalSinkState &gstate, Pipeline &pipeline,
                                                       Event &event, const PhysicalPathFinding &op,
                                                       ClientContext &context) {
	auto start_time = std::chrono::steady_clock::now();
	RecordStrategy(context, "scheduling", "source_grouped");
	int64_t single_source;
	if (TryGetExactSingleSearchSource(gstate, single_source)) {
		auto output_chunk_count = gstate.global_output_batches.size();
		gstate.source_group_sources.push_back(single_source);
		gstate.source_group_output_chunks.push_back(std::move(gstate.global_output_batches));
		gstate.source_group_zero_copy = true;
		gstate.use_source_grouping = true;
		auto end_time = std::chrono::steady_clock::now();
		auto build_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
		AppendOperatorPhaseTiming(context, "source_group_zero_copy", gstate.num_threads, gstate.pair_stats.pair_count,
		                          1, output_chunk_count, build_ms, 0);
		ScheduleLocalCSRBuildThenSourceGrouped(gstate, pipeline, event, op, context);

		++gstate.child;
		MarkTransientGlobalCSRForDeletion(gstate, context);
		return SinkFinalizeType::READY;
	}

	std::unordered_map<int64_t, idx_t> source_to_group;
	vector<SourceGroupBuildState> groups;

	for (auto &chunk : gstate.global_output_batches) {
		vector<idx_t> output_states;
		output_states.reserve(chunk->size());
		UnifiedVectorFormat src_format;
		UnifiedVectorFormat dst_format;
		chunk->data[0].ToUnifiedFormat(src_format);
		chunk->data[1].ToUnifiedFormat(dst_format);
		auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
		auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);

		for (idx_t row = 0; row < chunk->size(); row++) {
			auto src_idx = src_format.sel->get_index(row);
			auto dst_idx = dst_format.sel->get_index(row);
			if (!src_format.validity.RowIsValid(src_idx) || !dst_format.validity.RowIsValid(dst_idx)) {
				// No group searches this row; the scatter emits NULL for it
				output_states.push_back(DConstants::INVALID_INDEX);
				continue;
			}
			auto source = gstate.search_orientation == PathFindingSearchOrientation::REVERSE ? dst_data[dst_idx]
			                                                                                 : src_data[src_idx];
			auto entry = source_to_group.find(source);
			if (entry == source_to_group.end()) {
				auto group_idx = groups.size();
				source_to_group.emplace(source, group_idx);
				SourceGroupBuildState group;
				group.source = source;
				groups.push_back(std::move(group));
				entry = source_to_group.find(source);
			}
			output_states.push_back(entry->second);
		}
		gstate.source_group_output_states.push_back(std::move(output_states));
	}

	idx_t output_chunk_count = gstate.global_output_batches.size();
	for (auto &group : groups) {
		gstate.source_group_sources.push_back(group.source);
		gstate.source_group_output_chunks.emplace_back();
	}

	auto end_time = std::chrono::steady_clock::now();
	auto build_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
	AppendOperatorPhaseTiming(context, "source_group_build", gstate.num_threads, gstate.pair_stats.pair_count,
	                          groups.size(), output_chunk_count, build_ms, 0);

	gstate.source_group_preserve_output_order = true;
	gstate.use_source_grouping = true;
	ScheduleLocalCSRBuildThenSourceGrouped(gstate, pipeline, event, op, context);

	++gstate.child;
	MarkTransientGlobalCSRForDeletion(gstate, context);
	return SinkFinalizeType::READY;
}

SinkFinalizeType FinalizeCSRIdPhase(PathFindingGlobalSinkState &gstate) {
	++gstate.child;
	return SinkFinalizeType::READY;
}

SinkFinalizeType FinalizeEndpointPairPhase(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	auto analysis_start = std::chrono::steady_clock::now();
	PathFindingPairStats pair_stats;
	HyperLogLog distinct_srcs;
	HyperLogLog distinct_dsts;
	gstate.global_pairs->InitializeScan(gstate.global_scan_state);
	while (gstate.global_scan_state.next_row_index < gstate.global_pairs->Count()) {
		DataChunk current_chunk;
		current_chunk.Initialize(context, gstate.global_pairs->Types());
		gstate.global_pairs->Scan(gstate.global_scan_state, current_chunk);
		AccumulatePairStats(current_chunk, pair_stats, distinct_srcs, distinct_dsts);
	}
	FinalizePairStats(gstate, pair_stats, distinct_srcs, distinct_dsts);
	auto analysis_end = std::chrono::steady_clock::now();
	AppendOperatorPhaseTiming(context, "pair_analysis_precompute", gstate.num_threads, gstate.pair_stats.pair_count,
	                          gstate.pair_stats.distinct_src_count, gstate.pair_stats.distinct_dst_count,
	                          std::chrono::duration<double, std::milli>(analysis_end - analysis_start).count(), 0);
	gstate.search_orientation = ChooseSearchOrientation(gstate.pair_stats, gstate.path_finding_mode, context);
	if (gstate.search_orientation == PathFindingSearchOrientation::REVERSE) {
		gstate.search_orientation = PathFindingSearchOrientation::FORWARD;
	}
	++gstate.child;
	return SinkFinalizeType::READY;
}

SinkFinalizeType FinalizeGlobalDeduplicatedPathFindingPhase(PathFindingGlobalSinkState &gstate, Pipeline &pipeline,
                                                            Event &event, const PhysicalPathFinding &op,
                                                            ClientContext &context) {
	auto start_time = std::chrono::steady_clock::now();
	auto pair_types = gstate.global_pairs->Types();
	std::unordered_map<PairKey, idx_t, PairKeyHash> unique_pair_to_row;
	unique_pair_to_row.reserve(gstate.global_pairs->Count());
	vector<std::pair<idx_t, idx_t>> unique_source_rows;
	unique_source_rows.reserve(gstate.global_pairs->Count());
	PathFindingPairStats pair_stats;
	HyperLogLog distinct_srcs;
	HyperLogLog distinct_dsts;

	bool has_duplicates = false;
	idx_t input_count = 0;
	while (gstate.global_scan_state.next_row_index < gstate.global_pairs->Count()) {
		auto current_chunk = make_shared_ptr<DataChunk>();
		current_chunk->Initialize(context, pair_types);
		gstate.global_pairs->Scan(gstate.global_scan_state, *current_chunk);
		AccumulatePairStats(*current_chunk, pair_stats, distinct_srcs, distinct_dsts);

		auto output_batch_idx = gstate.global_output_batches.size();
		vector<idx_t> output_to_search;
		output_to_search.reserve(current_chunk->size());

		UnifiedVectorFormat src_format;
		UnifiedVectorFormat dst_format;
		current_chunk->data[0].ToUnifiedFormat(src_format);
		current_chunk->data[1].ToUnifiedFormat(dst_format);
		auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
		auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);

		for (idx_t row = 0; row < current_chunk->size(); row++) {
			auto key = GetPairKey(src_format, dst_format, src_data, dst_data, row);
			auto entry = unique_pair_to_row.find(key);
			if (entry != unique_pair_to_row.end()) {
				output_to_search.push_back(entry->second);
				has_duplicates = true;
			} else {
				auto unique_row = unique_source_rows.size();
				unique_pair_to_row.emplace(key, unique_row);
				unique_source_rows.emplace_back(output_batch_idx, row);
				output_to_search.push_back(unique_row);
			}
			input_count++;
		}

		gstate.global_output_batches.push_back(current_chunk);
		gstate.global_output_to_search.push_back(std::move(output_to_search));
	}
	FinalizePairStats(gstate, pair_stats, distinct_srcs, distinct_dsts);
	gstate.search_orientation = ChooseSupportedSearchOrientation(gstate, context);

	auto duplicate_count = input_count - unique_source_rows.size();
	RecordStrategy(context, "orientation", OrientationName(gstate.search_orientation));
	RecordStrategy(context, "dedupe", "true");
	RecordStrategy(context, "duplicate_pairs", std::to_string(duplicate_count));
	idx_t remap_memory = 0;
	if (has_duplicates) {
		for (auto &mapping : gstate.global_output_to_search) {
			remap_memory += mapping.capacity() * sizeof(idx_t);
		}
	}
	auto build_end_time = std::chrono::steady_clock::now();
	auto build_ms = std::chrono::duration<double, std::milli>(build_end_time - start_time).count();
	AppendOperatorPhaseTiming(context, "dedupe_build", gstate.num_threads, input_count, unique_source_rows.size(),
	                          duplicate_count, build_ms, remap_memory);

	if (!has_duplicates) {
		vector<shared_ptr<PathFindingBatch>> batches;
		batches.reserve(gstate.global_output_batches.size());
		for (auto &output_batch : gstate.global_output_batches) {
			batches.push_back(CreateOrientedPathFindingBatch(context, gstate.search_orientation, output_batch,
			                                                 gstate.next_batch_index++));
		}
		ScheduleLocalCSRBuildThenPathFinding(gstate, std::move(batches), pipeline, event, op, context);
		gstate.global_output_batches.clear();
		gstate.global_output_to_search.clear();
		gstate.use_global_deduplication = false;
		++gstate.child;
		MarkTransientGlobalCSRForDeletion(gstate, context);
		return SinkFinalizeType::READY;
	}

	gstate.use_global_deduplication = true;
	idx_t unique_row = 0;
	vector<shared_ptr<PathFindingBatch>> batches;
	while (unique_row < unique_source_rows.size()) {
		auto search_chunk = make_shared_ptr<DataChunk>();
		search_chunk->Initialize(context, pair_types);
		idx_t chunk_count = 0;
		while (unique_row < unique_source_rows.size() && chunk_count < STANDARD_VECTOR_SIZE) {
			auto source = unique_source_rows[unique_row];
			auto &output_batch = *gstate.global_output_batches[source.first];
			if (gstate.search_orientation == PathFindingSearchOrientation::REVERSE) {
				search_chunk->data[0].SetValue(chunk_count, output_batch.GetValue(1, source.second));
				search_chunk->data[1].SetValue(chunk_count, output_batch.GetValue(0, source.second));
			} else {
				search_chunk->data[0].SetValue(chunk_count, output_batch.GetValue(0, source.second));
				search_chunk->data[1].SetValue(chunk_count, output_batch.GetValue(1, source.second));
			}
			unique_row++;
			chunk_count++;
		}
		search_chunk->SetChildCardinality(chunk_count);
		auto batch = make_shared_ptr<PathFindingBatch>(search_chunk, search_chunk, gstate.next_batch_index++);
		batches.push_back(std::move(batch));
	}
	ScheduleLocalCSRBuildThenPathFinding(gstate, std::move(batches), pipeline, event, op, context);

	++gstate.child;
	MarkTransientGlobalCSRForDeletion(gstate, context);
	return SinkFinalizeType::READY;
}

SinkFinalizeType FinalizePathFindingPhase(PathFindingGlobalSinkState &gstate, Pipeline &pipeline, Event &event,
                                          const PhysicalPathFinding &op, ClientContext &context) {
	if (gstate.global_pairs->Count() == 0) {
		return SinkFinalizeType::READY;
	}

	auto analysis_start = std::chrono::steady_clock::now();
	PathFindingPairStats pair_stats;
	HyperLogLog distinct_srcs;
	HyperLogLog distinct_dsts;
	// These chunks remain live until BFS finishes and cannot reference the scan state's current buffer handles.
	// Under memory pressure, a zero-copy scan can evict or reuse an earlier chunk's backing block, silently
	// corrupting the retained source/destination pairs.
	gstate.global_pairs->InitializeScan(gstate.global_scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
	while (gstate.global_scan_state.next_row_index < gstate.global_pairs->Count()) {
		auto current_chunk = make_shared_ptr<DataChunk>();
		current_chunk->Initialize(context, gstate.global_pairs->Types());
		gstate.global_pairs->Scan(gstate.global_scan_state, *current_chunk);
		if (gstate.index_snapshot) {
			TranslatePairKeysInPlace(*gstate.index_snapshot, *current_chunk);
		}
		AccumulatePairStats(*current_chunk, pair_stats, distinct_srcs, distinct_dsts);
		gstate.global_output_batches.push_back(current_chunk);
	}
	FinalizePairStats(gstate, pair_stats, distinct_srcs, distinct_dsts);
	auto analysis_end = std::chrono::steady_clock::now();
	AppendOperatorPhaseTiming(context, "pair_analysis", gstate.num_threads, gstate.pair_stats.pair_count,
	                          gstate.pair_stats.distinct_src_count, gstate.pair_stats.distinct_dst_count,
	                          std::chrono::duration<double, std::milli>(analysis_end - analysis_start).count(), 0);
	gstate.search_orientation = ChooseSupportedSearchOrientation(gstate, context);
	RecordStrategy(context, "mode",
	               gstate.path_finding_mode == PathFindingOperatorMode::ITERATIVE_LENGTH ? "iterativelength" : op.mode);
	RecordStrategy(context, "pairs", std::to_string(gstate.pair_stats.pair_count));
	RecordStrategy(context, "distinct_src", std::to_string(gstate.pair_stats.distinct_src_count));
	RecordStrategy(context, "distinct_dst", std::to_string(gstate.pair_stats.distinct_dst_count));
	RecordStrategy(context, "orientation", OrientationName(gstate.search_orientation));
	int64_t single_source;
	if (ShouldUseSourceGroupedIterativeLength(gstate, context) &&
	    TryGetExactSingleSearchSource(gstate, single_source)) {
		return FinalizeSourceGroupedPathFindingPhase(gstate, pipeline, event, op, context);
	}

	// The dedupe path rescans the raw pairs, which for an index-backed query are keys rather than ids
	if (GetPathFindingDeduplicatePairs(context) && !gstate.index_snapshot) {
		gstate.global_output_batches.clear();
		gstate.global_pairs->InitializeScan(gstate.global_scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
		return FinalizeGlobalDeduplicatedPathFindingPhase(gstate, pipeline, event, op, context);
	}

	if (ShouldUseSourceGroupedIterativeLength(gstate, context)) {
		return FinalizeSourceGroupedPathFindingPhase(gstate, pipeline, event, op, context);
	}

	vector<shared_ptr<PathFindingBatch>> oriented_batches;
	oriented_batches.reserve(gstate.global_output_batches.size());
	for (auto &output_batch : gstate.global_output_batches) {
		oriented_batches.push_back(CreateOrientedPathFindingBatch(context, gstate.search_orientation, output_batch,
		                                                          gstate.next_batch_index++));
	}
	ScheduleLocalCSRBuildThenPathFinding(gstate, std::move(oriented_batches), pipeline, event, op, context);

	++gstate.child;
	MarkTransientGlobalCSRForDeletion(gstate, context);
	return SinkFinalizeType::READY;
}

} // namespace

PhysicalPathFinding::PhysicalPathFinding(PhysicalPlan &physical_plan, LogicalExtensionOperator &op,
                                         PhysicalOperator &pairs, PhysicalOperator *csr, PhysicalOperator *counts)
    : PhysicalComparisonJoin(physical_plan, op, TYPE, {}, JoinType::INNER, op.estimated_cardinality) {
	children.push_back(pairs);
	if (counts) {
		children.push_back(*counts);
	}
	if (csr) {
		children.push_back(*csr);
	}
	expressions = std::move(op.expressions);
	estimated_cardinality = op.estimated_cardinality;
	auto &path_finding_op = op.Cast<LogicalPathFindingOperator>();
	mode = path_finding_op.mode;
	cache_key = path_finding_op.cache_key;
	max_depth = path_finding_op.max_depth;
	edge_input = path_finding_op.edge_input;
	precounted_edge_input = path_finding_op.precounted_edge_input;
	precounted_vertex_count = path_finding_op.precounted_vertex_count;
	precounted_edge_count = path_finding_op.precounted_edge_count;
	cached_partitioned_csr_input = path_finding_op.cached_partitioned_csr_input;
	buffered_edge_input = path_finding_op.buffered_edge_input;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
PathFindingLocalSinkState::PathFindingLocalSinkState(ClientContext &context, const PhysicalPathFinding &op)
    : local_pairs(context, op.children[0].get().GetTypes()), context(context) {
}

void PathFindingLocalSinkState::SinkPairs(DataChunk &input) {
	local_pairs.Append(input);
}

void PathFindingLocalSinkState::SinkEndpoints(DataChunk &input) {
	if (input.size() == 0) {
		return;
	}
	auto input_vertex_count = input.GetValue(2, 0).GetValue<int64_t>();
	auto input_edge_count = input.GetValue(3, 0).GetValue<int64_t>();
	if (input_vertex_count < 0 || static_cast<uint64_t>(input_vertex_count) > NumericLimits<uint32_t>::Maximum()) {
		throw OutOfRangeException("Direct endpoint CSR vertex count is outside the supported uint32 range: %lld",
		                          input_vertex_count);
	}
	if (input_edge_count < 0) {
		throw OutOfRangeException("Direct endpoint CSR edge count cannot be negative: %lld", input_edge_count);
	}
	if (endpoint_metadata_initialized && (vertex_count != static_cast<idx_t>(input_vertex_count) ||
	                                      expected_edge_count != static_cast<idx_t>(input_edge_count))) {
		throw InvalidInputException("Inconsistent direct endpoint CSR metadata");
	}
	vertex_count = static_cast<idx_t>(input_vertex_count);
	expected_edge_count = static_cast<idx_t>(input_edge_count);
	endpoint_metadata_initialized = true;
	if (endpoint_partition_width == 0) {
		auto thread_count = std::max<idx_t>(1, TaskScheduler::GetScheduler(context).NumberOfThreads());
		endpoint_partition_width = GetBufferedPartitionedCSRWidth(vertex_count, thread_count, context);
		auto partition_count = (vertex_count + 2 + endpoint_partition_width - 1) / endpoint_partition_width;
		local_endpoint_partitions.resize(partition_count);
	}

	UnifiedVectorFormat src_format;
	UnifiedVectorFormat dst_format;
	input.data[0].ToUnifiedFormat(src_format);
	input.data[1].ToUnifiedFormat(dst_format);
	auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
	auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);
	for (idx_t row = 0; row < input.size(); row++) {
		auto src_idx = src_format.sel->get_index(row);
		auto dst_idx = dst_format.sel->get_index(row);
		if (!src_format.validity.RowIsValid(src_idx) || !dst_format.validity.RowIsValid(dst_idx)) {
			throw ConstraintException("Path-finding graph endpoints cannot be NULL");
		}
		auto src = src_data[src_idx];
		auto dst = dst_data[dst_idx];
		if (src < 0 || dst < 0 || static_cast<idx_t>(src) >= vertex_count || static_cast<idx_t>(dst) >= vertex_count) {
			throw ConstraintException("Path-finding graph endpoint is outside the vertex rowid range");
		}
		auto partition_idx = static_cast<idx_t>(dst) / endpoint_partition_width;
		auto destination = static_cast<idx_t>(dst) - partition_idx * endpoint_partition_width;
		local_endpoint_partitions[partition_idx].Append(static_cast<idx_t>(src), destination);
	}
}

static void InitializePrecountedEndpointState(PathFindingGlobalSinkState &gstate) {
	lock_guard<mutex> initialize_lock(gstate.endpoint_init_lock);
	if (gstate.endpoint_counts_initialized) {
		return;
	}
	gstate.endpoint_build_start = std::chrono::steady_clock::now();
	gstate.endpoint_build_started = true;
	gstate.endpoint_partition_width =
	    GetBufferedPartitionedCSRWidth(gstate.vertex_count, gstate.num_threads, gstate.context_);
	auto partition_count =
	    (gstate.vertex_count + 2 + gstate.endpoint_partition_width - 1) / gstate.endpoint_partition_width;
	gstate.endpoint_partition_csrs.reserve(partition_count);
	for (idx_t partition_idx = 0; partition_idx < partition_count; partition_idx++) {
		auto start_vertex = partition_idx * gstate.endpoint_partition_width;
		auto end_vertex = std::min(start_vertex + gstate.endpoint_partition_width, gstate.vertex_count + 2);
		gstate.endpoint_partition_csrs.push_back(
		    make_shared_ptr<LocalCSR>(start_vertex, end_vertex, gstate.vertex_count, true));
	}
	gstate.endpoint_counts_initialized = true;
}

static void SinkPrecountEndpoints(PathFindingGlobalSinkState &gstate, PathFindingLocalSinkState &lstate,
                                  DataChunk &input) {
	if (input.size() == 0) {
		return;
	}
	if (input.data.size() != 2) {
		throw InternalException("Pre-counted endpoint scan expected src and dst columns");
	}
	InitializePrecountedEndpointState(gstate);
	UnifiedVectorFormat src_format;
	UnifiedVectorFormat dst_format;
	input.data[0].ToUnifiedFormat(src_format);
	input.data[1].ToUnifiedFormat(dst_format);
	auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
	auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);
	for (idx_t row = 0; row < input.size(); row++) {
		auto src_idx = src_format.sel->get_index(row);
		auto dst_idx = dst_format.sel->get_index(row);
		if (!src_format.validity.RowIsValid(src_idx) || !dst_format.validity.RowIsValid(dst_idx)) {
			throw ConstraintException("Path-finding graph endpoints cannot be NULL");
		}
		auto src = src_data[src_idx];
		auto dst = dst_data[dst_idx];
		if (src < 0 || dst < 0 || static_cast<idx_t>(src) >= gstate.vertex_count ||
		    static_cast<idx_t>(dst) >= gstate.vertex_count) {
			throw ConstraintException("Path-finding graph endpoint is outside the vertex rowid range");
		}
		auto partition_idx = static_cast<idx_t>(dst) / gstate.endpoint_partition_width;
		gstate.endpoint_partition_csrs[partition_idx]->v[static_cast<idx_t>(src) + 1].fetch_add(
		    1, std::memory_order_relaxed);
	}
	lstate.local_counted_endpoint_count += input.size();
}

void PathFindingLocalSinkState::SinkBufferedEndpoints(PathFindingGlobalSinkState &gstate, DataChunk &input) {
	if (input.size() == 0) {
		return;
	}
	if (input.data.size() != 2) {
		throw InternalException("Partitioned endpoint input expected src and dst columns");
	}
	if (!gstate.endpoint_build_started) {
		lock_guard<mutex> initialize_lock(gstate.endpoint_init_lock);
		if (!gstate.endpoint_build_started) {
			gstate.endpoint_build_start = std::chrono::steady_clock::now();
			gstate.endpoint_build_started = true;
		}
	}
	if (!local_endpoint_partition_data) {
		local_endpoint_partition_data = gstate.endpoint_partition_data->CreateShared();
		endpoint_partition_append_state = make_uniq<PartitionedColumnDataAppendState>();
		local_endpoint_partition_data->InitializeAppendState(*endpoint_partition_append_state);
		endpoint_partition_chunk.Initialize(context, {LogicalType::HASH});
	}

	UnifiedVectorFormat src_format;
	UnifiedVectorFormat dst_format;
	input.data[0].ToUnifiedFormat(src_format);
	input.data[1].ToUnifiedFormat(dst_format);
	auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
	auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);
	endpoint_partition_chunk.Reset();
	endpoint_partition_chunk.SetChildCardinality(input.size());
	auto output_endpoint = FlatVector::Writer<hash_t>(endpoint_partition_chunk.data[0], input.size());
	for (idx_t row = 0; row < input.size(); row++) {
		auto src_idx = src_format.sel->get_index(row);
		auto dst_idx = dst_format.sel->get_index(row);
		if (!src_format.validity.RowIsValid(src_idx) || !dst_format.validity.RowIsValid(dst_idx)) {
			throw ConstraintException("Path-finding graph endpoints cannot be NULL");
		}
		auto src = src_data[src_idx];
		auto dst = dst_data[dst_idx];
		if (src < 0 || dst < 0 || static_cast<idx_t>(src) >= gstate.vertex_count ||
		    static_cast<idx_t>(dst) >= gstate.vertex_count) {
			throw ConstraintException("Path-finding graph endpoint is outside the vertex rowid range");
		}
		auto partition_idx = static_cast<idx_t>(dst) / gstate.endpoint_partition_width;
		auto local_dst = static_cast<idx_t>(dst) - partition_idx * gstate.endpoint_partition_width;
		if (partition_idx >= gstate.endpoint_logical_partition_count || local_dst > UINT16_MAX) {
			throw InternalException("Partitioned endpoint is outside the logical destination partition range");
		}
		output_endpoint.WriteValue(PackPartitionedEndpoint(
		    NumericCast<uint32_t>(src), partition_idx, NumericCast<uint16_t>(local_dst), gstate.endpoint_radix_bits));
	}
	local_endpoint_partition_data->Append(*endpoint_partition_append_state, endpoint_partition_chunk);
	local_counted_endpoint_count += input.size();
}

static void SinkPreallocatedEndpoints(PathFindingGlobalSinkState &gstate, PathFindingLocalSinkState &lstate,
                                      DataChunk &input) {
	if (input.size() == 0) {
		return;
	}
	if (input.data.size() != 2) {
		throw InternalException("Pre-counted endpoint fill expected src and dst columns");
	}
	if (!gstate.endpoint_counts_initialized || !gstate.endpoint_counts_finalized) {
		throw InternalException("Endpoint fill started before count finalization");
	}
	UnifiedVectorFormat src_format;
	UnifiedVectorFormat dst_format;
	input.data[0].ToUnifiedFormat(src_format);
	input.data[1].ToUnifiedFormat(dst_format);
	auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
	auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);
	for (idx_t row = 0; row < input.size(); row++) {
		auto src_idx = src_format.sel->get_index(row);
		auto dst_idx = dst_format.sel->get_index(row);
		if (!src_format.validity.RowIsValid(src_idx) || !dst_format.validity.RowIsValid(dst_idx)) {
			throw ConstraintException("Path-finding graph endpoints cannot be NULL");
		}
		auto src = src_data[src_idx];
		auto dst = dst_data[dst_idx];
		if (src < 0 || dst < 0 || static_cast<idx_t>(src) >= gstate.vertex_count ||
		    static_cast<idx_t>(dst) >= gstate.vertex_count) {
			throw ConstraintException("Path-finding graph endpoint is outside the vertex rowid range");
		}
		auto partition_idx = static_cast<idx_t>(dst) / gstate.endpoint_partition_width;
		auto &partition = *gstate.endpoint_partition_csrs[partition_idx];
		auto position = partition.v[static_cast<idx_t>(src) + 1].fetch_add(1, std::memory_order_relaxed);
		if (position >= partition.e.size()) {
			throw InternalException("Pre-counted endpoint cursor exceeded its allocated partition");
		}
		partition.e[position] = NumericCast<uint16_t>(static_cast<idx_t>(dst) - partition.start_vertex);
	}
	lstate.local_filled_endpoint_count += input.size();
}

PathFindingGlobalSinkState::PathFindingGlobalSinkState(ClientContext &context, const PhysicalPathFinding &op)
    : context_(context) {
	global_pairs = make_uniq<ColumnDataCollection>(context, op.children[0].get().GetTypes());

	global_pairs->InitializeScan(global_scan_state);
	result_scan_idx = 0;
	next_batch_index = 0;
	use_global_deduplication = false;
	use_source_grouping = false;
	source_group_zero_copy = false;
	source_group_preserve_output_order = false;
	global_dedupe_results_initialized = false;
	edge_input = op.edge_input;
	precounted_edge_input = op.precounted_edge_input;
	cached_partitioned_csr_input = op.cached_partitioned_csr_input;
	buffered_edge_input = op.buffered_edge_input;
	vertex_count = op.precounted_vertex_count;
	expected_edge_count = op.precounted_edge_count;
	max_depth = op.max_depth;
	if (cached_partitioned_csr_input && CSRIndex::IsCacheKey(op.cache_key)) {
		index_snapshot = CSRIndex::ResolveCacheKey(context, op.cache_key);
		if (!index_snapshot) {
			throw InvalidInputException(
			    "The CSR index behind '%s' has no snapshot for this transaction; rerun the query", op.cache_key);
		}
		vertex_count = index_snapshot->vertex_count;
		expected_edge_count = index_snapshot->edge_count;
		endpoint_partition_width = index_snapshot->partition_width;
	}
	auto &scheduler = TaskScheduler::GetScheduler(context);
	num_threads = scheduler.NumberOfThreads();
	if (buffered_edge_input) {
		endpoint_radix_bits = GetBufferedPartitionedCSRRadixBits(vertex_count, num_threads, context);
		endpoint_partition_width = GetBufferedPartitionedCSRWidth(vertex_count, num_threads, context);
		endpoint_logical_partition_count =
		    GetBufferedPartitionedCSRLogicalPartitionCount(vertex_count, num_threads, context);
		endpoint_partition_data = make_uniq<RadixPartitionedColumnData>(
		    context, vector<LogicalType> {LogicalType::HASH}, endpoint_radix_bits, 0);
		// Ensure that an empty graph also has all destination partitions.
		auto empty_partitions = endpoint_partition_data->CreateShared();
		endpoint_partition_data->Combine(*empty_partitions);
		if (GetPathFindingBenchmarkOption(context)) {
			auto &buffer_manager = BufferManager::GetBufferManager(context);
			csr_build_buffer_baseline_bytes = buffer_manager.GetUsedMemory();
			csr_build_swap_baseline_bytes = buffer_manager.GetUsedSwap();
			csr_build_buffer_peak_bytes.store(csr_build_buffer_baseline_bytes, std::memory_order_relaxed);
			csr_build_swap_peak_bytes.store(csr_build_swap_baseline_bytes, std::memory_order_relaxed);
		}
	}
	csr = nullptr;

	child = 0;
	mode = op.mode;
	path_finding_mode = ParsePathFindingOperatorMode(mode);
	search_orientation = PathFindingSearchOrientation::FORWARD;
}

void PathFindingGlobalSinkState::Sink(DataChunk &input, PathFindingLocalSinkState &lstate) {
	if (cached_partitioned_csr_input) {
		lstate.SinkPairs(input);
		return;
	}
	if (edge_input) {
		if (child == 0) {
			lstate.SinkPairs(input);
		} else if (buffered_edge_input) {
			lstate.SinkBufferedEndpoints(*this, input);
		} else if (precounted_edge_input && child == 1) {
			SinkPrecountEndpoints(*this, lstate, input);
		} else if (precounted_edge_input) {
			SinkPreallocatedEndpoints(*this, lstate, input);
		} else {
			lstate.SinkEndpoints(input);
		}
		return;
	}
	if (child == 0) {
		// CSR phase
		auto duckpgq_state = GetDuckPGQState(context_);
		csr_id = input.GetValue(0, 0).GetValue<int64_t>();
		csr = duckpgq_state->GetCSR(csr_id);
	} else {
		// path-finding phase
		lstate.SinkPairs(input);
	}
}

unique_ptr<GlobalSinkState> PhysicalPathFinding::GetGlobalSinkState(ClientContext &context) const {
	D_ASSERT(!sink_state);
	GetDuckPGQState(context)->ResetPathFindingStrategy();
	return make_uniq<PathFindingGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalPathFinding::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PathFindingLocalSinkState>(context.client, *this);
}

SinkResultType PhysicalPathFinding::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PathFindingLocalSinkState>();
	gstate.Sink(chunk, lstate);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalPathFinding::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PathFindingLocalSinkState>();
	if (gstate.cached_partitioned_csr_input) {
		if (lstate.local_pairs.Count() > 0) {
			lock_guard<mutex> pair_lock(gstate.global_pairs_lock);
			gstate.global_pairs->Combine(lstate.local_pairs);
		}
		return SinkCombineResultType::FINISHED;
	}
	if (gstate.edge_input) {
		if (gstate.child == 0) {
			if (lstate.local_pairs.Count() > 0) {
				lock_guard<mutex> pair_lock(gstate.global_pairs_lock);
				gstate.global_pairs->Combine(lstate.local_pairs);
			}
			return SinkCombineResultType::FINISHED;
		}
		if (gstate.precounted_edge_input) {
			lock_guard<mutex> endpoint_lock(gstate.global_endpoints_lock);
			if (gstate.child == 1) {
				gstate.counted_endpoint_count += lstate.local_counted_endpoint_count;
			} else {
				gstate.endpoint_count += lstate.local_filled_endpoint_count;
			}
			return SinkCombineResultType::FINISHED;
		}
		if (gstate.buffered_edge_input) {
			lock_guard<mutex> endpoint_lock(gstate.global_endpoints_lock);
			gstate.counted_endpoint_count += lstate.local_counted_endpoint_count;
			if (lstate.local_endpoint_partition_data) {
				lstate.local_endpoint_partition_data->FlushAppendState(*lstate.endpoint_partition_append_state);
				gstate.endpoint_partition_data->Combine(*lstate.local_endpoint_partition_data);
				lstate.local_endpoint_partition_data.reset();
				lstate.endpoint_partition_append_state.reset();
			}
			SampleCSRBuildMemory(gstate);
			return SinkCombineResultType::FINISHED;
		}
		if (!lstate.endpoint_metadata_initialized) {
			return SinkCombineResultType::FINISHED;
		}
		lock_guard<mutex> endpoint_lock(gstate.global_endpoints_lock);
		if (gstate.endpoint_count > 0 &&
		    (gstate.vertex_count != lstate.vertex_count || gstate.expected_edge_count != lstate.expected_edge_count)) {
			throw InvalidInputException("Inconsistent direct endpoint CSR metadata across workers");
		}
		gstate.vertex_count = lstate.vertex_count;
		gstate.expected_edge_count = lstate.expected_edge_count;
		if (gstate.endpoint_partition_width != 0 &&
		    gstate.endpoint_partition_width != lstate.endpoint_partition_width) {
			throw InvalidInputException("Inconsistent direct streaming CSR partition width across workers");
		}
		gstate.endpoint_partition_width = lstate.endpoint_partition_width;
		idx_t local_edge_count = 0;
		for (auto &partition : lstate.local_endpoint_partitions) {
			partition.Finalize();
			local_edge_count += partition.destinations.size();
		}
		gstate.endpoint_count += local_edge_count;
		if (local_edge_count > 0) {
			gstate.endpoint_build_runs.push_back(std::move(lstate.local_endpoint_partitions));
		}
		return SinkCombineResultType::FINISHED;
	}
	if (gstate.child == 0) {
		return SinkCombineResultType::FINISHED;
	}
	if (lstate.local_pairs.Count() > 0) {
		lock_guard<mutex> pair_lock(gstate.global_pairs_lock);
		gstate.global_pairs->Combine(lstate.local_pairs);
	}
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

static SinkFinalizeType FinalizePrecountedEndpointCounts(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	InitializePrecountedEndpointState(gstate);
	if (gstate.counted_endpoint_count != gstate.expected_edge_count) {
		throw ConstraintException("Endpoint count scan expected %llu graph edges but received %llu",
		                          gstate.expected_edge_count, gstate.counted_endpoint_count);
	}

	auto start_time = std::chrono::steady_clock::now();
	AppendOperatorPhaseTiming(
	    context, "precount_scan", gstate.num_threads, gstate.pair_stats.pair_count, gstate.counted_endpoint_count,
	    gstate.endpoint_partition_csrs.size(),
	    std::chrono::duration<double, std::milli>(start_time - gstate.endpoint_build_start).count(), 0);
	for (auto &partition_ptr : gstate.endpoint_partition_csrs) {
		auto &partition = *partition_ptr;
		uint64_t running_sum = 0;
		idx_t sparse_row_count = 0;
		for (idx_t source = 0; source < partition.v_array_size; source++) {
			auto count = partition.v[source].load(std::memory_order_relaxed);
			partition.v[source].store(NumericCast<uint32_t>(running_sum), std::memory_order_relaxed);
			running_sum += count;
			sparse_row_count += count > 0;
		}
		partition.expected_sparse_row_count = sparse_row_count;
		partition.e.resize(NumericCast<idx_t>(running_sum));
		partition.initialized_e = true;
	}
	gstate.endpoint_counts_finalized = true;
	auto end_time = std::chrono::steady_clock::now();
	AppendOperatorPhaseTiming(context, "precount_allocate", gstate.num_threads, gstate.pair_stats.pair_count,
	                          gstate.counted_endpoint_count, gstate.endpoint_partition_csrs.size(),
	                          std::chrono::duration<double, std::milli>(end_time - start_time).count(), 0);
	gstate.endpoint_fill_start = end_time;
	++gstate.child;
	return SinkFinalizeType::READY;
}

static void FinalizePartitionedEndpointInput(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	if (gstate.counted_endpoint_count > gstate.expected_edge_count) {
		throw ConstraintException("Partitioned endpoint input expected at most %llu graph edges but received %llu; "
		                          "check vertex uniqueness",
		                          gstate.expected_edge_count, gstate.counted_endpoint_count);
	}
	if (!gstate.endpoint_build_started) {
		gstate.endpoint_build_start = std::chrono::steady_clock::now();
		gstate.endpoint_build_started = true;
	}
	auto partition_end = std::chrono::steady_clock::now();
	auto &endpoint_partitions = gstate.endpoint_partition_data->GetPartitions();
	if (endpoint_partitions.size() < gstate.endpoint_logical_partition_count) {
		throw InternalException("Radix endpoint input has fewer buckets than logical CSR partitions");
	}
	// ColumnDataCollection::SizeInBytes counts a shared allocator once per segment.
	// Report the logical endpoint payload so the metric does not multiply shared DuckDB blocks.
	gstate.endpoint_partition_bytes = gstate.counted_endpoint_count * sizeof(hash_t);
	gstate.endpoint_partition_csrs.clear();
	gstate.endpoint_partition_csrs.resize(gstate.endpoint_logical_partition_count);
	gstate.endpoint_partition_build_order.resize(gstate.endpoint_logical_partition_count);
	std::iota(gstate.endpoint_partition_build_order.begin(), gstate.endpoint_partition_build_order.end(), 0);
	std::stable_sort(gstate.endpoint_partition_build_order.begin(), gstate.endpoint_partition_build_order.end(),
	                 [&](idx_t left, idx_t right) {
		                 auto left_count = endpoint_partitions[left] ? endpoint_partitions[left]->Count() : 0;
		                 auto right_count = endpoint_partitions[right] ? endpoint_partitions[right]->Count() : 0;
		                 return left_count > right_count;
	                 });
	gstate.next_endpoint_partition.store(0, std::memory_order_relaxed);
	gstate.endpoint_count = gstate.counted_endpoint_count;
	SampleCSRBuildMemory(gstate);
	AppendOperatorPhaseTiming(
	    context, "endpoint_radix_partition", gstate.num_threads, gstate.pair_stats.pair_count, gstate.endpoint_count,
	    gstate.endpoint_logical_partition_count,
	    std::chrono::duration<double, std::milli>(partition_end - gstate.endpoint_build_start).count(),
	    gstate.endpoint_partition_bytes);
	gstate.endpoint_fill_start = partition_end;
}

static void FinalizePrecountedEndpointRows(PathFindingGlobalSinkState &gstate) {
	auto finalize_start = std::chrono::steady_clock::now();
	AppendOperatorPhaseTiming(
	    gstate.context_, "precount_fill", gstate.num_threads, gstate.pair_stats.pair_count, gstate.endpoint_count,
	    gstate.endpoint_partition_csrs.size(),
	    std::chrono::duration<double, std::milli>(finalize_start - gstate.endpoint_fill_start).count(), 0);
	idx_t memory_bytes = 0;
	for (auto &partition : gstate.endpoint_partition_csrs) {
		partition->FinalizeSparseRows();
		memory_bytes += partition->source_vertices.capacity() * sizeof(uint32_t);
		memory_bytes += partition->row_offsets.capacity() * sizeof(uint32_t);
		memory_bytes += partition->e.capacity() * sizeof(uint16_t);
	}
	if (gstate.endpoint_build_started) {
		auto end_time = std::chrono::steady_clock::now();
		AppendOperatorPhaseTiming(
		    gstate.context_, "precount_sparse_finalize", gstate.num_threads, gstate.pair_stats.pair_count,
		    gstate.endpoint_count, gstate.endpoint_partition_csrs.size(),
		    std::chrono::duration<double, std::milli>(end_time - finalize_start).count(), memory_bytes);
		AppendOperatorPhaseTiming(
		    gstate.context_, "local_csr_forward", gstate.num_threads, gstate.pair_stats.pair_count,
		    gstate.endpoint_count, gstate.endpoint_partition_csrs.size(),
		    std::chrono::duration<double, std::milli>(end_time - gstate.endpoint_build_start).count(), memory_bytes);
	}
}

SinkFinalizeType PhysicalPathFinding::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
	if (gstate.cached_partitioned_csr_input) {
		return FinalizePathFindingPhase(gstate, pipeline, event, *this, context);
	}
	if (gstate.edge_input) {
		if (gstate.child == 0) {
			return FinalizeEndpointPairPhase(gstate, context);
		}
		if (gstate.buffered_edge_input) {
			FinalizePartitionedEndpointInput(gstate, context);
			return FinalizePathFindingPhase(gstate, pipeline, event, *this, context);
		}
		if (gstate.precounted_edge_input && gstate.child == 1) {
			return FinalizePrecountedEndpointCounts(gstate, context);
		}
		if (gstate.endpoint_count != gstate.expected_edge_count) {
			throw ConstraintException("Direct endpoint CSR expected %llu graph edges but received %llu; check vertex "
			                          "uniqueness and references",
			                          gstate.expected_edge_count, gstate.endpoint_count);
		}
		if (gstate.precounted_edge_input) {
			FinalizePrecountedEndpointRows(gstate);
		}
		return FinalizePathFindingPhase(gstate, pipeline, event, *this, context);
	}
	if (gstate.csr == nullptr) {
		throw InternalException("CSR not initialized");
	}

	if (gstate.child == 0) {
		return FinalizeCSRIdPhase(gstate);
	}
	return FinalizePathFindingPhase(gstate, pipeline, event, *this, context);
}

InsertionOrderPreservingMap<string> PhysicalPathFinding::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Mode"] = mode;
	if (buffered_edge_input) {
		result["CSR Input"] = "radix-partitioned endpoints";
	}
	if (cached_partitioned_csr_input) {
		result["CSR Input"] = "cached PartitionCSR";
	}
	if (!cache_key.empty()) {
		result["CSR Cache Key"] = cache_key;
	}
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
OperatorResultType PhysicalPathFinding::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                        GlobalOperatorState &gstate, OperatorState &state) const {
	return OperatorResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

class PathFindingLocalSourceState : public LocalSourceState {
public:
	explicit PathFindingLocalSourceState(ClientContext &context, const PhysicalPathFinding &op) : op(op) {
	}

	const PhysicalPathFinding &op;
};

class PathFindingGlobalSourceState : public GlobalSourceState {
public:
	explicit PathFindingGlobalSourceState(const PhysicalPathFinding &op) : op(op), initialized(false) {
	}

public:
	idx_t MaxThreads() override {
		return 1;
	}
	const PhysicalPathFinding &op;

	mutex lock;
	bool initialized;
};

unique_ptr<GlobalSourceState> PhysicalPathFinding::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PathFindingGlobalSourceState>(*this);
}

unique_ptr<LocalSourceState> PhysicalPathFinding::GetLocalSourceState(ExecutionContext &context,
                                                                      GlobalSourceState &gstate) const {
	return make_uniq<PathFindingLocalSourceState>(context.client, *this);
}

SourceResultType PhysicalPathFinding::GetDataInternal(ExecutionContext &context, DataChunk &result,
                                                      OperatorSourceInput &input) const {
	auto &pf_sink = sink_state->Cast<PathFindingGlobalSinkState>();
	// If there are no pairs, we're done
	if (pf_sink.global_pairs->Count() == 0) {
		return SourceResultType::FINISHED;
	}

	if (pf_sink.use_source_grouping) {
		auto scatter_start = std::chrono::steady_clock::now();
		D_ASSERT(pf_sink.result_scan_idx < pf_sink.source_group_output_refs.size());
		auto output_ref = pf_sink.source_group_output_refs[pf_sink.result_scan_idx];
		shared_ptr<DataChunk> output_pairs;
		optional_ptr<vector<idx_t>> output_states;
		optional_ptr<SourceGroupedIterativeLengthState> single_source_group;
		if (pf_sink.source_group_preserve_output_order) {
			output_pairs = pf_sink.global_output_batches[output_ref.second];
			output_states = pf_sink.source_group_output_states[output_ref.second];
			D_ASSERT(output_states->size() == output_pairs->size());
		} else {
			single_source_group = pf_sink.source_group_states[output_ref.first].get();
			output_pairs = single_source_group->output_chunks[output_ref.second];
		}

		auto output_size = output_pairs->size();
		auto output_results = make_shared_ptr<DataChunk>();
		output_results->Initialize(context.client, {LogicalType::BIGINT}, output_size);
		output_results->SetChildCardinality(output_size);
		auto result_data = FlatVector::GetDataMutable<int64_t>(output_results->data[0]);
		auto &result_validity = FlatVector::ValidityMutable(output_results->data[0]);

		UnifiedVectorFormat src_format;
		UnifiedVectorFormat dst_format;
		output_pairs->data[0].ToUnifiedFormat(src_format);
		output_pairs->data[1].ToUnifiedFormat(dst_format);
		auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
		auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);

		for (idx_t row = 0; row < output_size; row++) {
			auto src_idx = src_format.sel->get_index(row);
			auto dst_idx = dst_format.sel->get_index(row);
			auto target = pf_sink.search_orientation == PathFindingSearchOrientation::REVERSE ? src_data[src_idx]
			                                                                                  : dst_data[dst_idx];
			optional_ptr<SourceGroupedIterativeLengthState> source_group = single_source_group;
			if (pf_sink.source_group_preserve_output_order) {
				auto group_idx = (*output_states)[row];
				source_group =
				    group_idx == DConstants::INVALID_INDEX ? nullptr : pf_sink.source_group_states[group_idx].get();
			}
			// Rows without a group have a NULL endpoint and no path
			auto distance = source_group ? source_group->Distance(target) : -1;
			if (distance < 0) {
				result_validity.SetInvalid(row);
				result_data[row] = -1;
			} else {
				result_data[row] = distance;
			}
		}

		output_pairs->Fuse(*output_results);
		result.Move(*output_pairs);
		auto scatter_end = std::chrono::steady_clock::now();
		pf_sink.source_group_scatter_ms +=
		    std::chrono::duration<double, std::milli>(scatter_end - scatter_start).count();
		pf_sink.source_group_scatter_rows += output_size;
		pf_sink.source_group_scatter_chunks++;

		pf_sink.result_scan_idx++;
		if (pf_sink.result_scan_idx == pf_sink.source_group_output_refs.size()) {
			AppendOperatorPhaseTiming(context.client, "source_group_scatter", pf_sink.num_threads,
			                          pf_sink.source_group_scatter_rows, pf_sink.source_group_scatter_chunks,
			                          pf_sink.source_group_zero_copy ? 1 : 0, pf_sink.source_group_scatter_ms, 0);
			return SourceResultType::FINISHED;
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	if (pf_sink.use_global_deduplication) {
		if (!pf_sink.global_dedupe_results_initialized) {
			for (auto &state : pf_sink.bfs_states) {
				state->pf_results->SetChildCardinality(state->pairs->size());
			}
			pf_sink.global_dedupe_results_initialized = true;
		}

		D_ASSERT(pf_sink.result_scan_idx < pf_sink.global_output_batches.size());
		auto output_pairs = pf_sink.global_output_batches[pf_sink.result_scan_idx];
		auto &output_to_search = pf_sink.global_output_to_search[pf_sink.result_scan_idx];
		D_ASSERT(output_to_search.size() == output_pairs->size());

		auto scatter_start = std::chrono::steady_clock::now();
		auto output_results = make_shared_ptr<DataChunk>();
		output_results->Initialize(context.client, {pf_sink.bfs_states[0]->bfs_type}, output_pairs->size());
		output_results->SetChildCardinality(output_pairs->size());
		for (idx_t output_row = 0; output_row < output_pairs->size(); output_row++) {
			auto global_search_row = output_to_search[output_row];
			auto search_batch_idx = global_search_row / STANDARD_VECTOR_SIZE;
			auto search_row = global_search_row - search_batch_idx * STANDARD_VECTOR_SIZE;
			D_ASSERT(search_batch_idx < pf_sink.bfs_states.size());
			auto &search_state = pf_sink.bfs_states[search_batch_idx];
			D_ASSERT(search_row < search_state->pf_results->size());
			output_results->data[0].SetValue(output_row, search_state->pf_results->GetValue(0, search_row));
		}
		output_pairs->Fuse(*output_results);
		result.Move(*output_pairs);
		auto scatter_end = std::chrono::steady_clock::now();
		auto scatter_ms = std::chrono::duration<double, std::milli>(scatter_end - scatter_start).count();
		AppendOperatorPhaseTiming(context.client, "dedupe_scatter", pf_sink.num_threads, output_to_search.size(), 0, 0,
		                          scatter_ms, output_to_search.capacity() * sizeof(idx_t));

		pf_sink.result_scan_idx++;
		if (pf_sink.result_scan_idx == pf_sink.global_output_batches.size()) {
			return SourceResultType::FINISHED;
		}
		return SourceResultType::HAVE_MORE_OUTPUT;
	}

	D_ASSERT(pf_sink.result_scan_idx < pf_sink.bfs_states.size());
	auto current_state = pf_sink.bfs_states[pf_sink.result_scan_idx];
	D_ASSERT(current_state->batch->output_index == pf_sink.result_scan_idx);
	current_state->pf_results->SetChildCardinality(current_state->pairs->size());
	current_state->batch->output_pairs->Fuse(*current_state->pf_results);
	result.Move(*current_state->batch->output_pairs);

	pf_sink.result_scan_idx++;
	if (pf_sink.result_scan_idx == pf_sink.bfs_states.size()) {
		return SourceResultType::FINISHED;
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalPathFinding::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 1 || children.size() == 2 || children.size() == 3);
	if (meta_pipeline.HasRecursiveCTE()) {
		throw NotImplementedException("Path Finding is not supported in recursive CTEs yet");
	}

	// becomes a source after both children fully sink their data
	meta_pipeline.GetState().SetPipelineSource(current, *this);

	// Create one child meta pipeline that will hold the LHS and RHS pipelines
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	if (cached_partitioned_csr_input) {
		D_ASSERT(children.size() == 1);
		auto pair_pipeline = child_meta_pipeline.GetBasePipeline();
		children[0].get().BuildPipelines(*pair_pipeline, child_meta_pipeline);
		return;
	}
	if (precounted_edge_input) {
		// Nest the phases in reverse dependency order. Each meta-pipeline has its own finalize event, so the
		// count prefix sums and exact allocation complete before the fill scan starts.
		auto fill_pipeline = child_meta_pipeline.GetBasePipeline();
		children[2].get().BuildPipelines(*fill_pipeline, child_meta_pipeline);

		auto &count_meta_pipeline = child_meta_pipeline.CreateChildMetaPipeline(*fill_pipeline, *this);
		auto count_pipeline = count_meta_pipeline.GetBasePipeline();
		children[1].get().BuildPipelines(*count_pipeline, count_meta_pipeline);

		auto &pair_meta_pipeline = count_meta_pipeline.CreateChildMetaPipeline(*count_pipeline, *this);
		auto pair_pipeline = pair_meta_pipeline.GetBasePipeline();
		children[0].get().BuildPipelines(*pair_pipeline, pair_meta_pipeline);
		return;
	}

	vector<idx_t> child_order;
	if (edge_input) {
		child_order = {0, 1};
	} else {
		child_order = {1, 0};
	}

	// Endpoint inputs sink pairs before the edge scan.
	auto first_pipeline = child_meta_pipeline.GetBasePipeline();
	children[child_order[0]].get().BuildPipelines(*first_pipeline, child_meta_pipeline);
	for (idx_t order_idx = 1; order_idx < child_order.size(); order_idx++) {
		auto &pipeline = child_meta_pipeline.CreatePipeline();
		children[child_order[order_idx]].get().BuildPipelines(pipeline, child_meta_pipeline);
		child_meta_pipeline.AddFinishEvent(pipeline);
	}
}

} // namespace duckdb
