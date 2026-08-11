#include "duckpgq/core/operator/physical_path_finding_operator.hpp"
#include "duckpgq/common.hpp"
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/hyperloglog.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
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
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>

namespace duckdb {

namespace {

static mutex path_finding_operator_phase_timing_lock;

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

size_t GetLocalCSREdgeCount(const std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	size_t edge_count = 0;
	for (const auto &local_csr : partition_csrs) {
		edge_count += local_csr->GetEdgeSize();
	}
	return edge_count;
}

idx_t GetDirectEndpointPartitionWidth(idx_t vertex_count, idx_t num_threads, ClientContext &context) {
	auto target_partition_count =
	    std::max<idx_t>(1, num_threads) * (1 + std::max<int32_t>(1, GetLightPartitionMultiplier(context)));
	auto partition_width = std::max<idx_t>(1, (vertex_count + 2 + target_partition_count - 1) / target_partition_count);
	return std::min<idx_t>(partition_width, UINT16_MAX);
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

void MarkTransientGlobalCSRForDeletion(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	if (!gstate.edge_input && !gstate.cached_partitioned_csr_input) {
		GetDuckPGQState(context)->csr_to_delete.insert(gstate.csr_id);
	}
}

} // namespace

class SourceGroupedIterativeLengthState {
public:
	SourceGroupedIterativeLengthState(ClientContext &context_p, int64_t source_p,
	                                  std::vector<shared_ptr<LocalCSR>> &local_csrs_p, idx_t num_threads_p,
	                                  int64_t v_size_p)
	    : context(context_p), source(source_p), local_csrs(local_csrs_p), num_threads(num_threads_p), v_size(v_size_p),
	      distances(v_size, -1), seen(v_size), frontier(v_size), next(v_size), iter(1), change(false),
	      tasks_scheduled(0) {
		worker_changed.resize(num_threads, 0);
		partition_counter = 0;
		local_csr_counter = 0;
		benchmark_enabled = GetPathFindingBenchmarkOption(context);
		benchmark_output_prefix = GetPathFindingBenchmarkPrefix(context);
	}

	void Initialize() {
		if (source < 0 || source >= v_size) {
			return;
		}
		seen[source].store(1, std::memory_order_relaxed);
		frontier[source].store(1, std::memory_order_relaxed);
		distances[source] = 0;
		change = true;
	}

	int64_t Distance(int64_t target) const {
		if (target < 0 || target >= v_size) {
			return -1;
		}
		return distances[target];
	}

	ClientContext &context;
	int64_t source;
	std::vector<shared_ptr<LocalCSR>> local_csrs;
	idx_t num_threads;
	int64_t v_size;
	vector<int64_t> distances;
	vector<atomic<uint8_t>> seen;
	vector<atomic<uint8_t>> frontier;
	vector<atomic<uint8_t>> next;
	vector<uint8_t> worker_changed;
	atomic<int64_t> partition_counter;
	atomic<int64_t> local_csr_counter;
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
		Execute();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	void Execute() {
		auto &barrier = state->barrier;
		if (worker_id == 0) {
			state->Initialize();
		}
		barrier->Wait(worker_id);

		while (state->change) {
			while (true) {
				auto partition_idx = state->partition_counter.fetch_add(1);
				if (partition_idx >= static_cast<int64_t>(state->local_csrs.size())) {
					break;
				}
				auto &local_csr = state->local_csrs[partition_idx];
				for (auto vertex = local_csr->start_vertex; vertex < local_csr->end_vertex; vertex++) {
					state->next[vertex].store(0, std::memory_order_relaxed);
				}
			}
			barrier->Wait(worker_id);

			if (worker_id == 0) {
				state->partition_counter = 0;
				state->local_csr_counter = 0;
				state->change = false;
				std::fill(state->worker_changed.begin(), state->worker_changed.end(), 0);
			}
			barrier->Wait(worker_id);

			while (true) {
				auto partition_idx = state->local_csr_counter.fetch_add(1);
				if (partition_idx >= static_cast<int64_t>(state->local_csrs.size())) {
					break;
				}
				auto &local_csr = *state->local_csrs[partition_idx];
				for (const auto &segment : local_csr.segments) {
					for (idx_t row_idx = 0; row_idx < segment.source_vertices.size(); row_idx++) {
						auto source_vertex = segment.source_vertices[row_idx];
						if (!state->frontier[source_vertex].load(std::memory_order_relaxed)) {
							continue;
						}
						for (auto offset = segment.row_offsets[row_idx]; offset < segment.row_offsets[row_idx + 1];
						     offset++) {
							auto target = segment.edges[offset] + local_csr.start_vertex;
							if (!state->seen[target].load(std::memory_order_relaxed)) {
								state->next[target].store(1, std::memory_order_relaxed);
							}
						}
					}
				}
				if (!local_csr.segments.empty()) {
					continue;
				}
				if (local_csr.HasSparseRows()) {
					for (idx_t row_idx = 0; row_idx < local_csr.source_vertices.size(); row_idx++) {
						auto source_vertex = local_csr.source_vertices[row_idx];
						if (!state->frontier[source_vertex].load(std::memory_order_relaxed)) {
							continue;
						}
						auto start_edges = local_csr.row_offsets[row_idx];
						auto end_edges = local_csr.row_offsets[row_idx + 1];
						for (auto offset = start_edges; offset < end_edges; offset++) {
							auto target = local_csr.e[offset] + local_csr.start_vertex;
							if (!state->seen[target].load(std::memory_order_relaxed)) {
								state->next[target].store(1, std::memory_order_relaxed);
							}
						}
					}
					continue;
				}

				for (idx_t vertex = 0; vertex < local_csr.GetVertexSize(); vertex++) {
					if (!state->frontier[vertex].load(std::memory_order_relaxed)) {
						continue;
					}
					auto start_edges = local_csr.v[vertex].load(std::memory_order_relaxed);
					auto end_edges = local_csr.v[vertex + 1].load(std::memory_order_relaxed);
					for (auto offset = start_edges; offset < end_edges; offset++) {
						auto target = local_csr.e[offset] + local_csr.start_vertex;
						if (!state->seen[target].load(std::memory_order_relaxed)) {
							state->next[target].store(1, std::memory_order_relaxed);
						}
					}
				}
			}
			barrier->Wait(worker_id);

			bool local_change = false;
			while (true) {
				auto partition_idx = state->partition_counter.fetch_add(1);
				if (partition_idx >= static_cast<int64_t>(state->local_csrs.size())) {
					break;
				}
				auto &local_csr = state->local_csrs[partition_idx];
				for (auto vertex = local_csr->start_vertex; vertex < local_csr->end_vertex; vertex++) {
					if (state->next[vertex].load(std::memory_order_relaxed) &&
					    !state->seen[vertex].load(std::memory_order_relaxed)) {
						state->seen[vertex].store(1, std::memory_order_relaxed);
						state->frontier[vertex].store(1, std::memory_order_relaxed);
						state->distances[vertex] = state->iter;
						local_change = true;
					} else {
						state->frontier[vertex].store(0, std::memory_order_relaxed);
					}
				}
			}
			state->worker_changed[worker_id] = local_change ? 1 : 0;
			barrier->Wait(worker_id);

			if (worker_id == 0) {
				bool any_change = false;
				for (idx_t worker = 0; worker < state->tasks_scheduled; worker++) {
					any_change |= state->worker_changed[worker] != 0;
				}
				state->change = any_change;
				state->partition_counter = 0;
				state->iter++;
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
		idx_t num_partitions = state->local_csrs.size();
		for (idx_t worker = 0; worker < std::min(state->num_threads, num_partitions); worker++) {
			tasks.push_back(
			    make_uniq<SourceGroupedIterativeLengthTask>(shared_from_this(), context, state, worker, op));
			state->tasks_scheduled++;
		}
		state->barrier = make_uniq<Barrier>(state->tasks_scheduled);
		SetTasks(std::move(tasks));
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
		AppendOperatorPhaseTiming(state->context, "source_group_bfs", state->num_threads, output_count,
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

string GetPartitionedCSRCacheKey(const PhysicalPathFinding &op, const PathFindingGlobalSinkState &gstate,
                                 const LocalCSRState &local_csr_state, ClientContext &context) {
	if (op.cache_key.empty()) {
		return string();
	}

	std::ostringstream key;
	key << std::setprecision(std::numeric_limits<double>::max_digits10);
	key << "partitioned-csr-v2|graph=" << op.cache_key.size() << ":" << op.cache_key;
	key << "|vertices=" << GetPathFindingVSize(gstate) << "|edges=" << GetPathFindingEdgeCount(gstate);
	key << "|input="
	    << (gstate.cached_partitioned_csr_input || gstate.precounted_edge_input
	            ? "precounted-endpoints"
	            : (gstate.edge_input ? "segmented-endpoints" : "global-csr"));
	if (gstate.cached_partitioned_csr_input || gstate.edge_input) {
		key << "|partition_width=" << local_csr_state.streaming_partition_width;
	}
	key << "|threads=" << local_csr_state.num_threads;
	key << "|forward=" << local_csr_state.build_forward_csr;
	key << "|reverse=" << local_csr_state.build_reverse_csr;
	key << "|pull=" << local_csr_state.build_pull_csr;
	key << "|sparse=" << local_csr_state.finalize_sparse_rows;
	key << "|heavy_fraction=" << GetHeavyPartitionFraction(context);
	key << "|light_multiplier=" << GetLightPartitionMultiplier(context);
	return key.str();
}

bool TryLoadPartitionedCSR(PathFindingGlobalSinkState &gstate, const PhysicalPathFinding &op,
                           LocalCSRState &local_csr_state, ClientContext &context) {
	local_csr_state.cache_key = GetPartitionedCSRCacheKey(op, gstate, local_csr_state, context);
	if (local_csr_state.cache_key.empty()) {
		return false;
	}

	auto start_time = std::chrono::steady_clock::now();
	auto duckpgq_state = GetDuckPGQState(context);
	auto cached_index = duckpgq_state->GetPartitionedCSR(local_csr_state.cache_key);
	auto end_time = std::chrono::steady_clock::now();
	auto lookup_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
	if (!cached_index || cached_index->vertex_count != GetPathFindingVSize(gstate) ||
	    cached_index->edge_count != GetPathFindingEdgeCount(gstate)) {
		AppendOperatorPhaseTiming(context, "partitioned_csr_cache_miss", gstate.num_threads,
		                          gstate.pair_stats.pair_count, GetPathFindingEdgeCount(gstate), 0, lookup_ms, 0);
		return false;
	}

	local_csr_state.partition_csrs = cached_index->forward_partitions;
	local_csr_state.reverse_partition_csrs = cached_index->reverse_partitions;
	local_csr_state.pull_partition_csrs = cached_index->pull_partitions;
	local_csr_state.forward_build_buffers.clear();
	local_csr_state.loaded_from_cache = true;
	AppendOperatorPhaseTiming(context, "partitioned_csr_cache_hit", gstate.num_threads, gstate.pair_stats.pair_count,
	                          GetPathFindingEdgeCount(gstate),
	                          local_csr_state.partition_csrs.size() + local_csr_state.reverse_partition_csrs.size() +
	                              local_csr_state.pull_partition_csrs.size(),
	                          lookup_ms, 0);
	return true;
}

void PublishPartitionedCSR(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	auto &local_csr_state = *gstate.local_csr_state;
	if (local_csr_state.cache_key.empty() || local_csr_state.loaded_from_cache || local_csr_state.published_to_cache) {
		return;
	}

	auto start_time = std::chrono::steady_clock::now();
	auto index = make_shared_ptr<PartitionedCSRIndex>();
	index->vertex_count = GetPathFindingVSize(gstate);
	index->edge_count = GetPathFindingEdgeCount(gstate);
	index->forward_partitions = local_csr_state.partition_csrs;
	index->reverse_partitions = local_csr_state.reverse_partition_csrs;
	index->pull_partitions = local_csr_state.pull_partition_csrs;
	GetDuckPGQState(context)->PutPartitionedCSR(local_csr_state.cache_key, std::move(index));
	local_csr_state.published_to_cache = true;
	auto end_time = std::chrono::steady_clock::now();
	auto publish_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
	AppendOperatorPhaseTiming(context, "partitioned_csr_cache_publish", gstate.num_threads,
	                          gstate.pair_stats.pair_count, GetPathFindingEdgeCount(gstate),
	                          local_csr_state.partition_csrs.size() + local_csr_state.reverse_partition_csrs.size() +
	                              local_csr_state.pull_partition_csrs.size(),
	                          publish_ms, 0);
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

void ScheduleBFSBatchForMode(PathFindingGlobalSinkState &gstate, const shared_ptr<PathFindingBatch> &batch,
                             Pipeline &pipeline, Event &event, const PhysicalPathFinding *op, ClientContext &context) {
	auto bfs_state =
	    CreateBFSStateForMode(gstate.path_finding_mode, batch, gstate.search_orientation, *gstate.local_csr_state,
	                          gstate.num_threads, context, GetPathFindingVSize(gstate));
	bfs_state->ScheduleBFSBatch(pipeline, event, op);
	gstate.bfs_states.push_back(std::move(bfs_state));
}

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

bool ShouldUseSourceGroupedIterativeLength(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	auto ratio = GetPathFindingSourceGroupRatio(context);
	if (gstate.path_finding_mode != PathFindingOperatorMode::ITERATIVE_LENGTH || ratio <= 0 ||
	    gstate.pair_stats.null_pair_count > 0) {
		return false;
	}
	auto distinct_search_sources = gstate.search_orientation == PathFindingSearchOrientation::REVERSE
	                                   ? gstate.pair_stats.distinct_dst_count
	                                   : gstate.pair_stats.distinct_src_count;
	if (distinct_search_sources == 0) {
		return false;
	}
	return static_cast<double>(gstate.pair_stats.pair_count) >=
	       static_cast<double>(ratio) * static_cast<double>(distinct_search_sources);
}

void SchedulePathFindingBatches(PathFindingGlobalSinkState &gstate, vector<shared_ptr<PathFindingBatch>> &batches,
                                Pipeline &pipeline, Event &event, const PhysicalPathFinding &op,
                                ClientContext &context) {
	if (!ShouldUseGroupedIterativeLengthBatches(gstate, context, batches.size())) {
		for (auto &batch : batches) {
			ScheduleBFSBatchForMode(gstate, batch, pipeline, event, &op, context);
		}
		return;
	}

	auto workers_per_batch = GetGroupedWorkersPerBatch(gstate, context);
	auto group_count = GetGroupedBatchCount(gstate, context, workers_per_batch, batches.size());
	vector<shared_ptr<IterativeLengthState>> iterative_states;
	iterative_states.reserve(batches.size());

	for (auto &batch : batches) {
		auto &partition_csrs = gstate.search_orientation == PathFindingSearchOrientation::REVERSE
		                           ? gstate.local_csr_state->reverse_partition_csrs
		                           : gstate.local_csr_state->partition_csrs;
		auto state = make_shared_ptr<IterativeLengthState>(batch, partition_csrs, workers_per_batch, context,
		                                                   GetPathFindingVSize(gstate));
		iterative_states.push_back(state);
		gstate.bfs_states.push_back(state);
	}

	event.InsertEvent(make_shared_ptr<GroupedIterativeLengthEvent>(std::move(iterative_states), workers_per_batch,
	                                                               group_count, pipeline, op));
}

class SourceGroupedScheduleEvent : public BasePipelineEvent {
public:
	SourceGroupedScheduleEvent(PathFindingGlobalSinkState &gstate_p, Pipeline &pipeline_p,
	                           const PhysicalPathFinding &op_p, ClientContext &context_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p), op(op_p), context(context_p) {
	}

	void Schedule() override {
	}

	void FinishEvent() override {
		PublishPartitionedCSR(gstate, context);
		auto &partition_csrs = gstate.search_orientation == PathFindingSearchOrientation::REVERSE
		                           ? gstate.local_csr_state->reverse_partition_csrs
		                           : gstate.local_csr_state->partition_csrs;
		for (idx_t group_idx = 0; group_idx < gstate.source_group_sources.size(); group_idx++) {
			auto state = make_shared_ptr<SourceGroupedIterativeLengthState>(
			    context, gstate.source_group_sources[group_idx], partition_csrs, gstate.num_threads,
			    GetPathFindingVSize(gstate));
			state->output_chunks = std::move(gstate.source_group_output_chunks[group_idx]);
			auto state_idx = gstate.source_group_states.size();
			for (idx_t chunk_idx = 0; chunk_idx < state->output_chunks.size(); chunk_idx++) {
				gstate.source_group_output_refs.emplace_back(state_idx, chunk_idx);
			}
			gstate.source_group_states.push_back(state);
			InsertEvent(make_shared_ptr<SourceGroupedIterativeLengthEvent>(state, *pipeline, op));
		}
	}

private:
	PathFindingGlobalSinkState &gstate;
	const PhysicalPathFinding &op;
	ClientContext &context;
};

class PathFindingScheduleEvent : public BasePipelineEvent {
public:
	PathFindingScheduleEvent(vector<shared_ptr<PathFindingBatch>> batches_p, PathFindingGlobalSinkState &gstate_p,
	                         Pipeline &pipeline_p, const PhysicalPathFinding &op_p, ClientContext &context_p)
	    : BasePipelineEvent(pipeline_p), batches(std::move(batches_p)), gstate(gstate_p), op(op_p), context(context_p) {
	}

	void Schedule() override {
	}

	void FinishEvent() override {
		PublishPartitionedCSR(gstate, context);
		SchedulePathFindingBatches(gstate, batches, *pipeline, *this, op, context);
	}

private:
	vector<shared_ptr<PathFindingBatch>> batches;
	PathFindingGlobalSinkState &gstate;
	const PhysicalPathFinding &op;
	ClientContext &context;
};

void ScheduleLocalCSRBuildThenPathFinding(PathFindingGlobalSinkState &gstate,
                                          vector<shared_ptr<PathFindingBatch>> batches, Pipeline &pipeline,
                                          Event &event, const PhysicalPathFinding &op, ClientContext &context) {
	if (gstate.cached_partitioned_csr_input && gstate.endpoint_partition_width == 0) {
		gstate.endpoint_partition_width =
		    GetDirectEndpointPartitionWidth(gstate.vertex_count, gstate.num_threads, context);
	}
	shared_ptr<LocalCSRState> local_csr_state;
	if (gstate.cached_partitioned_csr_input || gstate.precounted_edge_input) {
		std::vector<std::vector<LocalCSRBuildPartition>> empty_buffers;
		local_csr_state = make_shared_ptr<LocalCSRState>(context, std::move(empty_buffers), gstate.vertex_count,
		                                                 GetPathFindingEdgeCount(gstate),
		                                                 gstate.endpoint_partition_width, gstate.num_threads);
		local_csr_state->partition_csrs = std::move(gstate.endpoint_partition_csrs);
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
	if (TryLoadPartitionedCSR(gstate, op, *local_csr_state, context)) {
		event.InsertEvent(make_shared_ptr<PathFindingScheduleEvent>(std::move(batches), gstate, pipeline, op, context));
		return;
	}
	if (gstate.cached_partitioned_csr_input) {
		throw ConstraintException(
		    "Cached PartitionCSR not found for cache key '%s'; build it before using the cache-only path",
		    op.cache_key);
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
	if (gstate.cached_partitioned_csr_input && gstate.endpoint_partition_width == 0) {
		gstate.endpoint_partition_width =
		    GetDirectEndpointPartitionWidth(gstate.vertex_count, gstate.num_threads, context);
	}
	shared_ptr<LocalCSRState> local_csr_state;
	if (gstate.cached_partitioned_csr_input || gstate.precounted_edge_input) {
		std::vector<std::vector<LocalCSRBuildPartition>> empty_buffers;
		local_csr_state = make_shared_ptr<LocalCSRState>(context, std::move(empty_buffers), gstate.vertex_count,
		                                                 GetPathFindingEdgeCount(gstate),
		                                                 gstate.endpoint_partition_width, gstate.num_threads);
		local_csr_state->partition_csrs = std::move(gstate.endpoint_partition_csrs);
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
	if (TryLoadPartitionedCSR(gstate, op, *local_csr_state, context)) {
		event.InsertEvent(make_shared_ptr<SourceGroupedScheduleEvent>(gstate, pipeline, op, context));
		return;
	}
	if (gstate.cached_partitioned_csr_input) {
		throw ConstraintException(
		    "Cached PartitionCSR not found for cache key '%s'; build it before using the cache-only path",
		    op.cache_key);
	}
	if (gstate.precounted_edge_input) {
		event.InsertEvent(make_shared_ptr<SourceGroupedScheduleEvent>(gstate, pipeline, op, context));
		return;
	}

	auto local_csr_event = make_shared_ptr<LocalCSREvent>(local_csr_state, pipeline, op, context);
	event.InsertEvent(local_csr_event);
	local_csr_event->InsertEvent(make_shared_ptr<SourceGroupedScheduleEvent>(gstate, pipeline, op, context));
}

struct SourceGroupBuildState {
	int64_t source;
	vector<shared_ptr<DataChunk>> output_chunks;
	shared_ptr<DataChunk> current_chunk;
	idx_t current_count = 0;
};

void AppendSourceGroupRow(ClientContext &context, SourceGroupBuildState &group, DataChunk &source_chunk, idx_t row) {
	if (!group.current_chunk || group.current_count == STANDARD_VECTOR_SIZE) {
		if (group.current_chunk) {
			group.current_chunk->SetChildCardinality(group.current_count);
			group.output_chunks.push_back(group.current_chunk);
		}
		group.current_chunk = make_shared_ptr<DataChunk>();
		group.current_chunk->Initialize(context, source_chunk.GetTypes());
		group.current_count = 0;
	}
	group.current_chunk->data[0].SetValue(group.current_count, source_chunk.GetValue(0, row));
	group.current_chunk->data[1].SetValue(group.current_count, source_chunk.GetValue(1, row));
	group.current_count++;
}

SinkFinalizeType FinalizeSourceGroupedPathFindingPhase(PathFindingGlobalSinkState &gstate, Pipeline &pipeline,
                                                       Event &event, const PhysicalPathFinding &op,
                                                       ClientContext &context) {
	auto start_time = std::chrono::steady_clock::now();
	std::unordered_map<int64_t, idx_t> source_to_group;
	vector<SourceGroupBuildState> groups;

	for (auto &chunk : gstate.global_output_batches) {
		UnifiedVectorFormat src_format;
		UnifiedVectorFormat dst_format;
		chunk->data[0].ToUnifiedFormat(src_format);
		chunk->data[1].ToUnifiedFormat(dst_format);
		auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
		auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);

		for (idx_t row = 0; row < chunk->size(); row++) {
			auto src_idx = src_format.sel->get_index(row);
			auto dst_idx = dst_format.sel->get_index(row);
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
			AppendSourceGroupRow(context, groups[entry->second], *chunk, row);
		}
	}

	idx_t output_chunk_count = 0;
	for (auto &group : groups) {
		if (group.current_chunk) {
			group.current_chunk->SetChildCardinality(group.current_count);
			group.output_chunks.push_back(group.current_chunk);
		}
		output_chunk_count += group.output_chunks.size();
		gstate.source_group_sources.push_back(group.source);
		gstate.source_group_output_chunks.push_back(std::move(group.output_chunks));
	}

	auto end_time = std::chrono::steady_clock::now();
	auto build_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
	AppendOperatorPhaseTiming(context, "source_group_build", gstate.num_threads, gstate.pair_stats.pair_count,
	                          groups.size(), output_chunk_count, build_ms, 0);

	gstate.global_output_batches.clear();
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
	gstate.search_orientation = ChooseSearchOrientation(gstate.pair_stats, gstate.path_finding_mode, context);

	auto duplicate_count = input_count - unique_source_rows.size();
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

	PathFindingPairStats pair_stats;
	HyperLogLog distinct_srcs;
	HyperLogLog distinct_dsts;
	gstate.global_pairs->InitializeScan(gstate.global_scan_state);
	while (gstate.global_scan_state.next_row_index < gstate.global_pairs->Count()) {
		auto current_chunk = make_shared_ptr<DataChunk>();
		current_chunk->Initialize(context, gstate.global_pairs->Types());
		gstate.global_pairs->Scan(gstate.global_scan_state, *current_chunk);
		AccumulatePairStats(*current_chunk, pair_stats, distinct_srcs, distinct_dsts);
		gstate.global_output_batches.push_back(current_chunk);
	}
	FinalizePairStats(gstate, pair_stats, distinct_srcs, distinct_dsts);
	gstate.search_orientation = ChooseSearchOrientation(gstate.pair_stats, gstate.path_finding_mode, context);
	if (ShouldUseSourceGroupedIterativeLength(gstate, context)) {
		return FinalizeSourceGroupedPathFindingPhase(gstate, pipeline, event, op, context);
	}

	if (GetPathFindingDeduplicatePairs(context)) {
		gstate.global_output_batches.clear();
		gstate.global_pairs->InitializeScan(gstate.global_scan_state);
		return FinalizeGlobalDeduplicatedPathFindingPhase(gstate, pipeline, event, op, context);
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
	edge_input = path_finding_op.edge_input;
	precounted_edge_input = path_finding_op.precounted_edge_input;
	precounted_vertex_count = path_finding_op.precounted_vertex_count;
	precounted_edge_count = path_finding_op.precounted_edge_count;
	cached_partitioned_csr_input = path_finding_op.cached_partitioned_csr_input;
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
		endpoint_partition_width = GetDirectEndpointPartitionWidth(vertex_count, thread_count, context);
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
	    GetDirectEndpointPartitionWidth(gstate.vertex_count, gstate.num_threads, gstate.context_);
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
	global_dedupe_results_initialized = false;
	edge_input = op.edge_input;
	precounted_edge_input = op.precounted_edge_input;
	cached_partitioned_csr_input = op.cached_partitioned_csr_input;
	vertex_count = op.precounted_vertex_count;
	expected_edge_count = op.precounted_edge_count;
	csr = nullptr;

	child = 0;
	mode = op.mode;
	path_finding_mode = ParsePathFindingOperatorMode(mode);
	search_orientation = PathFindingSearchOrientation::FORWARD;
	auto &scheduler = TaskScheduler::GetScheduler(context);
	num_threads = scheduler.NumberOfThreads();
}

void PathFindingGlobalSinkState::Sink(DataChunk &input, PathFindingLocalSinkState &lstate) {
	if (cached_partitioned_csr_input) {
		lstate.SinkPairs(input);
		return;
	}
	if (edge_input) {
		if (child == 0) {
			lstate.SinkPairs(input);
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
		D_ASSERT(pf_sink.result_scan_idx < pf_sink.source_group_output_refs.size());
		auto output_ref = pf_sink.source_group_output_refs[pf_sink.result_scan_idx];
		auto &source_group = pf_sink.source_group_states[output_ref.first];
		auto output_pairs = source_group->output_chunks[output_ref.second];

		auto output_results = make_shared_ptr<DataChunk>();
		output_results->Initialize(context.client, {LogicalType::BIGINT}, output_pairs->size());
		output_results->SetChildCardinality(output_pairs->size());
		auto result_data = FlatVector::GetDataMutable<int64_t>(output_results->data[0]);
		auto &result_validity = FlatVector::ValidityMutable(output_results->data[0]);

		UnifiedVectorFormat src_format;
		UnifiedVectorFormat dst_format;
		output_pairs->data[0].ToUnifiedFormat(src_format);
		output_pairs->data[1].ToUnifiedFormat(dst_format);
		auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
		auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);

		for (idx_t row = 0; row < output_pairs->size(); row++) {
			auto src_idx = src_format.sel->get_index(row);
			auto dst_idx = dst_format.sel->get_index(row);
			auto target = pf_sink.search_orientation == PathFindingSearchOrientation::REVERSE ? src_data[src_idx]
			                                                                                  : dst_data[dst_idx];
			auto distance = source_group->Distance(target);
			if (distance < 0) {
				result_validity.SetInvalid(row);
				result_data[row] = -1;
			} else {
				result_data[row] = distance;
			}
		}

		output_pairs->Fuse(*output_results);
		result.Move(*output_pairs);

		pf_sink.result_scan_idx++;
		if (pf_sink.result_scan_idx == pf_sink.source_group_output_refs.size()) {
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
