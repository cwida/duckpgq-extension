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
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
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
		return make_shared_ptr<PushPullIterativeLengthState>(batch, local_csr_state.partition_csrs,
		                                                     local_csr_state.pull_partition_csrs, num_threads, context,
		                                                     vsize);
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
	auto bfs_state = CreateBFSStateForMode(gstate.path_finding_mode, batch, gstate.search_orientation,
	                                       *gstate.local_csr_state, gstate.num_threads, context, gstate.csr->vsize);
	bfs_state->ScheduleBFSBatch(pipeline, event, op);
	gstate.bfs_states.push_back(std::move(bfs_state));
}

idx_t GetGroupedWorkersPerBatch(PathFindingGlobalSinkState &gstate, ClientContext &context) {
	auto total_threads = std::max<idx_t>(1, gstate.num_threads);
	auto configured_workers = GetPathFindingThreadsPerBatch(context);
	auto workers_per_batch =
	    configured_workers <= 0 ? total_threads : std::min<idx_t>(static_cast<idx_t>(configured_workers), total_threads);
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
		auto state =
		    make_shared_ptr<IterativeLengthState>(batch, partition_csrs, workers_per_batch, context, gstate.csr->vsize);
		iterative_states.push_back(state);
		gstate.bfs_states.push_back(state);
	}

	event.InsertEvent(make_shared_ptr<GroupedIterativeLengthEvent>(std::move(iterative_states), workers_per_batch,
	                                                               group_count, pipeline, op));
}

class PathFindingScheduleEvent : public BasePipelineEvent {
public:
	PathFindingScheduleEvent(vector<shared_ptr<PathFindingBatch>> batches_p, PathFindingGlobalSinkState &gstate_p,
	                         Pipeline &pipeline_p, const PhysicalPathFinding &op_p, ClientContext &context_p)
	    : BasePipelineEvent(pipeline_p), batches(std::move(batches_p)), gstate(gstate_p), op(op_p), context(context_p) {
	}

	void Schedule() override {
	}

	void FinishEvent() override {
		SchedulePathFindingBatches(gstate, batches, *pipeline, *this, op, context);
	}

private:
	vector<shared_ptr<PathFindingBatch>> batches;
	PathFindingGlobalSinkState &gstate;
	const PhysicalPathFinding &op;
	ClientContext &context;
};

void ScheduleLocalCSRBuildThenPathFinding(PathFindingGlobalSinkState &gstate, vector<shared_ptr<PathFindingBatch>> batches,
                                          Pipeline &pipeline, Event &event, const PhysicalPathFinding &op,
                                          ClientContext &context) {
	auto local_csr_state = make_shared_ptr<LocalCSRState>(context, gstate.csr, gstate.num_threads);
	ConfigureLocalCSRStateForMode(*local_csr_state, gstate.path_finding_mode);
	if (gstate.search_orientation == PathFindingSearchOrientation::REVERSE) {
		local_csr_state->build_forward_csr = false;
		local_csr_state->build_reverse_csr = true;
	}
	gstate.local_csr_state = local_csr_state;

	auto local_csr_event = make_shared_ptr<LocalCSREvent>(local_csr_state, pipeline, op, context);
	event.InsertEvent(local_csr_event);
	local_csr_event->InsertEvent(
	    make_shared_ptr<PathFindingScheduleEvent>(std::move(batches), gstate, pipeline, op, context));
}

SinkFinalizeType FinalizeCSRIdPhase(PathFindingGlobalSinkState &gstate) {
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
			batches.push_back(
			    CreateOrientedPathFindingBatch(context, gstate.search_orientation, output_batch, gstate.next_batch_index++));
		}
		ScheduleLocalCSRBuildThenPathFinding(gstate, std::move(batches), pipeline, event, op, context);
		gstate.global_output_batches.clear();
		gstate.global_output_to_search.clear();
		gstate.use_global_deduplication = false;
		++gstate.child;
		auto duckpgq_state = GetDuckPGQState(context);
		duckpgq_state->csr_to_delete.insert(gstate.csr_id);
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
	auto duckpgq_state = GetDuckPGQState(context);
	duckpgq_state->csr_to_delete.insert(gstate.csr_id);
	return SinkFinalizeType::READY;
}

SinkFinalizeType FinalizePathFindingPhase(PathFindingGlobalSinkState &gstate, Pipeline &pipeline, Event &event,
                                          const PhysicalPathFinding &op, ClientContext &context) {
	if (gstate.global_pairs->Count() == 0) {
		return SinkFinalizeType::READY;
	}

	if (GetPathFindingDeduplicatePairs(context)) {
		return FinalizeGlobalDeduplicatedPathFindingPhase(gstate, pipeline, event, op, context);
	}

	vector<shared_ptr<PathFindingBatch>> batches;
	PathFindingPairStats pair_stats;
	HyperLogLog distinct_srcs;
	HyperLogLog distinct_dsts;
	while (gstate.global_scan_state.next_row_index < gstate.global_pairs->Count()) {
		auto current_chunk = make_shared_ptr<DataChunk>();
		current_chunk->Initialize(context, gstate.global_pairs->Types());
		gstate.global_pairs->Scan(gstate.global_scan_state, *current_chunk);
		AccumulatePairStats(*current_chunk, pair_stats, distinct_srcs, distinct_dsts);
		auto batch = CreatePathFindingBatch(current_chunk, gstate.next_batch_index++);
		batches.push_back(std::move(batch));
	}
	FinalizePairStats(gstate, pair_stats, distinct_srcs, distinct_dsts);
	gstate.search_orientation = ChooseSearchOrientation(gstate.pair_stats, gstate.path_finding_mode, context);
	vector<shared_ptr<PathFindingBatch>> oriented_batches;
	oriented_batches.reserve(batches.size());
	for (auto &batch : batches) {
		oriented_batches.push_back(
		    CreateOrientedPathFindingBatch(context, gstate.search_orientation, batch->output_pairs, batch->output_index));
	}
	ScheduleLocalCSRBuildThenPathFinding(gstate, std::move(oriented_batches), pipeline, event, op, context);

	++gstate.child;
	auto duckpgq_state = GetDuckPGQState(context);
	duckpgq_state->csr_to_delete.insert(gstate.csr_id);
	return SinkFinalizeType::READY;
}

} // namespace

PhysicalPathFinding::PhysicalPathFinding(PhysicalPlan &physical_plan, LogicalExtensionOperator &op,
                                         PhysicalOperator &pairs, PhysicalOperator &csr)
    : PhysicalComparisonJoin(physical_plan, op, TYPE, {}, JoinType::INNER, op.estimated_cardinality) {
	children.push_back(pairs);
	children.push_back(csr);
	expressions = std::move(op.expressions);
	estimated_cardinality = op.estimated_cardinality;
	auto &path_finding_op = op.Cast<LogicalPathFindingOperator>();
	mode = path_finding_op.mode;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
PathFindingLocalSinkState::PathFindingLocalSinkState(ClientContext &context, const PhysicalPathFinding &op)
    : local_pairs(context, op.children[0].get().GetTypes()) {
}

void PathFindingLocalSinkState::Sink(DataChunk &input, idx_t child) {
	if (child == 1) {
		local_pairs.Append(input);
	}
}

PathFindingGlobalSinkState::PathFindingGlobalSinkState(ClientContext &context, const PhysicalPathFinding &op)
    : context_(context) {
	global_pairs = make_uniq<ColumnDataCollection>(context, op.children[0].get().GetTypes());

	global_pairs->InitializeScan(global_scan_state);
	result_scan_idx = 0;
	next_batch_index = 0;
	use_global_deduplication = false;
	global_dedupe_results_initialized = false;

	child = 0;
	mode = op.mode;
	path_finding_mode = ParsePathFindingOperatorMode(mode);
	search_orientation = PathFindingSearchOrientation::FORWARD;
	auto &scheduler = TaskScheduler::GetScheduler(context);
	num_threads = scheduler.NumberOfThreads();
}

void PathFindingGlobalSinkState::Sink(DataChunk &input, PathFindingLocalSinkState &lstate) {
	if (child == 0) {
		// CSR phase
		auto duckpgq_state = GetDuckPGQState(context_);
		csr_id = input.GetValue(0, 0).GetValue<int64_t>();
		csr = duckpgq_state->GetCSR(csr_id);
	} else {
		// path-finding phase
		lstate.Sink(input, child);
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
	if (gstate.child == 0) {
		return SinkCombineResultType::FINISHED;
	}
	gstate.global_pairs->Combine(lstate.local_pairs);
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

SinkFinalizeType PhysicalPathFinding::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                               OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PathFindingGlobalSinkState>();
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
		AppendOperatorPhaseTiming(context.client, "dedupe_scatter", pf_sink.num_threads, output_to_search.size(), 0,
		                          0, scatter_ms, output_to_search.capacity() * sizeof(idx_t));

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
	D_ASSERT(children.size() == 2);
	if (meta_pipeline.HasRecursiveCTE()) {
		throw NotImplementedException("Path Finding is not supported in recursive CTEs yet");
	}

	// becomes a source after both children fully sink their data
	meta_pipeline.GetState().SetPipelineSource(current, *this);

	// Create one child meta pipeline that will hold the LHS and RHS pipelines
	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);

	// Build out LHS
	auto lhs_pipeline = child_meta_pipeline.GetBasePipeline();
	children[1].get().BuildPipelines(*lhs_pipeline, child_meta_pipeline);

	// Build out RHS
	auto &rhs_pipeline = child_meta_pipeline.CreatePipeline();
	children[0].get().BuildPipelines(rhs_pipeline, child_meta_pipeline);

	// Despite having the same sink, RHS and everything created after it need
	// their own (same) PipelineFinishEvent
	child_meta_pipeline.AddFinishEvent(rhs_pipeline);
}

} // namespace duckdb
