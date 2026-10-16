#include "duckpgq/core/operator/physical_factorized_group_join.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/builtin_function_lookup.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

unique_ptr<BoundAggregateExpression> BindFactorizedAggregate(ClientContext &context, const string &name,
                                                             vector<unique_ptr<Expression>> children) {
	vector<LogicalType> types;
	for (auto &child : children) {
		types.push_back(child->GetReturnType());
	}
	auto function = TryGetBuiltinAggregateFunction(context, Identifier(name), types);
	if (!function) {
		throw InternalException("Factorized join: could not bind aggregate %s", name);
	}
	FunctionBinder binder(context);
	return binder.BindAggregateFunction(std::move(function), std::move(children));
}

//===--------------------------------------------------------------------===//
// Sink helper: one per input, both feeding the shared hash table
//===--------------------------------------------------------------------===//

class PhysicalFactorizedGroupJoinSink : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalFactorizedGroupJoinSink(PhysicalPlan &physical_plan, PhysicalFactorizedGroupJoin &op_p,
	                                PhysicalOperator &input, idx_t side_p)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {}, input.estimated_cardinality), op(op_p),
	      side(side_p) {
		children.push_back(input);
	}

	PhysicalFactorizedGroupJoin &op;
	idx_t side;

public:
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	string GetName() const override {
		return side == 0 ? "FACTORIZED_GROUP_JOIN_LEFT" : "FACTORIZED_GROUP_JOIN_RIGHT";
	}
};

struct FactorizedGroupJoinGlobalSinkState : public GlobalSinkState {
	explicit FactorizedGroupJoinGlobalSinkState(shared_ptr<FactorizedGroupJoinBuildState> build_p)
	    : build(std::move(build_p)) {
	}
	shared_ptr<FactorizedGroupJoinBuildState> build;
};

struct FactorizedGroupJoinLocalSinkState : public LocalSinkState {
	FactorizedGroupJoinLocalSinkState(ExecutionContext &context, const PhysicalFactorizedGroupJoin &op, idx_t side)
	    : key_executor(context.client), weight_executor(context.client), sel(STANDARD_VECTOR_SIZE) {
		if (!op.UsesDirectory()) {
			radix_state = op.radix_table->GetLocalSinkState(context);
		}
		for (auto &key : op.keys[side]) {
			key_executor.AddExpression(*key);
		}
		if (op.weights[side]) {
			weight_executor.AddExpression(*op.weights[side]);
		}
		keys.Initialize(Allocator::Get(context.client), op.key_types);
		payload.Initialize(Allocator::Get(context.client), op.payload_types);
	}

	ExpressionExecutor key_executor;
	ExpressionExecutor weight_executor;
	DataChunk keys;
	DataChunk payload;
	unique_ptr<LocalSinkState> radix_state;
	SelectionVector sel;
	vector<UnifiedVectorFormat> key_formats;
	//! Directory mode: this thread's private counters (hubs receive many increments, shared counters would contend
	//! on them), allocated on the first chunk and merged into the shared directory on Combine
	unique_ptr<int64_t[]> directory;
	//! Range of directory entries this thread touched, so Combine only merges that part
	idx_t touched_begin = 0;
	idx_t touched_end = 0;
};

//! Rows with a NULL key never match; returns how many rows remain and fills `sel` with them if any were dropped
static idx_t SelectNonNullKeys(DataChunk &keys, SelectionVector &sel, vector<UnifiedVectorFormat> &formats) {
	idx_t count = keys.size();
	formats.resize(keys.ColumnCount());
	bool any_null = false;
	for (idx_t col = 0; col < keys.ColumnCount(); col++) {
		keys.data[col].ToUnifiedFormat(formats[col]);
		if (!formats[col].validity.CannotHaveNull()) {
			any_null = true;
		}
	}
	if (!any_null) {
		return count;
	}
	idx_t result = 0;
	for (idx_t i = 0; i < count; i++) {
		bool valid = true;
		for (auto &format : formats) {
			if (!format.validity.RowIsValid(format.sel->get_index(i))) {
				valid = false;
				break;
			}
		}
		if (valid) {
			sel.set_index(result++, i);
		}
	}
	return result;
}

//! The directory covers the statistics bounds of both sides, so this means the statistics were wrong. Fail the query
//! rather than count wrongly, but do not invalidate the database over it.
static void ThrowDirectoryRange(int64_t key) {
	throw OutOfRangeException("FACTORIZED_GROUP_JOIN: key %lld lies outside of the range given by the column "
	                          "statistics; disable the directory with SET duckpgq_factorized_join_directory = false",
	                          key);
}

//! Adds the weight of every row to the counter of its key; NULL keys never match and are skipped
template <class KEY>
static void SinkIntoDirectory(Vector &keys, optional_ptr<Vector> weights, idx_t count,
                              FactorizedGroupJoinLocalSinkState &lstate, int64_t directory_min, idx_t directory_range) {
	auto directory = lstate.directory.get();
	auto touched_begin = lstate.touched_begin;
	auto touched_end = lstate.touched_end;
	auto key_values = keys.Values<KEY>();
	if (!weights) {
		for (auto entry : key_values) {
			if (!entry.IsValid()) {
				continue;
			}
			auto index = static_cast<int64_t>(entry.GetValue()) - directory_min;
			if (index < 0 || static_cast<idx_t>(index) >= directory_range) {
				ThrowDirectoryRange(static_cast<int64_t>(entry.GetValue()));
			}
			directory[index]++;
			touched_begin = MinValue<idx_t>(touched_begin, index);
			touched_end = MaxValue<idx_t>(touched_end, index + 1);
		}
		lstate.touched_begin = touched_begin;
		lstate.touched_end = touched_end;
		return;
	}
	auto weight_values = weights->Values<int64_t>();
	for (idx_t i = 0; i < count; i++) {
		auto key = key_values[i];
		auto weight = weight_values[i];
		if (!key.IsValid() || !weight.IsValid()) {
			continue;
		}
		auto index = static_cast<int64_t>(key.GetValue()) - directory_min;
		if (index < 0 || static_cast<idx_t>(index) >= directory_range) {
			ThrowDirectoryRange(static_cast<int64_t>(key.GetValue()));
		}
		// A weighted multiplicity is a partial path count, so it only overflows when the final BIGINT count would
		if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(directory[index], weight.GetValue(),
		                                                          directory[index])) {
			throw OutOfRangeException("FACTORIZED_GROUP_JOIN: multiplicity overflows BIGINT");
		}
		touched_begin = MinValue<idx_t>(touched_begin, index);
		touched_end = MaxValue<idx_t>(touched_end, index + 1);
	}
	lstate.touched_begin = touched_begin;
	lstate.touched_end = touched_end;
}

static void SinkIntoDirectory(Vector &keys, optional_ptr<Vector> weights, idx_t count,
                              FactorizedGroupJoinLocalSinkState &lstate, int64_t directory_min, idx_t directory_range) {
	switch (keys.GetType().InternalType()) {
	case PhysicalType::INT8:
		return SinkIntoDirectory<int8_t>(keys, weights, count, lstate, directory_min, directory_range);
	case PhysicalType::INT16:
		return SinkIntoDirectory<int16_t>(keys, weights, count, lstate, directory_min, directory_range);
	case PhysicalType::INT32:
		return SinkIntoDirectory<int32_t>(keys, weights, count, lstate, directory_min, directory_range);
	case PhysicalType::INT64:
		return SinkIntoDirectory<int64_t>(keys, weights, count, lstate, directory_min, directory_range);
	case PhysicalType::UINT8:
		return SinkIntoDirectory<uint8_t>(keys, weights, count, lstate, directory_min, directory_range);
	case PhysicalType::UINT16:
		return SinkIntoDirectory<uint16_t>(keys, weights, count, lstate, directory_min, directory_range);
	case PhysicalType::UINT32:
		return SinkIntoDirectory<uint32_t>(keys, weights, count, lstate, directory_min, directory_range);
	default:
		throw InternalException("FACTORIZED_GROUP_JOIN: unsupported directory key type %s", keys.GetType().ToString());
	}
}

unique_ptr<GlobalSinkState> PhysicalFactorizedGroupJoinSink::GetGlobalSinkState(ClientContext &context) const {
	// Make sure the shared table exists before either side starts sinking
	auto build = op.AcquireBuildState();
	if (op.UsesDirectory()) {
		op.GetDirectory(side, *build);
	} else {
		op.GetRadixSinkState(context, *build);
	}
	return make_uniq<FactorizedGroupJoinGlobalSinkState>(std::move(build));
}

unique_ptr<LocalSinkState> PhysicalFactorizedGroupJoinSink::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<FactorizedGroupJoinLocalSinkState>(context, op, side);
}

SinkResultType PhysicalFactorizedGroupJoinSink::Sink(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<FactorizedGroupJoinGlobalSinkState>();
	auto &lstate = input.local_state.Cast<FactorizedGroupJoinLocalSinkState>();

	lstate.keys.Reset();
	lstate.key_executor.Execute(chunk, lstate.keys);
	if (op.UsesDirectory()) {
		if (!lstate.directory) {
			// Value-initialized to zero
			lstate.directory = unique_ptr<int64_t[]>(new int64_t[op.directory_range]());
			lstate.touched_begin = op.directory_range;
			lstate.touched_end = 0;
		}
		optional_ptr<Vector> weight;
		if (op.weights[side]) {
			lstate.payload.Reset();
			weight = lstate.payload.data[op.weight_payload_index[side].GetIndex()];
			lstate.weight_executor.ExecuteExpression(chunk, *weight);
		}
		SinkIntoDirectory(lstate.keys.data[0], weight, chunk.size(), lstate, op.directory_min, op.directory_range);
		return SinkResultType::NEED_MORE_INPUT;
	}
	lstate.payload.Reset();
	for (idx_t other = 0; other < PhysicalFactorizedGroupJoin::SIDES; other++) {
		if (!op.weight_payload_index[other].IsValid()) {
			continue;
		}
		auto &column = lstate.payload.data[op.weight_payload_index[other].GetIndex()];
		if (other == side) {
			lstate.weight_executor.ExecuteExpression(chunk, column);
		} else {
			// Not updated by this side, only there to keep the payload layout complete
			column.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(column, count_t(chunk.size()));
		}
	}
	lstate.payload.SetCardinalityUnsafe(chunk.size());

	auto count = SelectNonNullKeys(lstate.keys, lstate.sel, lstate.key_formats);
	if (count == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	if (count < chunk.size()) {
		lstate.keys.Slice(lstate.sel, count);
		lstate.payload.Slice(lstate.sel, count);
	}
	OperatorSinkInput radix_input {*gstate.build->radix_state, *lstate.radix_state, input.interrupt_state};
	op.radix_table->Sink(context, lstate.keys, radix_input, lstate.payload, op.aggregate_filter[side]);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalFactorizedGroupJoinSink::Combine(ExecutionContext &context,
                                                               OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<FactorizedGroupJoinGlobalSinkState>();
	auto &lstate = input.local_state.Cast<FactorizedGroupJoinLocalSinkState>();
	if (op.UsesDirectory()) {
		if (!lstate.directory) {
			return SinkCombineResultType::FINISHED;
		}
		// One sequential pass per thread over the part it touched; the shared counters only see as many adds as
		// there are threads
		auto shared = gstate.build->directory[side].get();
		auto local = lstate.directory.get();
		for (idx_t i = lstate.touched_begin; i < lstate.touched_end; i++) {
			if (local[i] == 0) {
				continue;
			}
			auto previous = shared[i].fetch_add(local[i], std::memory_order_relaxed);
			int64_t sum;
			if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(previous, local[i], sum)) {
				throw OutOfRangeException("FACTORIZED_GROUP_JOIN: multiplicity overflows BIGINT");
			}
		}
		return SinkCombineResultType::FINISHED;
	}
	op.radix_table->Combine(context, *gstate.build->radix_state, *lstate.radix_state);
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalFactorizedGroupJoinSink::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                           OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<FactorizedGroupJoinGlobalSinkState>();
	// The table is complete once both sides have finished their Combine
	if (++gstate.build->finalized_sides == PhysicalFactorizedGroupJoin::SIDES && !op.UsesDirectory()) {
		op.radix_table->Finalize(context, *gstate.build->radix_state);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// PhysicalFactorizedGroupJoin
//===--------------------------------------------------------------------===//

PhysicalFactorizedGroupJoin::PhysicalFactorizedGroupJoin(
    PhysicalPlan &physical_plan, ClientContext &context, LogicalOperator &op, PhysicalOperator &left,
    PhysicalOperator &right, vector<vector<unique_ptr<Expression>>> keys_p, vector<unique_ptr<Expression>> weights_p,
    const Value &directory_min_p, idx_t directory_range_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, estimated_cardinality),
      keys(std::move(keys_p)), weights(std::move(weights_p)), directory_range(directory_range_p) {
	if (directory_range > 0) {
		directory_min = directory_min_p.GetValue<int64_t>();
	}
	vector<unique_ptr<Expression>> groups;
	for (idx_t i = 0; i < keys[0].size(); i++) {
		key_types.push_back(keys[0][i]->GetReturnType());
		groups.push_back(make_uniq<BoundReferenceExpression>(key_types[i], i));
		grouping_set.insert(ProjectionIndex(i));
	}
	vector<unique_ptr<Expression>> aggregates;
	for (idx_t side = 0; side < SIDES; side++) {
		if (weights[side]) {
			auto &weight_type = weights[side]->GetReturnType();
			weight_payload_index[side] = payload_types.size();
			vector<unique_ptr<Expression>> children;
			children.push_back(make_uniq<BoundReferenceExpression>(weight_type, payload_types.size()));
			payload_types.push_back(weight_type);
			aggregates.push_back(BindFactorizedAggregate(context, "sum", std::move(children)));
		} else {
			aggregates.push_back(BindFactorizedAggregate(context, "count_star", {}));
		}
		aggregate_filter[side].push_back(side);
	}
	scan_types = key_types;
	for (auto &aggregate : aggregates) {
		scan_types.push_back(aggregate->GetReturnType());
	}
	if (scan_types != types) {
		throw InternalException("PhysicalFactorizedGroupJoin: hash table layout does not match the operator types");
	}
	grouped_aggregate_data.InitializeGroupby(std::move(groups), std::move(aggregates), {});
	radix_table = make_uniq<RadixPartitionedHashTable>(grouping_set, grouped_aggregate_data,
	                                                   TupleDataValidityType::CANNOT_HAVE_NULL_VALUES);
	// The sinks are this operator's children so that EXPLAIN and the profiler account for the build work
	sinks[0] = &physical_plan.Make<PhysicalFactorizedGroupJoinSink>(*this, left, 0);
	sinks[1] = &physical_plan.Make<PhysicalFactorizedGroupJoinSink>(*this, right, 1);
	for (idx_t side = 0; side < SIDES; side++) {
		children.push_back(*sinks[side]);
	}
}

string PhysicalFactorizedGroupJoin::GetName() const {
	return "FACTORIZED_GROUP_JOIN";
}

InsertionOrderPreservingMap<string> PhysicalFactorizedGroupJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string conditions;
	for (idx_t i = 0; i < keys[0].size(); i++) {
		if (i > 0) {
			conditions += "\n";
		}
		conditions += keys[0][i]->ToString() + " = " + keys[1][i]->ToString();
	}
	result["Conditions"] = conditions;
	for (idx_t side = 0; side < SIDES; side++) {
		if (weights[side]) {
			result[side == 0 ? "Left Weight" : "Right Weight"] = weights[side]->ToString();
		}
	}
	result["Lookup"] = UsesDirectory() ? "DIRECTORY" : "HASH";
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

shared_ptr<FactorizedGroupJoinBuildState> PhysicalFactorizedGroupJoin::AcquireBuildState() const {
	lock_guard<mutex> guard(build_lock);
	if (!build_state || build_state->consumed) {
		build_state = make_shared_ptr<FactorizedGroupJoinBuildState>();
	}
	return build_state;
}

shared_ptr<FactorizedGroupJoinBuildState> PhysicalFactorizedGroupJoin::ConsumeBuildState() const {
	lock_guard<mutex> guard(build_lock);
	if (!build_state) {
		throw InternalException("PhysicalFactorizedGroupJoin: scanning before either side was built");
	}
	build_state->consumed = true;
	return build_state;
}

GlobalSinkState &PhysicalFactorizedGroupJoin::GetRadixSinkState(ClientContext &context,
                                                                FactorizedGroupJoinBuildState &build) const {
	lock_guard<mutex> guard(build.lock);
	if (!build.radix_state) {
		build.radix_state = radix_table->GetGlobalSinkState(context);
	}
	return *build.radix_state;
}

atomic<int64_t> *PhysicalFactorizedGroupJoin::GetDirectory(idx_t side, FactorizedGroupJoinBuildState &build) const {
	lock_guard<mutex> guard(build.lock);
	if (!build.directory[side]) {
		// Value-initialized to zero
		build.directory[side] = unique_ptr<atomic<int64_t>[]>(new atomic<int64_t>[directory_range]());
	}
	return build.directory[side].get();
}

//===--------------------------------------------------------------------===//
// Pipelines
//===--------------------------------------------------------------------===//

void PhysicalFactorizedGroupJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	for (idx_t side = 0; side < SIDES; side++) {
		sinks[side]->sink_state.reset();
	}
	{
		lock_guard<mutex> guard(build_lock);
		build_state.reset();
	}

	// This operator scans the finished table; both inputs are built first and may run concurrently, every
	// thread of either side has its own local hash table that is merged into the shared one on Combine
	meta_pipeline.GetState().SetPipelineSource(current, *this);
	for (idx_t side = 0; side < SIDES; side++) {
		auto &child_meta = meta_pipeline.CreateChildMetaPipeline(current, *sinks[side], MetaPipelineType::REGULAR);
		child_meta.Build(sinks[side]->children[0].get());
	}
}

vector<const_reference<PhysicalOperator>> PhysicalFactorizedGroupJoin::GetSources() const {
	return {*this};
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

//! Keys handed out to scanning threads in slabs of this many directory entries
static constexpr idx_t DIRECTORY_SLAB = 1 << 16;

struct FactorizedGroupJoinGlobalSourceState : public GlobalSourceState {
	FactorizedGroupJoinGlobalSourceState(ClientContext &context, const PhysicalFactorizedGroupJoin &op)
	    : build(op.ConsumeBuildState()) {
		if (op.UsesDirectory()) {
			max_threads = (op.directory_range + DIRECTORY_SLAB - 1) / DIRECTORY_SLAB;
		} else {
			radix_source = op.radix_table->GetGlobalSourceState(context);
			max_threads = op.radix_table->MaxThreads(op.GetRadixSinkState(context, *build));
		}
	}

	shared_ptr<FactorizedGroupJoinBuildState> build;
	unique_ptr<GlobalSourceState> radix_source;
	idx_t max_threads;
	//! Directory mode: next slab to hand out
	atomic<idx_t> next_slab {0};

	idx_t MaxThreads() override {
		return max_threads;
	}
};

struct FactorizedGroupJoinLocalSourceState : public LocalSourceState {
	FactorizedGroupJoinLocalSourceState(ExecutionContext &context, const PhysicalFactorizedGroupJoin &op)
	    : sel(STANDARD_VECTOR_SIZE) {
		if (!op.UsesDirectory()) {
			radix_source = op.radix_table->GetLocalSourceState(context);
			scan_chunk.Initialize(Allocator::Get(context.client), op.scan_types);
		}
	}

	unique_ptr<LocalSourceState> radix_source;
	DataChunk scan_chunk;
	SelectionVector sel;
	vector<bool> matched;
	//! Directory mode: the slab this thread is scanning
	idx_t slab_position = 0;
	idx_t slab_end = 0;
};

unique_ptr<GlobalSourceState> PhysicalFactorizedGroupJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<FactorizedGroupJoinGlobalSourceState>(context, *this);
}

unique_ptr<LocalSourceState> PhysicalFactorizedGroupJoin::GetLocalSourceState(ExecutionContext &context,
                                                                              GlobalSourceState &gstate) const {
	return make_uniq<FactorizedGroupJoinLocalSourceState>(context, *this);
}

//! Marks the rows whose multiplicity is NULL (no rows on that side) or zero as unmatched
template <class T>
static void MarkUnmatched(Vector &multiplicity, vector<bool> &matched) {
	auto values = multiplicity.Values<T>();
	for (auto entry : values) {
		if (!entry.IsValid() || entry.GetValue() == T(0)) {
			matched[entry.GetIndex()] = false;
		}
	}
}

//! Keys that only one side produced are not part of the join result
static idx_t SelectMatched(DataChunk &scan_chunk, idx_t key_count, SelectionVector &sel, vector<bool> &matched) {
	auto count = scan_chunk.size();
	matched.assign(count, true);
	for (idx_t side = 0; side < PhysicalFactorizedGroupJoin::SIDES; side++) {
		auto &multiplicity = scan_chunk.data[key_count + side];
		if (multiplicity.GetType().id() == LogicalTypeId::HUGEINT) {
			MarkUnmatched<hugeint_t>(multiplicity, matched);
		} else {
			MarkUnmatched<int64_t>(multiplicity, matched);
		}
	}
	idx_t result = 0;
	for (idx_t i = 0; i < count; i++) {
		if (matched[i]) {
			sel.set_index(result++, i);
		}
	}
	return result;
}

//! Emits the keys of one slab whose counters are set on both sides
template <class KEY>
static void ScanDirectory(const PhysicalFactorizedGroupJoin &op, FactorizedGroupJoinGlobalSourceState &gstate,
                          FactorizedGroupJoinLocalSourceState &lstate, DataChunk &chunk) {
	auto left = gstate.build->directory[0].get();
	auto right = gstate.build->directory[1].get();
	auto keys = FlatVector::GetDataMutable<KEY>(chunk.data[0]);
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		if (lstate.slab_position >= lstate.slab_end) {
			auto slab = gstate.next_slab.fetch_add(1);
			lstate.slab_position = slab * DIRECTORY_SLAB;
			if (lstate.slab_position >= op.directory_range) {
				lstate.slab_position = lstate.slab_end = op.directory_range;
				break;
			}
			lstate.slab_end = MinValue<idx_t>(lstate.slab_position + DIRECTORY_SLAB, op.directory_range);
		}
		for (; lstate.slab_position < lstate.slab_end && count < STANDARD_VECTOR_SIZE; lstate.slab_position++) {
			auto index = lstate.slab_position;
			auto l = left ? left[index].load(std::memory_order_relaxed) : 0;
			if (l == 0) {
				continue;
			}
			auto r = right ? right[index].load(std::memory_order_relaxed) : 0;
			if (r == 0) {
				continue;
			}
			keys[count] = static_cast<KEY>(op.directory_min + static_cast<int64_t>(index));
			for (idx_t side = 0; side < PhysicalFactorizedGroupJoin::SIDES; side++) {
				auto value = side == 0 ? l : r;
				auto &column = chunk.data[1 + side];
				if (column.GetType().id() == LogicalTypeId::HUGEINT) {
					FlatVector::GetDataMutable<hugeint_t>(column)[count] = hugeint_t(value);
				} else {
					FlatVector::GetDataMutable<int64_t>(column)[count] = value;
				}
			}
			count++;
		}
	}
	chunk.SetChildCardinality(count);
}

static void ScanDirectory(const PhysicalFactorizedGroupJoin &op, FactorizedGroupJoinGlobalSourceState &gstate,
                          FactorizedGroupJoinLocalSourceState &lstate, DataChunk &chunk) {
	switch (chunk.data[0].GetType().InternalType()) {
	case PhysicalType::INT8:
		return ScanDirectory<int8_t>(op, gstate, lstate, chunk);
	case PhysicalType::INT16:
		return ScanDirectory<int16_t>(op, gstate, lstate, chunk);
	case PhysicalType::INT32:
		return ScanDirectory<int32_t>(op, gstate, lstate, chunk);
	case PhysicalType::INT64:
		return ScanDirectory<int64_t>(op, gstate, lstate, chunk);
	case PhysicalType::UINT8:
		return ScanDirectory<uint8_t>(op, gstate, lstate, chunk);
	case PhysicalType::UINT16:
		return ScanDirectory<uint16_t>(op, gstate, lstate, chunk);
	case PhysicalType::UINT32:
		return ScanDirectory<uint32_t>(op, gstate, lstate, chunk);
	default:
		throw InternalException("FACTORIZED_GROUP_JOIN: unsupported directory key type");
	}
}

SourceResultType PhysicalFactorizedGroupJoin::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                              OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<FactorizedGroupJoinGlobalSourceState>();
	auto &lstate = input.local_state.Cast<FactorizedGroupJoinLocalSourceState>();
	if (UsesDirectory()) {
		ScanDirectory(*this, gstate, lstate, chunk);
		return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
	}
	auto &radix_sink = GetRadixSinkState(context.client, *gstate.build);
	while (true) {
		lstate.scan_chunk.Reset();
		OperatorSourceInput radix_input {*gstate.radix_source, *lstate.radix_source, input.interrupt_state};
		auto result = radix_table->GetData(context, lstate.scan_chunk, radix_sink, radix_input);
		if (result == SourceResultType::BLOCKED) {
			return result;
		}
		if (lstate.scan_chunk.size() == 0) {
			return SourceResultType::FINISHED;
		}
		auto count = SelectMatched(lstate.scan_chunk, key_types.size(), lstate.sel, lstate.matched);
		if (count == 0) {
			continue;
		}
		chunk.Slice(lstate.scan_chunk, lstate.sel, count);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
}

} // namespace duckdb
