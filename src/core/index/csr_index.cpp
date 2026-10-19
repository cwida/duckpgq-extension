#include "duckpgq/core/index/csr_index.hpp"

#include "duckpgq/core/operator/partitioned_csr_cache.hpp"
#include "duckpgq/core/option/duckpgq_option.hpp"
#include "duckpgq/core/utils/csr_build.hpp"
#include "duckpgq/parser/property_graph_table.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/index/index_lock.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

#include <algorithm>
#include <bitset>
#include <cstring>
#include <thread>

namespace duckdb {

namespace {

bool ReadUndirectedOption(const case_insensitive_map_t<Value> &options) {
	auto entry = options.find(CSRIndex::UNDIRECTED_OPTION);
	if (entry == options.end() || entry->second.IsNull()) {
		return false;
	}
	return entry->second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
}

//! Appends the BIGINT-cast keys and the row ids of a chunk to an edge list. Rows with a NULL key have no endpoint
//! and are skipped, like the endpoint join of the on-the-fly build drops them.
void CollectEdges(ClientContext &context, DataChunk &key_chunk, Vector &row_id_vector, idx_t count,
                  CSRIndexEdgeList &list) {
	if (key_chunk.ColumnCount() != 2) {
		throw BinderException("A CSR index needs exactly two key columns: the edge source and destination");
	}
	Vector src_cast(LogicalType::BIGINT);
	Vector dst_cast(LogicalType::BIGINT);
	VectorOperations::Cast(context, key_chunk.data[0], src_cast, count);
	VectorOperations::Cast(context, key_chunk.data[1], dst_cast, count);
	UnifiedVectorFormat src_format;
	UnifiedVectorFormat dst_format;
	UnifiedVectorFormat row_format;
	src_cast.ToUnifiedFormat(count, src_format);
	dst_cast.ToUnifiedFormat(count, dst_format);
	row_id_vector.ToUnifiedFormat(count, row_format);
	auto src_data = UnifiedVectorFormat::GetData<int64_t>(src_format);
	auto dst_data = UnifiedVectorFormat::GetData<int64_t>(dst_format);
	auto row_data = UnifiedVectorFormat::GetData<row_t>(row_format);
	for (idx_t row = 0; row < count; row++) {
		auto src_idx = src_format.sel->get_index(row);
		auto dst_idx = dst_format.sel->get_index(row);
		if (!src_format.validity.RowIsValid(src_idx) || !dst_format.validity.RowIsValid(dst_idx)) {
			continue;
		}
		auto src = src_data[src_idx];
		auto dst = dst_data[dst_idx];
		list.src_keys.push_back(src);
		list.dst_keys.push_back(dst);
		list.row_ids.push_back(row_data[row_format.sel->get_index(row)]);
		list.min_key = std::min(list.min_key, std::min(src, dst));
		list.max_key = std::max(list.max_key, std::max(src, dst));
	}
}

//! Runs fn(begin, end, thread_idx) over [0, count) on up to thread_count threads and rethrows the first exception
template <class FUNC>
void ParallelFor(idx_t count, idx_t thread_count, FUNC &&fn) {
	thread_count = std::max<idx_t>(1, std::min(thread_count, count));
	if (thread_count == 1) {
		fn(idx_t(0), count, idx_t(0));
		return;
	}
	auto chunk = (count + thread_count - 1) / thread_count;
	vector<std::thread> threads;
	vector<std::exception_ptr> errors(thread_count);
	for (idx_t thread_idx = 0; thread_idx < thread_count; thread_idx++) {
		auto begin = thread_idx * chunk;
		auto end = std::min(count, begin + chunk);
		if (begin >= end) {
			break;
		}
		threads.emplace_back([&fn, &errors, thread_idx, begin, end]() {
			try {
				fn(begin, end, thread_idx);
			} catch (...) {
				errors[thread_idx] = std::current_exception();
			}
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	for (auto &error : errors) {
		if (error) {
			std::rethrow_exception(error);
		}
	}
}

inline idx_t PopCount(uint64_t word) {
	return std::bitset<64>(word).count();
}

//! A contiguous range of one edge list, the unit of parallel work over the collected edges
struct EdgeSlice {
	idx_t list_idx;
	idx_t begin;
	idx_t end;
	//! Position of the slice's first edge in the flat edge arrays
	idx_t offset;
};

constexpr idx_t EDGE_SLICE_SIZE = 65536;

vector<EdgeSlice> MakeEdgeSlices(const vector<CSRIndexEdgeList> &lists, idx_t directions) {
	vector<EdgeSlice> slices;
	idx_t offset = 0;
	for (idx_t list_idx = 0; list_idx < lists.size(); list_idx++) {
		auto size = lists[list_idx].Size();
		for (idx_t begin = 0; begin < size; begin += EDGE_SLICE_SIZE) {
			auto end = std::min(size, begin + EDGE_SLICE_SIZE);
			slices.push_back({list_idx, begin, end, offset});
			offset += (end - begin) * directions;
		}
	}
	return slices;
}

//! Builds the dictionary: a presence bitmap when the key space is compact, a sorted key array otherwise
shared_ptr<CSRIndexDictionary> BuildDictionary(const vector<CSRIndexEdgeList> &lists, idx_t total_edges,
                                               int64_t min_key, int64_t max_key, idx_t thread_count) {
	auto dictionary = make_shared_ptr<CSRIndexDictionary>();
	if (total_edges == 0) {
		return dictionary;
	}
	auto range = static_cast<uint64_t>(max_key) - static_cast<uint64_t>(min_key) + 1;
	// A bitmap costs range / 8 bytes; use it unless the keys are far sparser than the edges that reference them
	bool dense = range <= 8 * static_cast<uint64_t>(total_edges) * 2 + 4096;
	auto slices = MakeEdgeSlices(lists, 1);

	if (dense) {
		dictionary->dense = true;
		dictionary->min_key = min_key;
		auto word_count = (range + 63) / 64;
		unique_ptr<std::atomic<uint64_t>[]> bits(new std::atomic<uint64_t>[word_count]());
		ParallelFor(slices.size(), thread_count, [&](idx_t begin, idx_t end, idx_t) {
			for (idx_t slice_idx = begin; slice_idx < end; slice_idx++) {
				auto &slice = slices[slice_idx];
				auto &list = lists[slice.list_idx];
				for (idx_t edge_idx = slice.begin; edge_idx < slice.end; edge_idx++) {
					auto src = static_cast<uint64_t>(list.src_keys[edge_idx]) - static_cast<uint64_t>(min_key);
					auto dst = static_cast<uint64_t>(list.dst_keys[edge_idx]) - static_cast<uint64_t>(min_key);
					bits[src >> 6].fetch_or(uint64_t(1) << (src & 63), std::memory_order_relaxed);
					bits[dst >> 6].fetch_or(uint64_t(1) << (dst & 63), std::memory_order_relaxed);
				}
			}
		});
		dictionary->bitmap.resize(word_count);
		dictionary->word_rank.resize(word_count);
		idx_t rank = 0;
		for (idx_t word_idx = 0; word_idx < word_count; word_idx++) {
			auto word = bits[word_idx].load(std::memory_order_relaxed);
			dictionary->bitmap[word_idx] = word;
			dictionary->word_rank[word_idx] = NumericCast<uint32_t>(rank);
			rank += PopCount(word);
		}
		dictionary->count = rank;
		if (rank == range) {
			// Every key in the range is a vertex: ids are offsets, the bitmap is not needed
			dictionary->identity = true;
			dictionary->bitmap.clear();
			dictionary->word_rank.clear();
		}
		return dictionary;
	}

	// Sparse keys: bucket by the top bits of the key offset so that the buckets are ordered, sort and deduplicate
	// each bucket independently, then concatenate; no merge pass over all keys is needed
	constexpr idx_t KEY_BUCKET_BITS = 10;
	constexpr idx_t KEY_BUCKETS = idx_t(1) << KEY_BUCKET_BITS;
	idx_t range_bits = 1;
	while (range_bits < 64 && (uint64_t(1) << range_bits) < range) {
		range_bits++;
	}
	auto bucket_shift = range_bits > KEY_BUCKET_BITS ? range_bits - KEY_BUCKET_BITS : 0;
	vector<vector<vector<int64_t>>> thread_buckets(thread_count, vector<vector<int64_t>>(KEY_BUCKETS));
	ParallelFor(slices.size(), thread_count, [&](idx_t begin, idx_t end, idx_t thread_idx) {
		auto &buckets = thread_buckets[thread_idx];
		for (idx_t slice_idx = begin; slice_idx < end; slice_idx++) {
			auto &slice = slices[slice_idx];
			auto &list = lists[slice.list_idx];
			for (idx_t edge_idx = slice.begin; edge_idx < slice.end; edge_idx++) {
				auto src = list.src_keys[edge_idx];
				auto dst = list.dst_keys[edge_idx];
				auto src_offset = static_cast<uint64_t>(src) - static_cast<uint64_t>(min_key);
				auto dst_offset = static_cast<uint64_t>(dst) - static_cast<uint64_t>(min_key);
				buckets[std::min<idx_t>(KEY_BUCKETS - 1, src_offset >> bucket_shift)].push_back(src);
				buckets[std::min<idx_t>(KEY_BUCKETS - 1, dst_offset >> bucket_shift)].push_back(dst);
			}
		}
	});
	vector<vector<int64_t>> bucket_keys(KEY_BUCKETS);
	ParallelFor(KEY_BUCKETS, thread_count, [&](idx_t begin, idx_t end, idx_t) {
		for (idx_t bucket_idx = begin; bucket_idx < end; bucket_idx++) {
			auto &keys = bucket_keys[bucket_idx];
			for (auto &buckets : thread_buckets) {
				auto &bucket = buckets[bucket_idx];
				keys.insert(keys.end(), bucket.begin(), bucket.end());
				bucket.clear();
				bucket.shrink_to_fit();
			}
			std::sort(keys.begin(), keys.end());
			keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
		}
	});
	vector<idx_t> bucket_offsets(KEY_BUCKETS + 1, 0);
	for (idx_t bucket_idx = 0; bucket_idx < KEY_BUCKETS; bucket_idx++) {
		bucket_offsets[bucket_idx + 1] = bucket_offsets[bucket_idx] + bucket_keys[bucket_idx].size();
	}
	vector<int64_t> keys(bucket_offsets[KEY_BUCKETS]);
	ParallelFor(KEY_BUCKETS, thread_count, [&](idx_t begin, idx_t end, idx_t) {
		for (idx_t bucket_idx = begin; bucket_idx < end; bucket_idx++) {
			std::copy(bucket_keys[bucket_idx].begin(), bucket_keys[bucket_idx].end(),
			          keys.begin() + NumericCast<int64_t>(bucket_offsets[bucket_idx]));
		}
	});
	dictionary->sorted_keys = std::move(keys);
	dictionary->count = dictionary->sorted_keys.size();
	return dictionary;
}

//----------------------------------------------------------------------------------------------------------------------
// CREATE INDEX build pipeline
//----------------------------------------------------------------------------------------------------------------------
struct CSRBuildBindData : public IndexBuildBindData {};

struct CSRBuildGlobalState : public IndexBuildGlobalState {
	explicit CSRBuildGlobalState(ClientContext &context_p, AttachedDatabase &db_p) : context(context_p), db(db_p) {
	}
	ClientContext &context;
	AttachedDatabase &db;
	unique_ptr<CSRIndex> index;
	mutex lock;
	vector<CSRIndexEdgeList> lists;
};

struct CSRBuildLocalState : public IndexBuildLocalState {
	explicit CSRBuildLocalState(ClientContext &context_p) : context(context_p) {
	}
	ClientContext &context;
	CSRIndexEdgeList list;
};

unique_ptr<IndexBuildBindData> CSRBuildBind(IndexBuildBindInput &input) {
	if (input.expressions.size() != 2) {
		throw BinderException("A CSR index needs exactly two key columns: the edge source and destination");
	}
	for (auto &expr : input.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			throw BinderException("CSR index keys must be plain columns, not expressions");
		}
		auto physical = expr->GetReturnType().InternalType();
		if (!expr->GetReturnType().IsIntegral() || physical == PhysicalType::UINT64 || physical == PhysicalType::INT128 ||
		    physical == PhysicalType::UINT128) {
			throw BinderException("CSR index keys must be integer columns that fit a BIGINT, got %s",
			                      expr->GetReturnType().ToString());
		}
	}
	return make_uniq<CSRBuildBindData>();
}

bool CSRBuildSort(IndexBuildSortInput &) {
	return false;
}

unique_ptr<IndexBuildGlobalState> CSRBuildGlobalInit(IndexBuildInitGlobalStateInput &input) {
	auto &storage = input.table.GetStorage();
	auto state = make_uniq<CSRBuildGlobalState>(input.context, storage.db);
	state->index = make_uniq<CSRIndex>(input.info.GetIndexName(), input.info.constraint_type, input.storage_ids,
	                                   TableIOManager::Get(storage), input.expressions, storage.db, input.info.options);
	return std::move(state);
}

unique_ptr<IndexBuildLocalState> CSRBuildLocalInit(IndexBuildInitLocalStateInput &input) {
	return make_uniq<CSRBuildLocalState>(input.context);
}

void CSRBuildSink(IndexBuildSinkInput &input, DataChunk &key_chunk, DataChunk &row_chunk) {
	auto &lstate = input.local_state.Cast<CSRBuildLocalState>();
	CollectEdges(lstate.context, key_chunk, row_chunk.data[0], key_chunk.size(), lstate.list);
}

void CSRBuildCombine(IndexBuildCombineInput &input) {
	auto &gstate = input.global_state.Cast<CSRBuildGlobalState>();
	auto &lstate = input.local_state.Cast<CSRBuildLocalState>();
	lock_guard<mutex> guard(gstate.lock);
	gstate.lists.push_back(std::move(lstate.list));
}

unique_ptr<BoundIndex> CSRBuildFinalize(IndexBuildFinalizeInput &input) {
	auto &gstate = input.global_state.Cast<CSRBuildGlobalState>();
	auto &transaction = DuckTransaction::Get(gstate.context, gstate.db);
	auto epoch = CSRIndex::BuildEpoch(gstate.context, gstate.lists, gstate.index->IsUndirected(),
	                                  transaction.view.visibility_bound);
	gstate.index->InstallEpoch(std::move(epoch));
	return std::move(gstate.index);
}

} // namespace

//----------------------------------------------------------------------------------------------------------------------
// CSRIndexDictionary
//----------------------------------------------------------------------------------------------------------------------
idx_t CSRIndexDictionary::Lookup(int64_t key) const {
	if (dense) {
		if (key < min_key) {
			return DConstants::INVALID_INDEX;
		}
		auto offset = static_cast<uint64_t>(key) - static_cast<uint64_t>(min_key);
		if (identity) {
			return offset < count ? offset : DConstants::INVALID_INDEX;
		}
		auto word_idx = offset >> 6;
		if (word_idx >= bitmap.size()) {
			return DConstants::INVALID_INDEX;
		}
		auto bit = offset & 63;
		auto word = bitmap[word_idx];
		if (!((word >> bit) & 1)) {
			return DConstants::INVALID_INDEX;
		}
		return word_rank[word_idx] + PopCount(word & ((uint64_t(1) << bit) - 1));
	}
	auto entry = std::lower_bound(sorted_keys.begin(), sorted_keys.end(), key);
	if (entry == sorted_keys.end() || *entry != key) {
		return DConstants::INVALID_INDEX;
	}
	return static_cast<idx_t>(entry - sorted_keys.begin());
}

int64_t CSRIndexDictionary::KeyOf(idx_t id) const {
	D_ASSERT(id < count);
	if (!dense) {
		return sorted_keys[id];
	}
	if (identity) {
		return min_key + NumericCast<int64_t>(id);
	}
	// The word whose rank range contains id, then the (id - rank)-th set bit of that word
	auto word_entry = std::upper_bound(word_rank.begin(), word_rank.end(), NumericCast<uint32_t>(id));
	auto word_idx = static_cast<idx_t>(word_entry - word_rank.begin()) - 1;
	auto word = bitmap[word_idx];
	auto remaining = id - word_rank[word_idx];
	for (idx_t bit = 0; bit < 64; bit++) {
		if ((word >> bit) & 1) {
			if (remaining == 0) {
				return min_key + NumericCast<int64_t>(word_idx * 64 + bit);
			}
			remaining--;
		}
	}
	throw InternalException("CSR index dictionary rank out of range");
}

//----------------------------------------------------------------------------------------------------------------------
// CSRIndex
//----------------------------------------------------------------------------------------------------------------------
CSRIndex::CSRIndex(const Identifier &name, IndexConstraintType constraint_type, const vector<column_t> &column_ids,
                   TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
                   AttachedDatabase &db, const case_insensitive_map_t<Value> &options)
    : BoundIndex(name, TYPE_NAME, constraint_type, column_ids, table_io_manager, unbound_expressions, db),
      undirected(ReadUndirectedOption(options)) {
	if (constraint_type != IndexConstraintType::NONE) {
		throw BinderException("A CSR index cannot enforce a constraint");
	}
}

IndexType CSRIndex::GetCSRIndexType() {
	IndexType index_type;
	index_type.name = TYPE_NAME;
	index_type.create_instance = CSRIndex::Create;
	index_type.build_bind = CSRBuildBind;
	index_type.build_sort = CSRBuildSort;
	index_type.build_global_init = CSRBuildGlobalInit;
	index_type.build_local_init = CSRBuildLocalInit;
	index_type.build_sink = CSRBuildSink;
	index_type.build_combine = CSRBuildCombine;
	index_type.build_finalize = CSRBuildFinalize;
	return index_type;
}

unique_ptr<BoundIndex> CSRIndex::Create(CreateIndexInput &input) {
	// Used when an unbound index is bound after a restart: the epoch is rebuilt from the table on first use
	return make_uniq<CSRIndex>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
	                           input.unbound_expressions, input.db, input.options);
}

shared_ptr<CSRIndexEpoch> CSRIndex::BuildEpoch(ClientContext &context, vector<CSRIndexEdgeList> &lists,
                                               bool undirected, VisibilityBound bound) {
	auto epoch = make_shared_ptr<CSRIndexEpoch>();
	epoch->bound = bound;
	auto thread_count = std::max<idx_t>(1, TaskScheduler::GetScheduler(context).NumberOfThreads());

	idx_t total_edges = 0;
	int64_t min_key = NumericLimits<int64_t>::Maximum();
	int64_t max_key = NumericLimits<int64_t>::Minimum();
	for (auto &list : lists) {
		total_edges += list.Size();
		min_key = std::min(min_key, list.min_key);
		max_key = std::max(max_key, list.max_key);
	}

	auto dictionary = BuildDictionary(lists, total_edges, min_key, max_key, thread_count);
	auto vertex_count = dictionary->Size();
	if (vertex_count > NumericLimits<uint32_t>::Maximum()) {
		throw OutOfRangeException("A CSR index supports at most %llu distinct vertices",
		                          NumericLimits<uint32_t>::Maximum());
	}
	epoch->dictionary = dictionary;
	epoch->vertex_count = vertex_count;

	// The same destination-range partitioning the on-the-fly build uses
	idx_t directions = undirected ? 2 : 1;
	auto width = GetBufferedPartitionedCSRWidth(vertex_count, thread_count, context);
	auto partition_count = std::max<idx_t>(1, (vertex_count + 2 + width - 1) / width);
	epoch->partition_width = width;

	// Pass 1: map every edge (both directions when undirected) to dense ids and bucket it by destination partition.
	// Each thread appends to its own buckets, so there is no shared state and no per-vertex counter array.
	struct BuildEdge {
		uint32_t src;
		uint16_t local_dst;
		row_t row;
	};
	auto slices = MakeEdgeSlices(lists, directions);
	vector<vector<vector<BuildEdge>>> thread_buckets(thread_count, vector<vector<BuildEdge>>(partition_count));
	ParallelFor(slices.size(), thread_count, [&](idx_t begin, idx_t end, idx_t thread_idx) {
		auto &buckets = thread_buckets[thread_idx];
		for (idx_t slice_idx = begin; slice_idx < end; slice_idx++) {
			auto &slice = slices[slice_idx];
			auto &list = lists[slice.list_idx];
			for (idx_t edge_idx = slice.begin; edge_idx < slice.end; edge_idx++) {
				auto src = NumericCast<uint32_t>(dictionary->Lookup(list.src_keys[edge_idx]));
				auto dst = NumericCast<uint32_t>(dictionary->Lookup(list.dst_keys[edge_idx]));
				auto row = list.row_ids[edge_idx];
				buckets[dst / width].push_back({src, NumericCast<uint16_t>(dst % width), row});
				if (undirected) {
					buckets[src / width].push_back({dst, NumericCast<uint16_t>(src % width), row});
				}
			}
		}
	});
	lists.clear();

	// Pass 2: per partition, gather the thread buckets, radix-sort by source in cache, and emit the sparse rows,
	// the destination offsets, and the row ids in one sweep
	auto src_bits = CSRSourceBits(vertex_count);
	auto csr = make_shared_ptr<PartitionedCSRIndex>();
	csr->vertex_count = vertex_count + 2;
	csr->capabilities = PartitionedCSRCapabilities::FORWARD;
	csr->forward_partitions.resize(partition_count);
	epoch->partition_row_ids.resize(partition_count);
	std::atomic<idx_t> total_edge_count {0};
	ParallelFor(partition_count, thread_count, [&](idx_t begin, idx_t end, idx_t) {
		vector<BuildEdge> edges;
		vector<BuildEdge> scratch;
		vector<idx_t> histogram;
		for (idx_t partition_idx = begin; partition_idx < end; partition_idx++) {
			idx_t partition_edges = 0;
			for (auto &buckets : thread_buckets) {
				partition_edges += buckets[partition_idx].size();
			}
			edges.clear();
			edges.reserve(partition_edges);
			for (auto &buckets : thread_buckets) {
				auto &bucket = buckets[partition_idx];
				edges.insert(edges.end(), bucket.begin(), bucket.end());
				bucket.clear();
				bucket.shrink_to_fit();
			}
			CSRRadixSortBySource(edges, scratch, histogram, src_bits);
			auto start_vertex = partition_idx * width;
			auto end_vertex = std::min<idx_t>(start_vertex + width, vertex_count + 2);
			auto &row_ids = epoch->partition_row_ids[partition_idx];
			row_ids.resize(edges.size());
			csr->forward_partitions[partition_idx] =
			    CSREmitSparsePartition(start_vertex, end_vertex, vertex_count, edges,
			                           [&](idx_t position, const BuildEdge &edge) { row_ids[position] = edge.row; });
			total_edge_count.fetch_add(edges.size(), std::memory_order_relaxed);
		}
	});
	csr->edge_count = total_edge_count.load();
	epoch->edge_count = csr->edge_count;
	epoch->csr = std::move(csr);
	return epoch;
}

void CSRIndex::InstallEpoch(shared_ptr<CSRIndexEpoch> epoch_p) {
	IndexLock l(*this);
	epoch_p->generation = next_generation++;
	epochs.clear();
	epochs.push_back(std::move(epoch_p));
	delta.clear();
	tombstones.clear();
}

shared_ptr<CSRIndexEpoch> CSRIndex::RebuildFromTable(ClientContext &context, DuckTransaction &transaction,
                                                     DataTable &table) const {
	auto types = table.GetTypes();
	vector<StorageIndex> scan_columns {StorageIndex(column_ids[0]), StorageIndex(column_ids[1]),
	                                   StorageIndex(COLUMN_IDENTIFIER_ROW_ID)};
	vector<LogicalType> scan_types {types[column_ids[0]], types[column_ids[1]], LogicalType::ROW_TYPE};
	TableScanState state;
	table.InitializeScan(context, transaction, state, scan_columns);
	DataChunk chunk;
	chunk.Initialize(context, scan_types);
	vector<CSRIndexEdgeList> lists(1);
	while (true) {
		chunk.Reset();
		table.Scan(transaction, chunk, state);
		if (chunk.size() == 0) {
			break;
		}
		DataChunk key_chunk;
		key_chunk.InitializeEmpty({scan_types[0], scan_types[1]});
		key_chunk.ReferenceColumns(chunk, {0, 1});
		key_chunk.SetCardinality(chunk.size());
		CollectEdges(context, key_chunk, chunk.data[2], chunk.size(), lists[0]);
	}
	return BuildEpoch(context, lists, undirected, transaction.view.visibility_bound);
}

void CSRIndex::InstallEpochAndCompact(DuckTransaction &transaction, DataTable &table,
                                      shared_ptr<CSRIndexEpoch> epoch_p) const {
	// Decide, with the live transaction that built the epoch, which existing entries it accounts for. Entries appended
	// after this point are stamped by a later epoch.
	vector<CSRIndexDeltaEdge> delta_prefix;
	vector<CSRIndexTombstone> tombstone_prefix;
	{
		IndexLock l(*this);
		delta_prefix = delta;
		tombstone_prefix = tombstones;
	}
	vector<uint8_t> delta_covered(delta_prefix.size());
	vector<uint8_t> tombstone_excluded(tombstone_prefix.size());
	for (idx_t i = 0; i < delta_prefix.size(); i++) {
		delta_covered[i] = table.CanFetch(transaction, delta_prefix[i].row);
	}
	for (idx_t i = 0; i < tombstone_prefix.size(); i++) {
		tombstone_excluded[i] = !table.CanFetch(transaction, tombstone_prefix[i].row);
	}
	IndexLock l(*this);
	auto generation = next_generation++;
	epoch_p->generation = generation;
	for (idx_t i = 0; i < delta_covered.size() && i < delta.size(); i++) {
		if (delta_covered[i]) {
			delta[i].covered_epoch = std::min(delta[i].covered_epoch, generation);
		}
	}
	for (idx_t i = 0; i < tombstone_excluded.size() && i < tombstones.size(); i++) {
		if (tombstone_excluded[i]) {
			tombstones[i].excluded_epoch = std::min(tombstones[i].excluded_epoch, generation);
		}
	}
	// Keep the previous epoch for transactions that started before this fold
	epochs.push_back(std::move(epoch_p));
	while (epochs.size() > MAX_RETAINED_EPOCHS) {
		epochs.erase(epochs.begin());
	}
	CompactChangeLogLocked();
}

void CSRIndex::CompactChangeLogLocked() const {
	if (epochs.empty()) {
		return;
	}
	// An entry the oldest retained epoch accounts for is accounted for by every retained epoch
	auto oldest_generation = epochs.front()->generation;
	vector<CSRIndexDeltaEdge> new_delta;
	for (auto &edge : delta) {
		if (edge.covered_epoch > oldest_generation) {
			new_delta.push_back(edge);
		}
	}
	vector<CSRIndexTombstone> new_tombstones;
	for (auto &tombstone : tombstones) {
		if (tombstone.excluded_epoch > oldest_generation) {
			new_tombstones.push_back(tombstone);
		}
	}
	delta = std::move(new_delta);
	tombstones = std::move(new_tombstones);
	for (auto &epoch : epochs) {
		epoch->tombstone_positions.clear();
	}
}

shared_ptr<CSRIndexEpoch> CSRIndex::EpochForLocked(VisibilityBound bound) const {
	for (auto it = epochs.rbegin(); it != epochs.rend(); ++it) {
		if (VisibilityBound::Min((*it)->bound, bound) == (*it)->bound) {
			return *it;
		}
	}
	return nullptr;
}

void CSRIndex::ResolveTombstonePositions(CSRIndexEpoch &epoch, idx_t tombstone_count) const {
	vector<row_t> pending;
	idx_t known;
	{
		IndexLock l(*this);
		known = epoch.tombstone_positions.size();
		if (known >= tombstone_count) {
			return;
		}
		for (idx_t i = known; i < tombstone_count && i < tombstones.size(); i++) {
			pending.push_back(tombstones[i].row);
		}
	}
	// One scan over the epoch's row ids finds the positions of all pending rows; rows not found are not epoch edges
	std::unordered_map<row_t, idx_t> positions;
	positions.reserve(pending.size());
	for (auto row : pending) {
		positions.emplace(row, DConstants::INVALID_INDEX);
	}
	auto &partition_row_ids = epoch.partition_row_ids;
	mutex positions_lock;
	ParallelFor(partition_row_ids.size(), std::thread::hardware_concurrency(), [&](idx_t begin, idx_t end, idx_t) {
		vector<std::pair<row_t, idx_t>> found;
		for (idx_t partition_idx = begin; partition_idx < end; partition_idx++) {
			auto &rows = partition_row_ids[partition_idx];
			for (idx_t offset = 0; offset < rows.size(); offset++) {
				if (positions.find(rows[offset]) != positions.end()) {
					found.emplace_back(rows[offset], (partition_idx << 32) | offset);
				}
			}
		}
		if (!found.empty()) {
			lock_guard<mutex> guard(positions_lock);
			for (auto &entry : found) {
				positions[entry.first] = entry.second;
			}
		}
	});
	IndexLock l(*this);
	if (epoch.tombstone_positions.size() != known) {
		// Another reader resolved them meanwhile, or the log was compacted
		return;
	}
	for (auto row : pending) {
		epoch.tombstone_positions.push_back(positions[row]);
	}
}

shared_ptr<CSRIndexSnapshot> CSRIndex::BuildSnapshot(ClientContext &context, DuckTransaction &transaction,
                                                     DataTable &table, shared_ptr<CSRIndexEpoch> epoch_p,
                                                     idx_t delta_count, idx_t tombstone_count) const {
	auto snapshot = make_shared_ptr<CSRIndexSnapshot>();
	snapshot->epoch = epoch_p;
	auto &epoch = *snapshot->epoch;
	auto width = epoch.partition_width;
	snapshot->partition_width = width;
	snapshot->partitions = epoch.csr->forward_partitions;
	snapshot->edge_count = epoch.edge_count;
	auto epoch_vertex_count = epoch.vertex_count;

	// Tombstones: epoch edges deleted by commits this reader sees are suppressed
	if (tombstone_count > 0) {
		ResolveTombstonePositions(epoch, tombstone_count);
		vector<row_t> rows;
		vector<idx_t> positions;
		{
			IndexLock l(*this);
			tombstone_count = std::min<idx_t>(tombstone_count, epoch.tombstone_positions.size());
			for (idx_t i = 0; i < tombstone_count; i++) {
				rows.push_back(tombstones[i].row);
			}
			positions.assign(epoch.tombstone_positions.begin(),
			                 epoch.tombstone_positions.begin() + NumericCast<int64_t>(tombstone_count));
		}
		snapshot->suppressed.resize(snapshot->partitions.size());
		for (idx_t i = 0; i < tombstone_count; i++) {
			if (positions[i] == DConstants::INVALID_INDEX || table.CanFetch(transaction, rows[i])) {
				continue;
			}
			auto partition_idx = positions[i] >> 32;
			auto offset = positions[i] & 0xFFFFFFFF;
			auto &bitmap = snapshot->suppressed[partition_idx];
			if (!bitmap) {
				bitmap = make_shared_ptr<vector<uint64_t>>((snapshot->partitions[partition_idx]->e.size() + 63) / 64);
			}
			(*bitmap)[offset >> 6] |= uint64_t(1) << (offset & 63);
			snapshot->edge_count--;
		}
	}

	// Delta: edges committed after the epoch that this reader sees, bucketed by destination range
	vector<CSRIndexDeltaEdge> delta_edges;
	if (delta_count > 0) {
		IndexLock l(*this);
		delta_count = std::min<idx_t>(delta_count, delta.size());
		delta_edges.assign(delta.begin(), delta.begin() + NumericCast<int64_t>(delta_count));
	}
	struct DeltaEntry {
		uint32_t src;
		uint16_t local_dst;
	};
	auto lookup_or_add = [&](int64_t key) -> uint32_t {
		auto id = epoch.dictionary->Lookup(key);
		if (id != DConstants::INVALID_INDEX) {
			return NumericCast<uint32_t>(id);
		}
		auto entry = snapshot->extra_key_to_id.find(key);
		if (entry != snapshot->extra_key_to_id.end()) {
			return entry->second;
		}
		auto new_id = NumericCast<uint32_t>(epoch_vertex_count + snapshot->extra_keys.size());
		snapshot->extra_keys.push_back(key);
		snapshot->extra_key_to_id.emplace(key, new_id);
		return new_id;
	};
	vector<std::pair<uint32_t, uint32_t>> visible_edges;
	for (auto &edge : delta_edges) {
		if (!table.CanFetch(transaction, edge.row)) {
			continue;
		}
		auto src = lookup_or_add(edge.src_key);
		auto dst = lookup_or_add(edge.dst_key);
		visible_edges.emplace_back(src, dst);
		if (undirected) {
			visible_edges.emplace_back(dst, src);
		}
	}
	snapshot->vertex_count = epoch_vertex_count + snapshot->extra_keys.size();
	snapshot->edge_count += visible_edges.size();

	// Every destination range up to the snapshot's vertex count must have a partition, because the kernel clears and
	// checks the frontier per partition range. Ranges beyond the epoch get empty cover partitions; the part of the
	// epoch's last range above the epoch's vertex count gets one too.
	auto total_ranges = std::max<idx_t>(1, (snapshot->vertex_count + 2 + width - 1) / width);
	vector<idx_t> range_to_list(total_ranges, DConstants::INVALID_INDEX);
	for (idx_t partition_idx = 0; partition_idx < snapshot->partitions.size(); partition_idx++) {
		range_to_list[partition_idx] = partition_idx;
	}
	auto epoch_end = epoch_vertex_count + 2;
	auto snapshot_end = snapshot->vertex_count + 2;
	if (snapshot_end > epoch_end) {
		auto epoch_ranges = snapshot->partitions.size();
		auto last_range_end = epoch_ranges * width;
		if (epoch_end < std::min(last_range_end, snapshot_end)) {
			auto cover = make_shared_ptr<LocalCSR>(epoch_end, std::min(last_range_end, snapshot_end),
			                                       snapshot->vertex_count, false);
			cover->v_array_size = 0;
			cover->sparse_rows_initialized = true;
			cover->initialized_e = true;
			snapshot->partitions.push_back(std::move(cover));
		}
		for (idx_t range = epoch_ranges; range < total_ranges; range++) {
			auto start_vertex = range * width;
			auto end_vertex = std::min<idx_t>(start_vertex + width, snapshot_end);
			auto cover = make_shared_ptr<LocalCSR>(start_vertex, end_vertex, snapshot->vertex_count, false);
			cover->v_array_size = 0;
			cover->sparse_rows_initialized = true;
			cover->initialized_e = true;
			range_to_list[range] = snapshot->partitions.size();
			snapshot->partitions.push_back(std::move(cover));
		}
	}
	snapshot->delta_partitions.resize(snapshot->partitions.size());
	snapshot->suppressed.resize(snapshot->partitions.size());

	if (!visible_edges.empty()) {
		vector<vector<DeltaEntry>> buckets(total_ranges);
		for (auto &edge : visible_edges) {
			auto range = edge.second / width;
			buckets[range].push_back({edge.first, NumericCast<uint16_t>(edge.second - range * width)});
		}
		auto src_bits = CSRSourceBits(snapshot->vertex_count);
		vector<DeltaEntry> scratch;
		vector<idx_t> histogram;
		for (idx_t range = 0; range < total_ranges; range++) {
			if (buckets[range].empty()) {
				continue;
			}
			CSRRadixSortBySource(buckets[range], scratch, histogram, src_bits);
			auto start_vertex = range * width;
			auto end_vertex = std::min<idx_t>(start_vertex + width, snapshot_end);
			snapshot->delta_partitions[range_to_list[range]] = CSREmitSparsePartition(
			    start_vertex, end_vertex, snapshot->vertex_count, buckets[range], [](idx_t, const DeltaEntry &) {});
		}
	}
	return snapshot;
}

void CSRIndex::Fold(ClientContext &context, DuckTransaction &transaction, DataTable &table) const {
	if (transaction.ChangesMade()) {
		throw InvalidInputException("A CSR index cannot be folded by a transaction with uncommitted changes");
	}
	lock_guard<mutex> fold_guard(fold_lock);
	InstallEpochAndCompact(transaction, table, RebuildFromTable(context, transaction, table));
}

shared_ptr<CSRIndexSnapshot> CSRIndex::Snapshot(ClientContext &context, DuckTransaction &transaction,
                                                DataTable &table) const {
	if (transaction.ChangesMade()) {
		// The reader's own uncommitted writes are visible to it alone: build a private epoch from its view of the
		// table (which includes them) and leave the shared epochs, which must only contain committed rows, untouched
		return BuildSnapshot(context, transaction, table, RebuildFromTable(context, transaction, table), 0, 0);
	}
	auto bound = transaction.view.visibility_bound;
	shared_ptr<CSRIndexEpoch> current;
	bool have_epochs;
	idx_t delta_count;
	idx_t tombstone_count;
	{
		IndexLock l(*this);
		have_epochs = !epochs.empty();
		current = EpochForLocked(bound);
		delta_count = delta.size();
		tombstone_count = tombstones.size();
	}
	if (!have_epochs) {
		// No epoch yet (index created before a restart, or never read): build one at this reader's snapshot
		lock_guard<mutex> fold_guard(fold_lock);
		bool still_empty;
		{
			IndexLock l(*this);
			still_empty = epochs.empty();
		}
		if (still_empty) {
			InstallEpochAndCompact(transaction, table, RebuildFromTable(context, transaction, table));
		}
		IndexLock l(*this);
		current = EpochForLocked(bound);
		delta_count = delta.size();
		tombstone_count = tombstones.size();
	} else if (current && current == epochs.back()) {
		// Current readers fold when the change log has grown past the configured fraction of the epoch
		auto ratio = GetCSRIndexFoldRatio(context);
		if (ratio > 0 && static_cast<double>(delta_count + tombstone_count) >
		                     ratio * static_cast<double>(std::max<idx_t>(1, current->edge_count))) {
			Fold(context, transaction, table);
			IndexLock l(*this);
			current = EpochForLocked(bound);
			delta_count = delta.size();
			tombstone_count = tombstones.size();
		}
	}
	if (!current) {
		// Every retained epoch is newer than this reader's snapshot
		return nullptr;
	}
	return BuildSnapshot(context, transaction, table, current, delta_count, tombstone_count);
}

idx_t CSRIndex::DeltaSize() const {
	IndexLock l(*this);
	return delta.size();
}

idx_t CSRIndex::TombstoneCount() const {
	IndexLock l(*this);
	return tombstones.size();
}

idx_t CSRIndex::EpochCount() const {
	IndexLock l(*this);
	return epochs.size();
}

//----------------------------------------------------------------------------------------------------------------------
// CSRIndexSnapshot
//----------------------------------------------------------------------------------------------------------------------
idx_t CSRIndexSnapshot::Lookup(int64_t key) const {
	auto id = epoch->dictionary->Lookup(key);
	if (id != DConstants::INVALID_INDEX) {
		return id;
	}
	auto entry = extra_key_to_id.find(key);
	return entry == extra_key_to_id.end() ? DConstants::INVALID_INDEX : entry->second;
}

int64_t CSRIndexSnapshot::KeyOf(idx_t id) const {
	if (id < epoch->vertex_count) {
		return epoch->dictionary->KeyOf(id);
	}
	return extra_keys[id - epoch->vertex_count];
}

//----------------------------------------------------------------------------------------------------------------------
// Lookup helpers for the planner and the operator
//----------------------------------------------------------------------------------------------------------------------
static optional_ptr<DuckTableEntry> GetEdgeTableEntry(ClientContext &context, const PropertyGraphTable &edge_table) {
	auto entry = Catalog::GetEntry<TableCatalogEntry>(
	    context, QualifiedName(edge_table.catalog_name, edge_table.schema_name, edge_table.table_name),
	    OnEntryNotFound::RETURN_NULL);
	if (!entry || !entry->IsDuckTable()) {
		return nullptr;
	}
	return &entry->Cast<DuckTableEntry>();
}

string CSRIndex::FindForEdgeTable(ClientContext &context, const PropertyGraphTable &edge_table, bool undirected) {
	if (edge_table.source_fk.size() != 1 || edge_table.destination_fk.size() != 1) {
		return string();
	}
	auto table = GetEdgeTableEntry(context, edge_table);
	if (!table) {
		return string();
	}
	auto src_name = edge_table.source_fk[0];
	auto dst_name = edge_table.destination_fk[0];
	auto src_logical = table->GetColumnIndex(src_name, true);
	auto dst_logical = table->GetColumnIndex(dst_name, true);
	if (!src_logical.IsValid() || !dst_logical.IsValid()) {
		return string();
	}
	auto src_physical = table->GetColumns().LogicalToPhysical(src_logical).index;
	auto dst_physical = table->GetColumns().LogicalToPhysical(dst_logical).index;

	auto &storage = table->GetStorage();
	storage.BindIndexes(context);
	for (auto entry : storage.GetDataTableInfo()->GetIndexes().IndexEntries()) {
		if (entry->GetIndexType() != TYPE_NAME || entry->GetBindState() != IndexBindState::BOUND) {
			continue;
		}
		auto handle = entry->GetReadHandle<CSRIndex>();
		auto &index_columns = handle->GetColumnIds();
		if (index_columns.size() == 2 && index_columns[0] == src_physical && index_columns[1] == dst_physical &&
		    handle->IsUndirected() == undirected) {
			return handle->GetIndexName().GetIdentifierName();
		}
	}
	return string();
}

optional_ptr<DuckTableEntry> CSRIndex::FindTable(ClientContext &context, const string &table_name) {
	auto qualified = QualifiedName::Parse(table_name);
	auto entry = Catalog::GetEntry<TableCatalogEntry>(context, qualified, OnEntryNotFound::RETURN_NULL);
	if (!entry || !entry->IsDuckTable()) {
		return nullptr;
	}
	return &entry->Cast<DuckTableEntry>();
}

shared_ptr<CSRIndexSnapshot> CSRIndex::Resolve(ClientContext &context, const string &catalog, const string &schema,
                                               const string &table_name, const string &index_name) {
	auto entry = Catalog::GetEntry<TableCatalogEntry>(
	    context, QualifiedName(Identifier(catalog), Identifier(schema), Identifier(table_name)),
	    OnEntryNotFound::RETURN_NULL);
	if (!entry || !entry->IsDuckTable()) {
		return nullptr;
	}
	auto &table = entry->Cast<DuckTableEntry>();
	auto &storage = table.GetStorage();
	storage.BindIndexes(context);
	auto index_entry = storage.GetDataTableInfo()->GetIndexes().FindEntry(Identifier(index_name));
	if (!index_entry || index_entry->GetIndexType() != TYPE_NAME ||
	    index_entry->GetBindState() != IndexBindState::BOUND) {
		return nullptr;
	}
	auto handle = index_entry->GetReadHandle<CSRIndex>();
	auto &transaction = DuckTransaction::Get(context, table.catalog);
	return handle->Snapshot(context, transaction, storage);
}

static constexpr const char *CSR_INDEX_KEY_PREFIX = "csr-index|";

string CSRIndex::CacheKey(const PropertyGraphTable &edge_table, const string &index_name) {
	return string(CSR_INDEX_KEY_PREFIX) + edge_table.catalog_name.GetIdentifierName() + "|" +
	       edge_table.schema_name.GetIdentifierName() + "|" + edge_table.table_name.GetIdentifierName() + "|" +
	       index_name;
}

bool CSRIndex::IsCacheKey(const string &cache_key) {
	return StringUtil::StartsWith(cache_key, CSR_INDEX_KEY_PREFIX);
}

shared_ptr<CSRIndexSnapshot> CSRIndex::ResolveCacheKey(ClientContext &context, const string &cache_key) {
	if (!IsCacheKey(cache_key)) {
		return nullptr;
	}
	auto parts = StringUtil::Split(cache_key.substr(strlen(CSR_INDEX_KEY_PREFIX)), '|');
	if (parts.size() != 4) {
		throw InternalException("Malformed CSR index cache key '%s'", cache_key);
	}
	return Resolve(context, parts[0], parts[1], parts[2], parts[3]);
}

//----------------------------------------------------------------------------------------------------------------------
// BoundIndex interface
//----------------------------------------------------------------------------------------------------------------------
namespace {

template <class T>
void ReadIntegerKeys(Vector &input, idx_t count, vector<int64_t> &keys, vector<uint8_t> &valid) {
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);
	auto data = UnifiedVectorFormat::GetData<T>(format);
	for (idx_t row = 0; row < count; row++) {
		auto idx = format.sel->get_index(row);
		valid[row] = format.validity.RowIsValid(idx);
		keys[row] = valid[row] ? static_cast<int64_t>(data[idx]) : 0;
	}
}

//! Reads an integer key column as BIGINT without a ClientContext (the index hooks have none)
void ReadKeys(Vector &input, idx_t count, vector<int64_t> &keys, vector<uint8_t> &valid) {
	keys.resize(count);
	valid.resize(count);
	switch (input.GetType().InternalType()) {
	case PhysicalType::INT8:
		return ReadIntegerKeys<int8_t>(input, count, keys, valid);
	case PhysicalType::INT16:
		return ReadIntegerKeys<int16_t>(input, count, keys, valid);
	case PhysicalType::INT32:
		return ReadIntegerKeys<int32_t>(input, count, keys, valid);
	case PhysicalType::INT64:
		return ReadIntegerKeys<int64_t>(input, count, keys, valid);
	case PhysicalType::UINT8:
		return ReadIntegerKeys<uint8_t>(input, count, keys, valid);
	case PhysicalType::UINT16:
		return ReadIntegerKeys<uint16_t>(input, count, keys, valid);
	case PhysicalType::UINT32:
		return ReadIntegerKeys<uint32_t>(input, count, keys, valid);
	default:
		throw NotImplementedException("CSR index keys of type %s are not supported", input.GetType().ToString());
	}
}

} // namespace

ErrorData CSRIndex::Append(IndexLock &l, DataChunk &chunk, Vector &row_ids) {
	l.AssertHeld(*this);
	// Evaluate the key expressions on the incoming rows, like the ART does
	DataChunk key_chunk;
	key_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(chunk, key_chunk);
	auto count = key_chunk.size();
	vector<int64_t> src_keys;
	vector<int64_t> dst_keys;
	vector<uint8_t> src_valid;
	vector<uint8_t> dst_valid;
	ReadKeys(key_chunk.data[0], count, src_keys, src_valid);
	ReadKeys(key_chunk.data[1], count, dst_keys, dst_valid);
	UnifiedVectorFormat row_format;
	row_ids.ToUnifiedFormat(count, row_format);
	auto row_data = UnifiedVectorFormat::GetData<row_t>(row_format);
	for (idx_t row = 0; row < count; row++) {
		if (!src_valid[row] || !dst_valid[row]) {
			continue;
		}
		delta.push_back({src_keys[row], dst_keys[row], row_data[row_format.sel->get_index(row)],
		                 NumericLimits<uint32_t>::Maximum()});
	}
	return ErrorData();
}

ErrorData CSRIndex::Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids) {
	return Append(l, chunk, row_ids);
}

idx_t CSRIndex::TryDelete(IndexLock &l, DataChunk &entries, Vector &row_identifiers,
                          optional_ptr<SelectionVector> deleted_sel, optional_ptr<SelectionVector>) {
	l.AssertHeld(*this);
	auto count = entries.size();
	UnifiedVectorFormat row_format;
	row_identifiers.ToUnifiedFormat(count, row_format);
	auto row_data = UnifiedVectorFormat::GetData<row_t>(row_format);
	for (idx_t row = 0; row < count; row++) {
		// Whether the row is an epoch edge or a delta edge is resolved lazily by the readers
		tombstones.push_back({row_data[row_format.sel->get_index(row)], NumericLimits<uint32_t>::Maximum()});
		if (deleted_sel) {
			deleted_sel->set_index(row, row);
		}
	}
	return count;
}

void CSRIndex::ResetStorage(IndexLock &l) {
	l.AssertHeld(*this);
	epochs.clear();
	delta.clear();
	tombstones.clear();
}

bool CSRIndex::MergeIndexes(IndexLock &, BoundIndex &) {
	return false;
}

void CSRIndex::Vacuum(IndexLock &) {
}

idx_t CSRIndex::GetInMemorySize(IndexLock &l) const {
	l.AssertHeld(*this);
	idx_t bytes = delta.size() * sizeof(CSRIndexDeltaEdge) + tombstones.size() * sizeof(CSRIndexTombstone);
	for (auto &epoch : epochs) {
		bytes += epoch->tombstone_positions.size() * sizeof(idx_t);
		bytes += epoch->dictionary->bitmap.size() * sizeof(uint64_t) +
		         epoch->dictionary->word_rank.size() * sizeof(uint32_t) +
		         epoch->dictionary->sorted_keys.size() * sizeof(int64_t);
		for (auto &partition : epoch->csr->forward_partitions) {
			bytes += partition->e.size() * sizeof(uint16_t) + partition->row_offsets.size() * sizeof(uint32_t) +
			         partition->source_vertices.size() * sizeof(uint32_t);
		}
		for (auto &row_ids : epoch->partition_row_ids) {
			bytes += row_ids.size() * sizeof(row_t);
		}
	}
	return bytes;
}

void CSRIndex::Verify(IndexLock &) {
}

string CSRIndex::ToString(IndexLock &l, bool) {
	l.AssertHeld(*this);
	if (epochs.empty()) {
		return StringUtil::Format("CSR index (no epoch, %llu delta edges, %llu tombstones)", delta.size(),
		                          tombstones.size());
	}
	auto &epoch = *epochs.back();
	return StringUtil::Format(
	    "CSR index: %llu vertices, %llu edges, %llu partitions%s, %llu epochs, %llu delta edges, %llu tombstones",
	    epoch.vertex_count, epoch.edge_count, epoch.csr->forward_partitions.size(), undirected ? ", undirected" : "",
	    epochs.size(), delta.size(), tombstones.size());
}

void CSRIndex::VerifyAllocations(IndexLock &) {
}

string CSRIndex::GetConstraintViolationMessage(VerifyExistenceType, idx_t, DataChunk &) const {
	throw InternalException("A CSR index does not enforce constraints");
}

IndexStorageInfo CSRIndex::SerializeToDisk(QueryContext, const case_insensitive_map_t<Value> &) {
	// Nothing is persisted yet: after a restart the index comes back empty and rebuilds from the table on first use
	return IndexStorageInfo(name);
}

IndexStorageInfo CSRIndex::SerializeToWAL(const case_insensitive_map_t<Value> &) {
	return IndexStorageInfo(name);
}

//----------------------------------------------------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------------------------------------------------
void CorePGQIndex::Register(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.GetIndexTypes().RegisterIndexType(CSRIndex::GetCSRIndexType());
}

} // namespace duckdb
