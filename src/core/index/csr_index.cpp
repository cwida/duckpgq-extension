#include "duckpgq/core/index/csr_index.hpp"

#include "duckpgq/core/operator/partitioned_csr_cache.hpp"
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

//! Runs fn(begin, end) over [0, count) on up to thread_count threads and rethrows the first exception
template <class FUNC>
void ParallelFor(idx_t count, idx_t thread_count, FUNC &&fn) {
	thread_count = std::max<idx_t>(1, std::min(thread_count, count));
	if (thread_count == 1) {
		fn(idx_t(0), count);
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
				fn(begin, end);
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
		ParallelFor(slices.size(), thread_count, [&](idx_t begin, idx_t end) {
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
		return dictionary;
	}

	// Sparse keys: gather, sort the pieces in parallel, merge, deduplicate
	vector<int64_t> keys(total_edges * 2);
	ParallelFor(slices.size(), thread_count, [&](idx_t begin, idx_t end) {
		for (idx_t slice_idx = begin; slice_idx < end; slice_idx++) {
			auto &slice = slices[slice_idx];
			auto &list = lists[slice.list_idx];
			auto out = slice.offset * 2;
			for (idx_t edge_idx = slice.begin; edge_idx < slice.end; edge_idx++) {
				keys[out++] = list.src_keys[edge_idx];
				keys[out++] = list.dst_keys[edge_idx];
			}
		}
	});
	auto piece_count = std::max<idx_t>(1, std::min<idx_t>(thread_count, keys.size() / 4096 + 1));
	auto piece_size = (keys.size() + piece_count - 1) / piece_count;
	ParallelFor(piece_count, thread_count, [&](idx_t begin, idx_t end) {
		for (idx_t piece = begin; piece < end; piece++) {
			auto first = std::min<idx_t>(keys.size(), piece * piece_size);
			auto last = std::min<idx_t>(keys.size(), first + piece_size);
			std::sort(keys.begin() + first, keys.begin() + last);
		}
	});
	for (idx_t width = piece_size; width < keys.size(); width *= 2) {
		auto merge_count = (keys.size() + 2 * width - 1) / (2 * width);
		ParallelFor(merge_count, thread_count, [&](idx_t begin, idx_t end) {
			for (idx_t merge_idx = begin; merge_idx < end; merge_idx++) {
				auto first = merge_idx * 2 * width;
				auto middle = std::min<idx_t>(keys.size(), first + width);
				auto last = std::min<idx_t>(keys.size(), first + 2 * width);
				if (middle < last) {
					std::inplace_merge(keys.begin() + first, keys.begin() + middle, keys.begin() + last);
				}
			}
		});
	}
	keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
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
		if (!expr->GetReturnType().IsIntegral()) {
			throw BinderException("CSR index keys must be integer columns, got %s", expr->GetReturnType().ToString());
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

	// Map every edge (both directions when undirected) to dense ids, in parallel over slices of the edge lists
	idx_t directions = undirected ? 2 : 1;
	auto edge_count = total_edges * directions;
	auto slices = MakeEdgeSlices(lists, directions);
	vector<uint32_t> edge_src(edge_count);
	vector<uint32_t> edge_dst(edge_count);
	vector<row_t> edge_row(edge_count);
	ParallelFor(slices.size(), thread_count, [&](idx_t begin, idx_t end) {
		for (idx_t slice_idx = begin; slice_idx < end; slice_idx++) {
			auto &slice = slices[slice_idx];
			auto &list = lists[slice.list_idx];
			auto out = slice.offset;
			for (idx_t edge_idx = slice.begin; edge_idx < slice.end; edge_idx++) {
				auto src = NumericCast<uint32_t>(dictionary->Lookup(list.src_keys[edge_idx]));
				auto dst = NumericCast<uint32_t>(dictionary->Lookup(list.dst_keys[edge_idx]));
				edge_src[out] = src;
				edge_dst[out] = dst;
				edge_row[out++] = list.row_ids[edge_idx];
				if (undirected) {
					edge_src[out] = dst;
					edge_dst[out] = src;
					edge_row[out++] = list.row_ids[edge_idx];
				}
			}
		}
	});
	// The edge lists are no longer needed; free them before allocating the CSR
	lists.clear();

	// The same destination-range partitioning the on-the-fly build uses
	auto width = GetBufferedPartitionedCSRWidth(vertex_count, thread_count, context);
	auto partition_count = std::max<idx_t>(1, (vertex_count + 2 + width - 1) / width);
	epoch->partition_width = width;
	auto csr = make_shared_ptr<PartitionedCSRIndex>();
	csr->vertex_count = vertex_count + 2;
	csr->capabilities = PartitionedCSRCapabilities::FORWARD;
	csr->forward_partitions.reserve(partition_count);
	for (idx_t partition_idx = 0; partition_idx < partition_count; partition_idx++) {
		auto start_vertex = partition_idx * width;
		auto end_vertex = std::min<idx_t>(start_vertex + width, vertex_count + 2);
		csr->forward_partitions.push_back(make_shared_ptr<LocalCSR>(start_vertex, end_vertex, vertex_count, true));
	}
	epoch->partition_row_ids.resize(partition_count);

	// Count per source (stored at v[src + 1]) in parallel over the edges, prefix sum per partition in parallel over
	// the partitions, then fill with v[src + 1] as the cursor: the layout the operator's own build produces
	auto edge_chunks = (edge_count + EDGE_SLICE_SIZE - 1) / EDGE_SLICE_SIZE;
	ParallelFor(edge_chunks, thread_count, [&](idx_t begin, idx_t end) {
		for (idx_t edge_idx = begin * EDGE_SLICE_SIZE; edge_idx < std::min(edge_count, end * EDGE_SLICE_SIZE);
		     edge_idx++) {
			auto &partition = *csr->forward_partitions[edge_dst[edge_idx] / width];
			partition.v[edge_src[edge_idx] + 1].fetch_add(1, std::memory_order_relaxed);
		}
	});
	ParallelFor(partition_count, thread_count, [&](idx_t begin, idx_t end) {
		for (idx_t partition_idx = begin; partition_idx < end; partition_idx++) {
			auto &partition = *csr->forward_partitions[partition_idx];
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
			epoch->partition_row_ids[partition_idx].resize(NumericCast<idx_t>(running_sum));
		}
	});
	ParallelFor(edge_chunks, thread_count, [&](idx_t begin, idx_t end) {
		for (idx_t edge_idx = begin * EDGE_SLICE_SIZE; edge_idx < std::min(edge_count, end * EDGE_SLICE_SIZE);
		     edge_idx++) {
			auto partition_idx = edge_dst[edge_idx] / width;
			auto &partition = *csr->forward_partitions[partition_idx];
			auto position = partition.v[edge_src[edge_idx] + 1].fetch_add(1, std::memory_order_relaxed);
			partition.e[position] = NumericCast<uint16_t>(edge_dst[edge_idx] - partition.start_vertex);
			epoch->partition_row_ids[partition_idx][position] = edge_row[edge_idx];
		}
	});
	ParallelFor(partition_count, thread_count, [&](idx_t begin, idx_t end) {
		for (idx_t partition_idx = begin; partition_idx < end; partition_idx++) {
			csr->forward_partitions[partition_idx]->FinalizeSparseRows();
		}
	});
	csr->edge_count = edge_count;
	epoch->edge_count = edge_count;
	epoch->csr = std::move(csr);
	return epoch;
}

void CSRIndex::InstallEpoch(shared_ptr<CSRIndexEpoch> epoch_p) {
	IndexLock l(*this);
	epoch = std::move(epoch_p);
	stale = false;
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

shared_ptr<CSRIndexEpoch> CSRIndex::Snapshot(ClientContext &context, DuckTransaction &transaction,
                                             DataTable &table) const {
	if (transaction.ChangesMade()) {
		// The reader's own uncommitted writes are visible to it alone: build a private epoch from its view of the
		// table and leave the shared epoch, which must only ever contain committed rows, untouched
		return RebuildFromTable(context, transaction, table);
	}
	shared_ptr<CSRIndexEpoch> current;
	bool rebuild;
	{
		IndexLock l(*this);
		current = epoch;
		rebuild = !epoch || stale;
		if (rebuild) {
			// Changes committed while we scan set the flag again, so a concurrent writer is never missed
			stale = false;
		}
	}
	if (rebuild) {
		auto rebuilt = RebuildFromTable(context, transaction, table);
		IndexLock l(*this);
		epoch = rebuilt;
		return rebuilt;
	}
	// The epoch must not contain rows this reader cannot see: epoch.bound <= reader.bound
	if (VisibilityBound::Min(current->bound, transaction.view.visibility_bound) != current->bound) {
		return nullptr;
	}
	return current;
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

shared_ptr<CSRIndexEpoch> CSRIndex::Resolve(ClientContext &context, const string &catalog, const string &schema,
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
	if (!index_entry || index_entry->GetIndexType() != TYPE_NAME || index_entry->GetBindState() != IndexBindState::BOUND) {
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

shared_ptr<CSRIndexEpoch> CSRIndex::ResolveCacheKey(ClientContext &context, const string &cache_key) {
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
ErrorData CSRIndex::Append(IndexLock &l, DataChunk &, Vector &) {
	l.AssertHeld(*this);
	stale = true;
	return ErrorData();
}

ErrorData CSRIndex::Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids) {
	return Append(l, chunk, row_ids);
}

idx_t CSRIndex::TryDelete(IndexLock &l, DataChunk &entries, Vector &, optional_ptr<SelectionVector> deleted_sel,
                          optional_ptr<SelectionVector>) {
	l.AssertHeld(*this);
	// Deleted rows are not tracked yet; the epoch is rebuilt by the next reader
	stale = true;
	if (deleted_sel) {
		for (idx_t i = 0; i < entries.size(); i++) {
			deleted_sel->set_index(i, i);
		}
	}
	return entries.size();
}

void CSRIndex::ResetStorage(IndexLock &l) {
	l.AssertHeld(*this);
	epoch.reset();
	stale = false;
}

bool CSRIndex::MergeIndexes(IndexLock &, BoundIndex &) {
	return false;
}

void CSRIndex::Vacuum(IndexLock &) {
}

idx_t CSRIndex::GetInMemorySize(IndexLock &l) const {
	l.AssertHeld(*this);
	if (!epoch) {
		return 0;
	}
	idx_t bytes = epoch->dictionary->bitmap.size() * sizeof(uint64_t) +
	              epoch->dictionary->word_rank.size() * sizeof(uint32_t) +
	              epoch->dictionary->sorted_keys.size() * sizeof(int64_t);
	for (auto &partition : epoch->csr->forward_partitions) {
		bytes += partition->e.size() * sizeof(uint16_t) + partition->row_offsets.size() * sizeof(uint32_t) +
		         partition->source_vertices.size() * sizeof(uint32_t);
	}
	for (auto &row_ids : epoch->partition_row_ids) {
		bytes += row_ids.size() * sizeof(row_t);
	}
	return bytes;
}

void CSRIndex::Verify(IndexLock &) {
}

string CSRIndex::ToString(IndexLock &l, bool) {
	l.AssertHeld(*this);
	if (!epoch) {
		return "CSR index (no epoch)";
	}
	return StringUtil::Format("CSR index: %llu vertices, %llu edges, %llu partitions%s%s", epoch->vertex_count,
	                          epoch->edge_count, epoch->csr->forward_partitions.size(),
	                          undirected ? ", undirected" : "", stale ? ", stale" : "");
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
