#include "duckpgq/core/utils/parallel_for.hpp"
#include "duckpgq/core/index/csr_index.hpp"
#include "duckdb/main/attached_database.hpp"

#include "duckpgq/core/operator/partitioned_csr_geometry.hpp"
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
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

#include <algorithm>
#include <bitset>
#include <chrono>
#include <cstring>

namespace duckdb {

namespace {
//! Keys of the index's entries in IndexStorageInfo::options
constexpr const char *STORAGE_OPTION_FORMAT = "csr_format";
constexpr const char *STORAGE_OPTION_BYTES = "csr_bytes";
constexpr const char *STORAGE_OPTION_SEGMENTS = "csr_segments";

//! An epoch position names an edge by its partition and its offset in that partition's edge array, packed into one
//! idx_t (the tombstone position vectors and the suppression bitmaps address epoch edges this way)
inline idx_t PackPosition(idx_t partition_idx, idx_t offset) {
	return (partition_idx << 32) | offset;
}
inline idx_t PositionPartition(idx_t position) {
	return position >> 32;
}
inline idx_t PositionOffset(idx_t position) {
	return position & 0xFFFFFFFF;
}

//! A storage segment pointer travels in the storage options as one UBIGINT: buffer id high, offset low
inline uint64_t PackSegment(const IndexPointer &pointer) {
	return (static_cast<uint64_t>(pointer.GetBufferId()) << 32) | pointer.GetOffset();
}
inline idx_t SegmentBufferId(uint64_t packed) {
	return packed >> 32;
}
inline IndexPointer UnpackSegment(uint64_t packed) {
	return IndexPointer(NumericCast<uint32_t>(SegmentBufferId(packed)), NumericCast<uint32_t>(packed & 0xFFFFFFFF));
}

//! Bytes of a block that WriteSegments leaves to the FixedSizeAllocator so that one segment fits per block: the
//! allocator puts an 8-byte validity word in front of the segment and rejects anything larger than block size minus
//! that word. 64 is more than it needs, but every existing file was written with this segment size, so it stays.
constexpr idx_t SEGMENT_HEADER_RESERVE = 64;

//! Mixing constants of the snapshot cache's visibility hash (FNV-1a style: xor the value, multiply by the 64-bit FNV
//! prime; the seed is arbitrary). The hash only compares decisions within one process, it is never persisted.
constexpr uint64_t VISIBILITY_HASH_SEED = 1469598103934665603ULL;
constexpr uint64_t VISIBILITY_HASH_PRIME = 1099511628211ULL;
} // namespace

namespace {

bool ReadBooleanOption(const case_insensitive_map_t<Value> &options, const char *name) {
	auto entry = options.find(name);
	if (entry == options.end() || entry->second.IsNull()) {
		return false;
	}
	return entry->second.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
}

bool ReadUndirectedOption(const case_insensitive_map_t<Value> &options) {
	return ReadBooleanOption(options, CSRIndex::UNDIRECTED_OPTION);
}

//! A stored reverse side is pointless for an undirected index: its forward partitions hold both directions
bool ReadReverseOption(const case_insensitive_map_t<Value> &options) {
	return ReadBooleanOption(options, CSRIndex::REVERSE_OPTION) && !ReadUndirectedOption(options);
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
		if (!expr->GetReturnType().IsIntegral() || physical == PhysicalType::UINT64 ||
		    physical == PhysicalType::INT128 || physical == PhysicalType::UINT128) {
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
	auto epoch = CSRIndex::BuildEpoch(gstate.context, gstate.lists, gstate.index->IsUndirected(), gstate.index->IsReverse(),
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
      undirected(ReadUndirectedOption(options)), reverse(ReadReverseOption(options)) {
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
	// Binding after a restart: load the checkpointed state when there is one, else the epoch is rebuilt from the table
	// on first use
	auto index = make_uniq<CSRIndex>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
	                                 input.unbound_expressions, input.db, input.options);
	index->DeferLoad(input.storage_info);
	return std::move(index);
}

shared_ptr<CSRIndexEpoch> CSRIndex::BuildEpoch(ClientContext &context, vector<CSRIndexEdgeList> &lists, bool undirected,
                                               bool reverse, VisibilityBound bound) {
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
	// The reverse side buckets (dst, src) by the source's range; it carries no row ids (lengths only)
	vector<vector<vector<BuildEdge>>> reverse_buckets(reverse ? thread_count : 0,
	                                                  vector<vector<BuildEdge>>(partition_count));
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
				} else if (reverse) {
					reverse_buckets[thread_idx][src / width].push_back({dst, NumericCast<uint16_t>(src % width), 0});
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
			csr->forward_partitions[partition_idx]->row_ids = row_ids.data();
			total_edge_count.fetch_add(edges.size(), std::memory_order_relaxed);
		}
	});
	csr->edge_count = total_edge_count.load();
	if (reverse) {
		csr->capabilities |= PartitionedCSRCapabilities::REVERSE;
		csr->reverse_partitions.resize(partition_count);
		ParallelFor(partition_count, thread_count, [&](idx_t begin, idx_t end, idx_t) {
			vector<BuildEdge> edges;
			vector<BuildEdge> scratch;
			vector<idx_t> histogram;
			for (idx_t partition_idx = begin; partition_idx < end; partition_idx++) {
				edges.clear();
				for (auto &buckets : reverse_buckets) {
					auto &bucket = buckets[partition_idx];
					edges.insert(edges.end(), bucket.begin(), bucket.end());
					bucket.clear();
					bucket.shrink_to_fit();
				}
				CSRRadixSortBySource(edges, scratch, histogram, src_bits);
				auto start_vertex = partition_idx * width;
				auto end_vertex = std::min<idx_t>(start_vertex + width, vertex_count + 2);
				csr->reverse_partitions[partition_idx] = CSREmitSparsePartition(
				    start_vertex, end_vertex, vertex_count, edges, [](idx_t, const BuildEdge &) {});
			}
		});
	}
	epoch->edge_count = csr->edge_count;
	epoch->csr = std::move(csr);
	return epoch;
}

void CSRIndex::InstallEpoch(shared_ptr<CSRIndexEpoch> epoch_p) {
	IndexLock l(*this);
	// CREATE INDEX builds a fresh index: no earlier state (or pending load) applies
	ClearStateLocked();
	epoch_p->generation = next_generation++;
	epochs.push_back(std::move(epoch_p));
}

void CSRIndex::ClearStateLocked() const {
	storage_pending = false;
	pending_storage = IndexStorageInfo();
	epochs.clear();
	delta.clear();
	tombstones.clear();
	delta_row_index.clear();
	cached_snapshot.reset();
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
	return BuildEpoch(context, lists, undirected, reverse, transaction.view.visibility_bound);
}

void CSRIndex::InstallEpochAndCompact(DuckTransaction &transaction, DataTable &table,
                                      shared_ptr<CSRIndexEpoch> epoch_p) const {
	// Decide, with the live transaction that built the epoch, which existing entries it accounts for. Entries appended
	// after this point are stamped by a later epoch. Only the fields the decision reads are copied under the lock.
	struct PendingDelta {
		row_t row;
		uint32_t group;
		uint32_t deleted_group;
	};
	vector<PendingDelta> delta_prefix;
	vector<row_t> tombstone_prefix;
	{
		IndexLock l(*this);
		EnsureLoadedLocked(l);
		delta_prefix.reserve(delta.size());
		for (auto &entry : delta) {
			delta_prefix.push_back({entry.row, entry.group, entry.deleted_group});
		}
		tombstone_prefix.reserve(tombstones.size());
		for (auto &tombstone : tombstones) {
			tombstone_prefix.push_back(tombstone.row);
		}
	}
	vector<uint8_t> delta_covered(delta_prefix.size());
	vector<uint8_t> tombstone_excluded(tombstone_prefix.size());
	// A row the folding transaction can fetch is in the new epoch. A deleted delta row whose insert the folding
	// transaction sees was deleted before the fold, so the new epoch accounts for it (by excluding it) as well;
	// the insert's visibility is that of any undeleted row of the same change group.
	unordered_map<uint32_t, bool> group_insert_visible;
	for (idx_t i = 0; i < delta_prefix.size(); i++) {
		auto &entry = delta_prefix[i];
		if (entry.group == CSRIndexDeltaEdge::DEAD_GROUP) {
			delta_covered[i] = true; // never visible: every epoch accounts for it by not containing it
			continue;
		}
		if (entry.group != 0 && entry.deleted_group == 0) {
			auto known = group_insert_visible.find(entry.group);
			if (known == group_insert_visible.end()) {
				known = group_insert_visible.emplace(entry.group, table.CanFetch(transaction, entry.row)).first;
			}
			delta_covered[i] = known->second;
			continue;
		}
		delta_covered[i] = table.CanFetch(transaction, entry.row);
	}
	for (idx_t i = 0; i < delta_prefix.size(); i++) {
		auto &entry = delta_prefix[i];
		if (!delta_covered[i] && entry.deleted_group != 0 && entry.group != 0) {
			auto known = group_insert_visible.find(entry.group);
			delta_covered[i] = known != group_insert_visible.end() && known->second;
		}
	}
	for (idx_t i = 0; i < tombstone_prefix.size(); i++) {
		tombstone_excluded[i] = !table.CanFetch(transaction, tombstone_prefix[i]);
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
	cached_snapshot.reset();
	CompactChangeLogLocked();
	// The new dictionary is rebuilt in key order: ids resolved against the previous one no longer apply
	ResolveDeltaIdsLocked();
}

void CSRIndex::CompactChangeLogLocked() const {
	if (epochs.empty()) {
		return;
	}
	// An entry the oldest retained epoch accounts for is accounted for by every retained epoch
	auto oldest_generation = epochs.front()->generation;
	vector<CSRIndexDeltaEdge> new_delta;
	for (auto &edge : delta) {
		if (edge.covered_epoch > oldest_generation && edge.group != CSRIndexDeltaEdge::DEAD_GROUP) {
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
	RebuildDeltaRowIndexLocked();
	for (auto &epoch : epochs) {
		epoch->tombstone_positions.clear();
		epoch->reverse_tombstone_positions.clear();
		epoch->mirror_tombstone_positions.clear();
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
	vector<CSRIndexTombstone> pending_entries;
	idx_t known;
	{
		IndexLock l(*this);
		known = epoch.tombstone_positions.size();
		if (known >= tombstone_count) {
			return;
		}
		for (idx_t i = known; i < tombstone_count && i < tombstones.size(); i++) {
			pending_entries.push_back(tombstones[i]);
		}
	}
	vector<row_t> pending;
	pending.reserve(pending_entries.size());
	// Forward position of each pending row, and for undirected indexes the position of the mirrored edge
	std::unordered_map<row_t, idx_t> positions;
	std::unordered_map<row_t, idx_t> mirror_positions;
	positions.reserve(pending_entries.size());
	// A tombstone with keys names its edge: the destination's partition, the source's sparse row, and the row id
	// among that row's edges (a few dozen comparisons). Rows without keys (loaded from storage) are found by one
	// scan over the epoch's row ids below.
	std::unordered_set<row_t> scan_rows;
	auto &dictionary = *epoch.dictionary;
	auto width = epoch.partition_width;
	auto &partition_row_ids = epoch.partition_row_ids;
	// The position of the edge src -> dst with row id `row`, or INVALID_INDEX when the epoch has no such edge. Both
	// BuildEpoch and the loader lay the partitions out as consecutive ranges of `width` ids over every vertex, so the
	// destination names its partition; anything else is a broken epoch.
	auto locate = [&](idx_t src, idx_t dst, row_t row) -> idx_t {
		auto partition_idx = dst / width;
		if (partition_idx >= epoch.csr->forward_partitions.size() || partition_idx >= partition_row_ids.size()) {
			throw InternalException("CSR index: vertex %llu has no partition in an epoch of %llu partitions", dst,
			                        epoch.csr->forward_partitions.size());
		}
		auto &forward = *epoch.csr->forward_partitions[partition_idx];
		if (forward.start_vertex != partition_idx * width) {
			throw InternalException("CSR index: partition %llu starts at vertex %llu, expected %llu", partition_idx,
			                        forward.start_vertex, partition_idx * width);
		}
		auto row_idx = forward.FindSparseRow(src);
		if (row_idx == std::numeric_limits<idx_t>::max() || row_idx + 1 >= forward.row_offsets.size()) {
			return DConstants::INVALID_INDEX;
		}
		auto &rows = partition_row_ids[partition_idx];
		for (idx_t o = forward.row_offsets[row_idx]; o < forward.row_offsets[row_idx + 1]; o++) {
			if (rows[o] == row) {
				return PackPosition(partition_idx, o);
			}
		}
		return DConstants::INVALID_INDEX;
	};
	for (auto &tombstone : pending_entries) {
		pending.push_back(tombstone.row);
		idx_t position = DConstants::INVALID_INDEX;
		idx_t mirror = DConstants::INVALID_INDEX;
		if (!tombstone.has_keys) {
			scan_rows.insert(tombstone.row);
		} else {
			auto src = dictionary.Lookup(tombstone.src_key);
			auto dst = dictionary.Lookup(tombstone.dst_key);
			// A vertex the epoch does not know means the row is not an epoch edge: the positions stay INVALID_INDEX
			if (src != DConstants::INVALID_INDEX && dst != DConstants::INVALID_INDEX) {
				position = locate(src, dst, tombstone.row);
				if (undirected && src != dst) {
					mirror = locate(dst, src, tombstone.row);
				}
			}
		}
		positions.emplace(tombstone.row, position);
		mirror_positions.emplace(tombstone.row, mirror);
	}
	if (!scan_rows.empty()) {
		// Every position of a scanned row: one for a directed edge, two for an undirected one (both directions carry
		// the row id)
		mutex positions_lock;
		std::unordered_map<row_t, vector<idx_t>> found_positions;
		auto thread_count = std::max<idx_t>(1, TaskScheduler::GetScheduler(db.GetDatabase()).NumberOfThreads());
		ParallelFor(partition_row_ids.size(), thread_count, [&](idx_t begin, idx_t end, idx_t) {
			vector<std::pair<row_t, idx_t>> found;
			for (idx_t partition_idx = begin; partition_idx < end; partition_idx++) {
				auto &rows = partition_row_ids[partition_idx];
				for (idx_t offset = 0; offset < rows.size(); offset++) {
					if (scan_rows.find(rows[offset]) != scan_rows.end()) {
						found.emplace_back(rows[offset], PackPosition(partition_idx, offset));
					}
				}
			}
			if (!found.empty()) {
				lock_guard<mutex> guard(positions_lock);
				for (auto &entry : found) {
					found_positions[entry.first].push_back(entry.second);
				}
			}
		});
		for (auto &entry : found_positions) {
			std::sort(entry.second.begin(), entry.second.end());
			positions[entry.first] = entry.second[0];
			if (entry.second.size() > 1) {
				mirror_positions[entry.first] = entry.second[1];
			}
		}
	}
	// The same rows on the reverse side: the forward position names the edge (its source row and destination
	// offset); the reverse partition of the source's range holds it in the destination's row
	vector<idx_t> reverse_positions;
	if (!epoch.csr->reverse_partitions.empty()) {
		reverse_positions.reserve(pending.size());
		for (auto row : pending) {
			auto position = positions[row];
			if (position == DConstants::INVALID_INDEX) {
				reverse_positions.push_back(DConstants::INVALID_INDEX);
				continue;
			}
			auto partition_idx = PositionPartition(position);
			auto offset = PositionOffset(position);
			auto &forward = *epoch.csr->forward_partitions[partition_idx];
			// The sparse row that holds `offset`: row_offsets is sorted, row r covers [row_offsets[r], row_offsets[r+1])
			auto row_it = std::upper_bound(forward.row_offsets.begin(), forward.row_offsets.end(), offset);
			auto row_idx = NumericCast<idx_t>(row_it - forward.row_offsets.begin()) - 1;
			auto src = forward.source_vertices[row_idx];
			auto dst = NumericCast<uint32_t>(forward.start_vertex + forward.e[offset]);
			auto &reverse_partition = *epoch.csr->reverse_partitions[src / width];
			auto reverse_row = reverse_partition.FindSparseRow(dst);
			idx_t reverse_position = DConstants::INVALID_INDEX;
			if (reverse_row != std::numeric_limits<idx_t>::max()) {
				auto local_src = NumericCast<uint16_t>(src - (src / width) * width);
				for (auto o = reverse_partition.row_offsets[reverse_row]; o < reverse_partition.row_offsets[reverse_row + 1]; o++) {
					if (reverse_partition.e[o] == local_src) {
						reverse_position = PackPosition(src / width, o);
						break;
					}
				}
			}
			reverse_positions.push_back(reverse_position);
		}
	}
	IndexLock l(*this);
	if (epoch.tombstone_positions.size() != known) {
		// Another reader resolved them meanwhile, or the log was compacted
		return;
	}
	for (idx_t i = 0; i < pending.size(); i++) {
		epoch.tombstone_positions.push_back(positions[pending[i]]);
		epoch.mirror_tombstone_positions.push_back(mirror_positions[pending[i]]);
		if (!reverse_positions.empty()) {
			epoch.reverse_tombstone_positions.push_back(reverse_positions[i]);
		}
	}
}

shared_ptr<CSRIndexSnapshot> CSRIndex::BuildSnapshot(ClientContext &context, DuckTransaction &transaction,
                                                     DataTable &table, shared_ptr<CSRIndexEpoch> epoch_p,
                                                     idx_t delta_count, idx_t tombstone_count, bool shareable) const {
	auto &epoch = *epoch_p;
	auto width = epoch.partition_width;
	auto epoch_vertex_count = epoch.vertex_count;

	// The change log this reader considers: the publish lengths copied by the caller
	vector<CSRIndexDeltaEdge> delta_edges;
	vector<row_t> tombstone_rows;
	vector<uint32_t> tombstone_groups;
	vector<idx_t> tombstone_positions;
	vector<idx_t> reverse_tombstone_positions;
	vector<idx_t> mirror_tombstone_positions;
	if (tombstone_count > 0) {
		ResolveTombstonePositions(epoch, tombstone_count);
	}
	{
		IndexLock l(*this);
		delta_count = std::min<idx_t>(delta_count, delta.size());
		delta_edges.assign(delta.begin(), delta.begin() + NumericCast<int64_t>(delta_count));
		tombstone_count = std::min<idx_t>(tombstone_count, epoch.tombstone_positions.size());
		for (idx_t i = 0; i < tombstone_count; i++) {
			tombstone_rows.push_back(tombstones[i].row);
			tombstone_groups.push_back(tombstones[i].group);
		}
		tombstone_positions.assign(epoch.tombstone_positions.begin(),
		                           epoch.tombstone_positions.begin() + NumericCast<int64_t>(tombstone_count));
		if (epoch.mirror_tombstone_positions.size() >= tombstone_count) {
			mirror_tombstone_positions.assign(epoch.mirror_tombstone_positions.begin(),
			                                  epoch.mirror_tombstone_positions.begin() +
			                                      NumericCast<int64_t>(tombstone_count));
		}
		if (epoch.reverse_tombstone_positions.size() >= tombstone_count) {
			reverse_tombstone_positions.assign(epoch.reverse_tombstone_positions.begin(),
			                                   epoch.reverse_tombstone_positions.begin() +
			                                       NumericCast<int64_t>(tombstone_count));
		}
	}

	// Pass 1: which change groups' commits does this reader see? One row-version check per group; rows without a
	// group (loaded from storage) and deleted delta rows are checked individually. Commits are serialized, so the
	// visible groups are a prefix of the group sequence (aborted groups are visible to nobody) and the newest
	// visible group identifies the visible set.
	unordered_map<uint32_t, bool> commit_visible;
	uint32_t newest_visible_group = 0;
	// Microbenchmark switch: decide every logged row by its own row version instead of once per change group
	bool per_row = !GetCSRIndexChangeGroups(context);
	auto group_commit_visible = [&](uint32_t group, bool visible_if_fetchable, row_t representative) {
		auto known = commit_visible.find(group);
		if (known == commit_visible.end()) {
			auto fetchable = table.CanFetch(transaction, representative);
			known = commit_visible.emplace(group, fetchable == visible_if_fetchable).first;
			if (known->second) {
				newest_visible_group = std::max(newest_visible_group, group);
			}
		}
		return known->second;
	};
	// Insert groups are decided on an undeleted row (a deleted row is unfetchable for two possible reasons)
	for (auto &entry : delta_edges) {
		if (!per_row && entry.group != 0 && entry.group != CSRIndexDeltaEdge::DEAD_GROUP && entry.deleted_group == 0) {
			group_commit_visible(entry.group, true, entry.row);
		}
	}
	vector<bool> delta_visible(delta_edges.size());
	for (idx_t i = 0; i < delta_edges.size(); i++) {
		auto &entry = delta_edges[i];
		bool insert_visible;
		if (entry.group == CSRIndexDeltaEdge::DEAD_GROUP) {
			delta_visible[i] = false;
			continue;
		}
		if (per_row) {
			// Insert visible when the row fetches; a deleted delta row is unfetchable for either reason, so the
			// deleting commit is checked through its group's visibility like the storage-loaded entries
			if (entry.deleted_group == 0) {
				delta_visible[i] = table.CanFetch(transaction, entry.row);
				continue;
			}
			delta_visible[i] = !group_commit_visible(entry.deleted_group, false, entry.row);
			continue;
		}
		if (entry.group == 0) {
			// Loaded from storage: committed before the restart, so visible unless deleted
			if (entry.deleted_group == 0) {
				delta_visible[i] = table.CanFetch(transaction, entry.row);
				continue;
			}
			insert_visible = true;
		} else {
			auto known = commit_visible.find(entry.group);
			if (known == commit_visible.end()) {
				// Every row of the group is deleted: this row's fetchability decides for it
				delta_visible[i] = table.CanFetch(transaction, entry.row);
				continue;
			}
			insert_visible = known->second;
		}
		if (!insert_visible) {
			delta_visible[i] = false;
		} else if (entry.deleted_group == 0) {
			delta_visible[i] = true;
		} else {
			// The insert is visible, so the row is unfetchable exactly when the reader sees the deleting commit
			delta_visible[i] = !group_commit_visible(entry.deleted_group, false, entry.row);
		}
	}
	vector<bool> tombstone_applied(tombstone_rows.size());
	for (idx_t i = 0; i < tombstone_rows.size(); i++) {
		if (tombstone_positions[i] == DConstants::INVALID_INDEX) {
			continue; // a row no retained epoch contains
		}
		// An epoch row is visible to every reader of the epoch, so it is not fetchable exactly when the reader sees
		// the deleting commit
		tombstone_applied[i] = (per_row || tombstone_groups[i] == 0)
		                           ? !table.CanFetch(transaction, tombstone_rows[i])
		                           : group_commit_visible(tombstone_groups[i], false, tombstone_rows[i]);
	}

	// The cache is a microbenchmark switch: off, every query packs its own overlay. Two readers share a snapshot only
	// when they see the same visible groups and made the same per-row decisions (a fully deleted change group is
	// decided per row, so the newest visible group alone does not identify the view; 28 Oct audit).
	shareable = shareable && GetPathFindingSnapshotCache(context);
	uint64_t visibility_hash = VISIBILITY_HASH_SEED;
	auto mix = [&](uint64_t value) {
		visibility_hash = (visibility_hash ^ value) * VISIBILITY_HASH_PRIME;
	};
	mix(newest_visible_group);
	for (idx_t i = 0; i < delta_visible.size(); i++) {
		mix(delta_visible[i] ? i * 2 + 1 : i * 2);
	}
	for (idx_t i = 0; i < tombstone_applied.size(); i++) {
		mix(tombstone_applied[i] ? i * 2 + 1 : i * 2);
	}
	mix(delta_visible.size());
	mix(tombstone_applied.size());
	if (shareable) {
		IndexLock l(*this);
		if (cached_snapshot && cached_snapshot->epoch == epoch_p && cached_snapshot_generation == epoch.generation &&
		    cached_snapshot_group == newest_visible_group && cached_snapshot_visibility == visibility_hash) {
			return cached_snapshot;
		}
	}

	auto snapshot = make_shared_ptr<CSRIndexSnapshot>();
	snapshot->undirected = undirected;
	snapshot->epoch = epoch_p;
	snapshot->partition_width = width;
	snapshot->partitions = epoch.csr->forward_partitions;
	snapshot->edge_count = epoch.edge_count;

	bool stored_reverse = !epoch.csr->reverse_partitions.empty();
	if (stored_reverse) {
		snapshot->reverse_partitions = epoch.csr->reverse_partitions;
	}

	// Tombstones: epoch edges deleted by commits this reader sees are suppressed, on both sides
	auto suppress = [&](std::vector<shared_ptr<vector<uint64_t>>> &suppressed, const std::vector<shared_ptr<LocalCSR>> &partitions,
	                    idx_t position) {
		auto partition_idx = PositionPartition(position);
		auto offset = PositionOffset(position);
		auto &bitmap = suppressed[partition_idx];
		if (!bitmap) {
			bitmap = make_shared_ptr<vector<uint64_t>>((partitions[partition_idx]->e.size() + 63) / 64);
		}
		(*bitmap)[offset >> 6] |= uint64_t(1) << (offset & 63);
	};
	if (tombstone_count > 0) {
		snapshot->suppressed.resize(snapshot->partitions.size());
		if (stored_reverse) {
			snapshot->reverse_suppressed.resize(snapshot->reverse_partitions.size());
		}
		for (idx_t i = 0; i < tombstone_count; i++) {
			if (!tombstone_applied[i]) {
				continue;
			}
			suppress(snapshot->suppressed, snapshot->partitions, tombstone_positions[i]);
			snapshot->edge_count--;
			if (i < mirror_tombstone_positions.size() && mirror_tombstone_positions[i] != DConstants::INVALID_INDEX) {
				// Undirected: the mirrored edge of the same row goes too
				suppress(snapshot->suppressed, snapshot->partitions, mirror_tombstone_positions[i]);
			}
			if (stored_reverse && i < reverse_tombstone_positions.size() &&
			    reverse_tombstone_positions[i] != DConstants::INVALID_INDEX) {
				suppress(snapshot->reverse_suppressed, snapshot->reverse_partitions, reverse_tombstone_positions[i]);
			}
		}
	}

	// Delta: edges committed after the epoch that this reader sees, bucketed by destination range
	struct DeltaEntry {
		uint32_t src;
		uint16_t local_dst;
		row_t row;
	};
	struct VisibleDeltaEdge {
		uint32_t src;
		uint32_t dst;
		row_t row;
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
	vector<VisibleDeltaEdge> visible_edges;
	for (idx_t i = 0; i < delta_edges.size(); i++) {
		if (!delta_visible[i]) {
			continue;
		}
		auto &edge = delta_edges[i];
		if (edge.covered_epoch <= epoch.generation) {
			// Already an edge of this epoch (the entry stays in the log for readers of an older retained epoch);
			// adding it again would traverse it twice (28 Oct audit)
			continue;
		}
		bool ids_valid = edge.id_generation == epoch.generation;
		auto src = ids_valid && edge.src_id < epoch_vertex_count ? edge.src_id : lookup_or_add(edge.src_key);
		auto dst = ids_valid && edge.dst_id < epoch_vertex_count ? edge.dst_id : lookup_or_add(edge.dst_key);
		visible_edges.push_back({src, dst, edge.row});
		if (undirected) {
			visible_edges.push_back({dst, src, edge.row});
		}
	}
	snapshot->vertex_count = epoch_vertex_count + snapshot->extra_keys.size();
	snapshot->edge_count += visible_edges.size();

	// Every destination range up to the snapshot's vertex count must have a partition, because the kernel clears and
	// checks the frontier per partition range. Ranges beyond the epoch get empty cover partitions; the part of the
	// epoch's last range above the epoch's vertex count gets one too.
	auto total_ranges = std::max<idx_t>(1, (snapshot->vertex_count + 2 + width - 1) / width);
	auto epoch_end = epoch_vertex_count + 2;
	auto snapshot_end = snapshot->vertex_count + 2;
	// Ranges beyond the epoch get empty cover partitions (same ranges on both sides, so one range -> list mapping)
	vector<idx_t> range_to_list(total_ranges, DConstants::INVALID_INDEX);
	auto add_covers = [&](std::vector<shared_ptr<LocalCSR>> &partitions, bool record) {
		auto epoch_ranges = partitions.size();
		if (record) {
			for (idx_t partition_idx = 0; partition_idx < epoch_ranges; partition_idx++) {
				range_to_list[partition_idx] = partition_idx;
			}
		}
		if (snapshot_end <= epoch_end) {
			return;
		}
		auto last_range_end = epoch_ranges * width;
		if (epoch_end < std::min(last_range_end, snapshot_end)) {
			auto cover = make_shared_ptr<LocalCSR>(epoch_end, std::min(last_range_end, snapshot_end),
			                                       snapshot->vertex_count, false);
			cover->v_array_size = 0;
			cover->sparse_rows_initialized = true;
			cover->initialized_e = true;
			partitions.push_back(std::move(cover));
		}
		for (idx_t range = epoch_ranges; range < total_ranges; range++) {
			auto start_vertex = range * width;
			auto end_vertex = std::min<idx_t>(start_vertex + width, snapshot_end);
			auto cover = make_shared_ptr<LocalCSR>(start_vertex, end_vertex, snapshot->vertex_count, false);
			cover->v_array_size = 0;
			cover->sparse_rows_initialized = true;
			cover->initialized_e = true;
			if (record) {
				range_to_list[range] = partitions.size();
			}
			partitions.push_back(std::move(cover));
		}
	};
	add_covers(snapshot->partitions, true);
	if (stored_reverse) {
		add_covers(snapshot->reverse_partitions, false);
		snapshot->reverse_delta_partitions.resize(snapshot->reverse_partitions.size());
		snapshot->reverse_suppressed.resize(snapshot->reverse_partitions.size());
	}
	snapshot->delta_partitions.resize(snapshot->partitions.size());
	snapshot->suppressed.resize(snapshot->partitions.size());

	// Bucket the visible edges by destination range, then sort and pack every range in parallel: the rebuild after
	// a commit is O(D) and this is where its time goes once visibility is decided per change group. The reverse side
	// packs the same edges with the endpoints swapped and without row ids.
	auto pack_companions = [&](const vector<VisibleDeltaEdge> &edges, std::vector<shared_ptr<LocalCSR>> &companions,
	                           bool keep_rows) {
		if (edges.empty()) {
			return;
		}
		auto thread_count = std::max<idx_t>(1, TaskScheduler::GetScheduler(context).NumberOfThreads());
		idx_t slice_count = std::min<idx_t>(thread_count, (edges.size() + EDGE_SLICE_SIZE - 1) / EDGE_SLICE_SIZE);
		slice_count = std::max<idx_t>(1, slice_count);
		auto slice_size = (edges.size() + slice_count - 1) / slice_count;
		vector<vector<idx_t>> slice_histograms(slice_count, vector<idx_t>(total_ranges, 0));
		ParallelFor(slice_count, thread_count, [&](idx_t begin, idx_t end, idx_t) {
			for (idx_t slice = begin; slice < end; slice++) {
				auto &histogram = slice_histograms[slice];
				auto first = slice * slice_size;
				auto last = std::min<idx_t>(edges.size(), first + slice_size);
				for (idx_t i = first; i < last; i++) {
					histogram[edges[i].dst / width]++;
				}
			}
		});
		// Offsets per range and per slice within the range (counting sort, stable within a slice)
		vector<idx_t> range_offsets(total_ranges + 1, 0);
		for (idx_t range = 0; range < total_ranges; range++) {
			idx_t count = 0;
			for (idx_t slice = 0; slice < slice_count; slice++) {
				auto slice_count_in_range = slice_histograms[slice][range];
				slice_histograms[slice][range] = range_offsets[range] + count;
				count += slice_count_in_range;
			}
			range_offsets[range + 1] = range_offsets[range] + count;
		}
		vector<DeltaEntry> bucketed(edges.size());
		ParallelFor(slice_count, thread_count, [&](idx_t begin, idx_t end, idx_t) {
			for (idx_t slice = begin; slice < end; slice++) {
				auto &cursor = slice_histograms[slice];
				auto first = slice * slice_size;
				auto last = std::min<idx_t>(edges.size(), first + slice_size);
				for (idx_t i = first; i < last; i++) {
					auto &edge = edges[i];
					auto range = edge.dst / width;
					bucketed[cursor[range]++] = {edge.src, NumericCast<uint16_t>(edge.dst - range * width), edge.row};
				}
			}
		});
		auto src_bits = CSRSourceBits(snapshot->vertex_count);
		ParallelFor(total_ranges, thread_count, [&](idx_t begin, idx_t end, idx_t) {
			vector<DeltaEntry> entries;
			vector<DeltaEntry> scratch;
			vector<idx_t> histogram;
			for (idx_t range = begin; range < end; range++) {
				auto first = range_offsets[range];
				auto last = range_offsets[range + 1];
				if (first == last) {
					continue;
				}
				entries.assign(bucketed.begin() + NumericCast<int64_t>(first),
				               bucketed.begin() + NumericCast<int64_t>(last));
				CSRRadixSortBySource(entries, scratch, histogram, src_bits);
				auto start_vertex = range * width;
				auto end_vertex = std::min<idx_t>(start_vertex + width, snapshot_end);
				vector<row_t> delta_rows(keep_rows ? entries.size() : 0);
				auto delta_partition = CSREmitSparsePartition(
				    start_vertex, end_vertex, snapshot->vertex_count, entries, [&](idx_t position, const DeltaEntry &entry) {
					    if (keep_rows) {
						    delta_rows[position] = entry.row;
					    }
				    });
				if (keep_rows) {
					delta_partition->owned_row_ids = std::move(delta_rows);
					delta_partition->row_ids = delta_partition->owned_row_ids.data();
				}
				companions[range_to_list[range]] = std::move(delta_partition);
			}
		});
	};
	pack_companions(visible_edges, snapshot->delta_partitions, true);
	if (stored_reverse) {
		vector<VisibleDeltaEdge> reverse_edges;
		reverse_edges.reserve(visible_edges.size());
		for (auto &edge : visible_edges) {
			reverse_edges.push_back({edge.dst, edge.src, edge.row});
		}
		pack_companions(reverse_edges, snapshot->reverse_delta_partitions, false);
	} else if (undirected) {
		// Both directions are in the forward side already: the reverse view is the same vectors
		snapshot->reverse_partitions = snapshot->partitions;
		snapshot->reverse_delta_partitions = snapshot->delta_partitions;
		snapshot->reverse_suppressed = snapshot->suppressed;
	}
	if (shareable) {
		IndexLock l(*this);
		cached_snapshot = snapshot;
		cached_snapshot_generation = epoch.generation;
		cached_snapshot_group = newest_visible_group;
		cached_snapshot_visibility = visibility_hash;
	}
	return snapshot;
}

void CSRIndex::Fold(ClientContext &context, DuckTransaction &transaction, DataTable &table) const {
	if (transaction.ChangesMade()) {
		throw InvalidInputException("A CSR index cannot be folded by a transaction with uncommitted changes");
	}
	lock_guard<mutex> fold_guard(fold_lock);
	{
		// Folds install epochs in visibility order. An older transaction that folds after a newer one would install
		// an older epoch as the newest, and compaction would then drop the log entries only the newer epoch covered,
		// hiding committed edges from new transactions (28 Oct audit). Readers that all saw the log grow past the
		// ratio take the same path, so only the first of them rebuilds.
		IndexLock l(*this);
		EnsureLoadedLocked(l);
		if (!epochs.empty()) {
			auto &newest = epochs.back()->bound;
			auto mine = transaction.view.visibility_bound;
			if (VisibilityBound::Min(newest, mine) == mine) {
				return;
			}
		}
	}
	auto fold_start = std::chrono::steady_clock::now();
	InstallEpochAndCompact(transaction, table, RebuildFromTable(context, transaction, table));
	auto fold_us = static_cast<uint64_t>(
	    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - fold_start).count());
	fold_count++;
	fold_total_us += fold_us;
	fold_last_us = fold_us;
}

string CSRIndex::FoldStats() const {
	return StringUtil::Format("folds=%llu;last_fold_ms=%.1f;fold_ms=%.1f", fold_count.load(),
	                          static_cast<double>(fold_last_us.load()) / 1000.0,
	                          static_cast<double>(fold_total_us.load()) / 1000.0);
}

shared_ptr<CSRIndexSnapshot> CSRIndex::Snapshot(ClientContext &context, DuckTransaction &transaction,
                                                DataTable &table) const {
	if (transaction.ChangesMade()) {
		// The reader's own uncommitted writes are visible to it alone: build a private epoch from its view of the
		// table (which includes them) and leave the shared epochs, which must only contain committed rows, untouched
		return BuildSnapshot(context, transaction, table, RebuildFromTable(context, transaction, table), 0, 0, false);
	}
	auto bound = transaction.view.visibility_bound;
	auto state = LogStateFor(bound);
	if (!state.have_epochs) {
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
		state = LogStateFor(bound);
	} else if (state.epoch && state.epoch == epochs.back()) {
		// Current readers fold when the change log has grown past the configured fraction of the epoch
		auto ratio = GetCSRIndexFoldRatio(context);
		if (ratio > 0 && static_cast<double>(state.delta_count + state.tombstone_count) >
		                     ratio * static_cast<double>(std::max<idx_t>(1, state.epoch->edge_count))) {
			Fold(context, transaction, table);
			state = LogStateFor(bound);
		}
	}
	if (!state.epoch) {
		// Every retained epoch is newer than this reader's snapshot
		return nullptr;
	}
	return BuildSnapshot(context, transaction, table, state.epoch, state.delta_count, state.tombstone_count, true);
}

CSRIndex::LogState CSRIndex::LogStateFor(VisibilityBound bound) const {
	return WithLoadedState([&](IndexLock &) {
		LogState state;
		state.epoch = EpochForLocked(bound);
		state.have_epochs = !epochs.empty();
		state.delta_count = delta.size();
		state.tombstone_count = tombstones.size();
		return state;
	});
}

idx_t CSRIndex::InMemoryBytes() const {
	return WithLoadedState([&](IndexLock &l) { return GetInMemorySize(l); });
}

idx_t CSRIndex::EpochEdgeCount() const {
	return WithLoadedState([&](IndexLock &) { return epochs.empty() ? 0 : epochs.back()->edge_count; });
}

idx_t CSRIndex::DeltaSize() const {
	return WithLoadedState([&](IndexLock &) { return delta.size(); });
}

idx_t CSRIndex::TombstoneCount() const {
	return WithLoadedState([&](IndexLock &) { return tombstones.size(); });
}

idx_t CSRIndex::EpochCount() const {
	return WithLoadedState([&](IndexLock &) { return epochs.size(); });
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
optional_ptr<DuckTableEntry> CSRIndex::FindTable(ClientContext &context, const QualifiedName &name) {
	auto entry = Catalog::GetEntry<TableCatalogEntry>(context, name, OnEntryNotFound::RETURN_NULL);
	if (!entry || !entry->IsDuckTable()) {
		return nullptr;
	}
	return &entry->Cast<DuckTableEntry>();
}

optional_ptr<DuckTableEntry> CSRIndex::FindTable(ClientContext &context, const string &table_name) {
	return FindTable(context, QualifiedName::Parse(table_name));
}

optional_ptr<DuckTableEntry> CSRIndex::FindTable(ClientContext &context, const PropertyGraphTable &table) {
	return FindTable(context, QualifiedName(table.catalog_name, table.schema_name, table.table_name));
}

shared_ptr<IndexEntry> CSRIndex::FindIndexEntry(ClientContext &context, DuckTableEntry &table,
                                                const string &index_name) {
	auto &storage = table.GetStorage();
	storage.BindIndexes(context);
	auto entry = storage.GetDataTableInfo()->GetIndexes().FindEntry(Identifier(index_name));
	if (!entry || entry->GetIndexType() != TYPE_NAME || entry->GetBindState() != IndexBindState::BOUND) {
		return nullptr;
	}
	return entry;
}

string CSRIndex::FindForEdgeTable(ClientContext &context, const PropertyGraphTable &edge_table, bool undirected) {
	if (edge_table.source_fk.size() != 1 || edge_table.destination_fk.size() != 1) {
		return string();
	}
	auto table = FindTable(context, edge_table);
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

shared_ptr<CSRIndexSnapshot> CSRIndex::Resolve(ClientContext &context, const string &catalog, const string &schema,
                                               const string &table_name, const string &index_name) {
	auto table = FindTable(context, QualifiedName(Identifier(catalog), Identifier(schema), Identifier(table_name)));
	if (!table) {
		return nullptr;
	}
	auto index_entry = FindIndexEntry(context, *table, index_name);
	if (!index_entry) {
		return nullptr;
	}
	auto handle = index_entry->GetReadHandle<CSRIndex>();
	auto &transaction = DuckTransaction::Get(context, table->catalog);
	return handle->Snapshot(context, transaction, table->GetStorage());
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
	EnsureLoadedLocked(l);
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
	// One call appends the rows of one commit chunk of one transaction: one change group
	auto group = next_change_group++;
	// Resolve the keys once here rather than in every reader's snapshot; unknown keys stay unresolved
	auto dictionary = epochs.empty() ? nullptr : epochs.back()->dictionary.get();
	auto id_generation = epochs.empty() ? NumericLimits<uint32_t>::Maximum() : epochs.back()->generation;
	auto resolve = [&](int64_t key) -> uint32_t {
		if (!dictionary) {
			return NumericLimits<uint32_t>::Maximum();
		}
		auto id = dictionary->Lookup(key);
		return id == DConstants::INVALID_INDEX ? NumericLimits<uint32_t>::Maximum() : NumericCast<uint32_t>(id);
	};
	for (idx_t row = 0; row < count; row++) {
		if (!src_valid[row] || !dst_valid[row]) {
			continue;
		}
		auto row_id = row_data[row_format.sel->get_index(row)];
		auto previous = delta_row_index.find(row_id);
		if (previous != delta_row_index.end()) {
			// The row id is reused: its previous append was reverted (a commit that failed after the index logged it)
			// and the revert reached the index as a delete. That entry must never become visible, which it would once
			// this row commits and its row id fetches again (28 Oct audit); mark it dead.
			delta[previous->second].group = CSRIndexDeltaEdge::DEAD_GROUP;
		}
		delta_row_index[row_id] = delta.size();
		delta.push_back(CSRIndexDeltaEdge::Appended(src_keys[row], dst_keys[row], row_id, group, resolve(src_keys[row]),
		                                            resolve(dst_keys[row]), id_generation));
	}
	return ErrorData();
}

void CSRIndex::ResolveDeltaIdsLocked() const {
	if (epochs.empty()) {
		return;
	}
	auto &dictionary = *epochs.back()->dictionary;
	auto generation = epochs.back()->generation;
	for (auto &entry : delta) {
		auto src = dictionary.Lookup(entry.src_key);
		auto dst = dictionary.Lookup(entry.dst_key);
		entry.src_id =
		    src == DConstants::INVALID_INDEX ? NumericLimits<uint32_t>::Maximum() : NumericCast<uint32_t>(src);
		entry.dst_id =
		    dst == DConstants::INVALID_INDEX ? NumericLimits<uint32_t>::Maximum() : NumericCast<uint32_t>(dst);
		entry.id_generation = generation;
	}
}

ErrorData CSRIndex::Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids) {
	return Append(l, chunk, row_ids);
}

idx_t CSRIndex::TryDelete(IndexLock &l, DataChunk &entries, Vector &row_identifiers,
                          optional_ptr<SelectionVector> deleted_sel, optional_ptr<SelectionVector>) {
	l.AssertHeld(*this);
	EnsureLoadedLocked(l);
	auto count = entries.size();
	UnifiedVectorFormat row_format;
	row_identifiers.ToUnifiedFormat(count, row_format);
	auto row_data = UnifiedVectorFormat::GetData<row_t>(row_format);
	// The deleted rows' keys, so that a tombstone's epoch position is a lookup rather than a scan
	DataChunk key_chunk;
	key_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
	ExecuteExpressions(entries, key_chunk);
	vector<int64_t> src_keys;
	vector<int64_t> dst_keys;
	vector<uint8_t> src_valid;
	vector<uint8_t> dst_valid;
	ReadKeys(key_chunk.data[0], key_chunk.size(), src_keys, src_valid);
	ReadKeys(key_chunk.data[1], key_chunk.size(), dst_keys, dst_valid);
	bool keys_available = key_chunk.size() == count;
	auto group = next_change_group++;
	for (idx_t row = 0; row < count; row++) {
		auto row_id = row_data[row_format.sel->get_index(row)];
		auto delta_entry = delta_row_index.find(row_id);
		if (delta_entry != delta_row_index.end()) {
			// A delta row: readers drop it once they see this deleting commit
			delta[delta_entry->second].deleted_group = group;
		} else {
			// An epoch row (or one the index never saw); its position is resolved lazily by the readers
			bool has_keys = keys_available && src_valid[row] && dst_valid[row];
			tombstones.push_back({row_id, NumericLimits<uint32_t>::Maximum(), group, has_keys,
			                      has_keys ? src_keys[row] : 0, has_keys ? dst_keys[row] : 0});
		}
		if (deleted_sel) {
			deleted_sel->set_index(row, row);
		}
	}
	return count;
}

void CSRIndex::ResetStorage(IndexLock &l) {
	l.AssertHeld(*this);
	ClearStateLocked();
}

void CSRIndex::RebuildDeltaRowIndexLocked() const {
	delta_row_index.clear();
	delta_row_index.reserve(delta.size());
	for (idx_t i = 0; i < delta.size(); i++) {
		delta_row_index[delta[i].row] = i;
	}
}

bool CSRIndex::MergeIndexes(IndexLock &, BoundIndex &) {
	return false;
}

void CSRIndex::Vacuum(IndexLock &) {
}

namespace {
//! Bytes of one partition's arrays: edges, sparse rows with their offsets and bucket index, owned row ids (epoch
//! partitions point at the epoch's row ids instead, counted separately)
idx_t PartitionBytes(const LocalCSR &partition) {
	return partition.e.size() * sizeof(uint16_t) + partition.row_offsets.size() * sizeof(uint32_t) +
	       partition.source_vertices.size() * sizeof(uint32_t) + partition.row_buckets.size() * sizeof(uint32_t) +
	       partition.owned_row_ids.size() * sizeof(row_t);
}
} // namespace

idx_t CSRIndex::GetInMemorySize(IndexLock &l) const {
	l.AssertHeld(*this);
	if (storage_pending) {
		// Not deserialized yet: the serialized size is the estimate, and DuckDB's accounting must not force the load
		auto byte_count = pending_storage.options.find(STORAGE_OPTION_BYTES);
		return byte_count == pending_storage.options.end() ? 0 : byte_count->second.GetValue<uint64_t>();
	}
	idx_t bytes = delta.size() * sizeof(CSRIndexDeltaEdge) + tombstones.size() * sizeof(CSRIndexTombstone);
	// The delta row index: one bucket pointer per bucket, one node (next pointer plus the pair) per entry
	bytes += delta_row_index.bucket_count() * sizeof(void *) +
	         delta_row_index.size() * (sizeof(void *) + sizeof(std::pair<const row_t, idx_t>));
	for (auto &epoch : epochs) {
		bytes += (epoch->tombstone_positions.size() + epoch->reverse_tombstone_positions.size() +
		          epoch->mirror_tombstone_positions.size()) *
		         sizeof(idx_t);
		bytes += epoch->dictionary->bitmap.size() * sizeof(uint64_t) +
		         epoch->dictionary->word_rank.size() * sizeof(uint32_t) +
		         epoch->dictionary->sorted_keys.size() * sizeof(int64_t);
		for (auto &partition : epoch->csr->forward_partitions) {
			bytes += PartitionBytes(*partition);
		}
		for (auto &partition : epoch->csr->reverse_partitions) {
			bytes += PartitionBytes(*partition);
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
	EnsureLoadedLocked(l);
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

//----------------------------------------------------------------------------------------------------------------------
// Persistence
//----------------------------------------------------------------------------------------------------------------------
namespace {

struct ByteWriter {
	vector<uint8_t> bytes;

	template <class T>
	void Write(const T &value) {
		auto offset = bytes.size();
		bytes.resize(offset + sizeof(T));
		memcpy(bytes.data() + offset, &value, sizeof(T));
	}
	//! A length-prefixed array; duckdb::vector<T> derives from std::vector<T>, so this takes both
	template <class T>
	void WriteVector(const std::vector<T> &values) {
		Write<uint64_t>(values.size());
		auto offset = bytes.size();
		bytes.resize(offset + values.size() * sizeof(T));
		if (!values.empty()) {
			memcpy(bytes.data() + offset, values.data(), values.size() * sizeof(T));
		}
	}
};

//! Reads the serialized state from a byte range; several readers may read disjoint parts of one range in parallel
struct ByteReader {
	const uint8_t *bytes;
	idx_t size;
	idx_t position = 0;

	ByteReader(const uint8_t *bytes_p, idx_t size_p) : bytes(bytes_p), size(size_p) {
	}
	template <class T>
	T Read() {
		if (position + sizeof(T) > size) {
			throw SerializationException("Truncated CSR index storage");
		}
		T value;
		memcpy(&value, bytes + position, sizeof(T));
		position += sizeof(T);
		return value;
	}
	//! Advances over a length-prefixed vector of T without reading it
	template <class T>
	void SkipVector() {
		auto count = Read<uint64_t>();
		if (position + count * sizeof(T) > size) {
			throw SerializationException("Truncated CSR index storage");
		}
		position += count * sizeof(T);
	}
	//! A length-prefixed array into a std::vector or a duckdb::vector (which derives from it). assign() copies once;
	//! resize() followed by memcpy would write the memory twice
	template <class T>
	void ReadVector(std::vector<T> &values) {
		auto count = Read<uint64_t>();
		if (position + count * sizeof(T) > size) {
			throw SerializationException("Truncated CSR index storage");
		}
		auto data = reinterpret_cast<const T *>(bytes + position);
		values.assign(data, data + count);
		position += count * sizeof(T);
	}
};

//! One partition of the stream: start and end vertex, sparse rows, their edge offsets, the edges, and (forward side
//! only) the row ids aligned with the edges. The three functions below must agree on this layout.
void WritePartition(ByteWriter &writer, const LocalCSR &partition, const vector<row_t> *row_ids) {
	writer.Write<uint64_t>(partition.start_vertex);
	writer.Write<uint64_t>(partition.end_vertex);
	writer.WriteVector<uint32_t>(partition.source_vertices);
	writer.WriteVector<uint32_t>(partition.row_offsets);
	writer.WriteVector<uint16_t>(partition.e);
	if (row_ids) {
		writer.WriteVector<row_t>(*row_ids);
	}
}

//! Advances over a partition written by WritePartition
void SkipPartition(ByteReader &reader, bool with_row_ids) {
	reader.Read<uint64_t>();
	reader.Read<uint64_t>();
	reader.SkipVector<uint32_t>();
	reader.SkipVector<uint32_t>();
	reader.SkipVector<uint16_t>();
	if (with_row_ids) {
		reader.SkipVector<row_t>();
	}
}

//! Reads a partition written by WritePartition as a finalized LocalCSR (sparse rows and bucket index, no vertex
//! array); the row ids go to `*row_ids`, which the partition then points at
shared_ptr<LocalCSR> ReadPartition(ByteReader &reader, idx_t vertex_count, vector<row_t> *row_ids) {
	auto start_vertex = reader.Read<uint64_t>();
	auto end_vertex = reader.Read<uint64_t>();
	auto partition = make_shared_ptr<LocalCSR>(start_vertex, end_vertex, vertex_count, false);
	reader.ReadVector<uint32_t>(partition->source_vertices);
	reader.ReadVector<uint32_t>(partition->row_offsets);
	reader.ReadVector<uint16_t>(partition->e);
	if (row_ids) {
		reader.ReadVector<row_t>(*row_ids);
		partition->row_ids = row_ids->data();
	}
	partition->expected_sparse_row_count = partition->source_vertices.size();
	partition->v_array_size = 0;
	partition->initialized_e = true;
	partition->sparse_rows_initialized = true;
	partition->BuildRowBuckets(vertex_count);
	return partition;
}

//! Reads `count` partitions: a skip pass over `reader` locates every partition's bytes, then the partitions are
//! deserialized in parallel, each by its own reader over the same bytes. Forward partitions pass `row_ids`, which
//! receives one row-id vector per partition; the reverse side passes nullptr.
void ReadPartitions(ByteReader &reader, idx_t count, idx_t vertex_count, idx_t thread_count,
                    std::vector<shared_ptr<LocalCSR>> &partitions, vector<vector<row_t>> *row_ids) {
	vector<idx_t> offsets(count);
	for (idx_t partition_idx = 0; partition_idx < count; partition_idx++) {
		offsets[partition_idx] = reader.position;
		SkipPartition(reader, row_ids != nullptr);
	}
	partitions.resize(count);
	if (row_ids) {
		row_ids->resize(count);
	}
	ParallelFor(count, thread_count, [&](idx_t begin, idx_t end, idx_t) {
		for (idx_t partition_idx = begin; partition_idx < end; partition_idx++) {
			ByteReader partition_reader(reader.bytes, reader.size);
			partition_reader.position = offsets[partition_idx];
			partitions[partition_idx] =
			    ReadPartition(partition_reader, vertex_count, row_ids ? &(*row_ids)[partition_idx] : nullptr);
		}
	});
}

} // namespace

vector<uint8_t> CSRIndex::SerializeState() const {
	IndexLock l(*this);
	EnsureLoadedLocked(l);
	ByteWriter writer;
	if (epochs.empty()) {
		return writer.bytes;
	}
	auto &epoch = *epochs.back();
	writer.Write<uint32_t>(STORAGE_FORMAT_VERSION);
	writer.Write<uint8_t>(undirected ? 1 : 0);
	writer.Write<uint64_t>(epoch.vertex_count);
	writer.Write<uint64_t>(epoch.edge_count);
	writer.Write<uint64_t>(epoch.partition_width);

	auto &dictionary = *epoch.dictionary;
	writer.Write<uint8_t>(dictionary.dense ? 1 : 0);
	writer.Write<uint8_t>(dictionary.identity ? 1 : 0);
	writer.Write<int64_t>(dictionary.min_key);
	writer.Write<uint64_t>(dictionary.count);
	writer.WriteVector<uint64_t>(dictionary.bitmap);
	writer.WriteVector<uint32_t>(dictionary.word_rank);
	writer.WriteVector<int64_t>(dictionary.sorted_keys);

	auto &partitions = epoch.csr->forward_partitions;
	writer.Write<uint64_t>(partitions.size());
	for (idx_t partition_idx = 0; partition_idx < partitions.size(); partition_idx++) {
		WritePartition(writer, *partitions[partition_idx], &epoch.partition_row_ids[partition_idx]);
	}
	// Version 2: the reverse side (edges by source range, no row ids), present when the index stores one
	auto &reverse_partitions = epoch.csr->reverse_partitions;
	writer.Write<uint8_t>(reverse_partitions.empty() ? 0 : 1);
	if (!reverse_partitions.empty()) {
		writer.Write<uint64_t>(reverse_partitions.size());
		for (auto &partition : reverse_partitions) {
			WritePartition(writer, *partition, nullptr);
		}
	}

	// Only the change-log entries the persisted epoch does not account for; the stamps restart from zero on load
	vector<CSRIndexDeltaEdge> pending_delta;
	for (auto &edge : delta) {
		if (edge.covered_epoch > epoch.generation && edge.group != CSRIndexDeltaEdge::DEAD_GROUP) {
			pending_delta.push_back(edge);
		}
	}
	writer.Write<uint64_t>(pending_delta.size());
	for (auto &edge : pending_delta) {
		writer.Write<int64_t>(edge.src_key);
		writer.Write<int64_t>(edge.dst_key);
		writer.Write<int64_t>(edge.row);
	}
	vector<row_t> pending_tombstones;
	for (auto &tombstone : tombstones) {
		if (tombstone.excluded_epoch > epoch.generation) {
			pending_tombstones.push_back(tombstone.row);
		}
	}
	writer.WriteVector<row_t>(pending_tombstones);
	return writer.bytes;
}

IndexStorageInfo CSRIndex::WriteSegments(const vector<uint8_t> &bytes, optional_ptr<QueryContext> checkpoint_context) {
	IndexStorageInfo info(name);
	if (bytes.empty()) {
		return info;
	}
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	// One segment per block (see SEGMENT_HEADER_RESERVE); the loader takes the size from the allocator info
	auto segment_size = block_manager.GetBlockSize() - SEGMENT_HEADER_RESERVE;
	storage_allocator = make_uniq<FixedSizeAllocator>(segment_size, block_manager, MemoryTag::EXTENSION);
	vector<Value> segments;
	for (idx_t offset = 0; offset < bytes.size(); offset += segment_size) {
		auto length = std::min<idx_t>(segment_size, bytes.size() - offset);
		auto pointer = storage_allocator->New();
		memcpy(storage_allocator->Get(pointer, true), bytes.data() + offset, length);
		segments.push_back(Value::UBIGINT(PackSegment(pointer)));
	}
	if (checkpoint_context) {
		PartialBlockManager partial_block_manager(*checkpoint_context, block_manager,
		                                          PartialBlockType::FULL_CHECKPOINT);
		storage_allocator->SerializeBuffers(partial_block_manager);
		partial_block_manager.FlushPartialBlocks();
	} else {
		info.buffers.push_back(storage_allocator->InitSerializationToWAL());
	}
	info.allocator_infos.push_back(storage_allocator->GetInfo());
	info.options[STORAGE_OPTION_FORMAT] = Value::UBIGINT(STORAGE_FORMAT_VERSION);
	info.options[STORAGE_OPTION_BYTES] = Value::UBIGINT(bytes.size());
	info.options[STORAGE_OPTION_SEGMENTS] = Value::LIST(LogicalType::UBIGINT, std::move(segments));
	return info;
}

void CSRIndex::DeferLoad(const IndexStorageInfo &info) {
	if (!info.IsValid() || info.allocator_infos.empty()) {
		return;
	}
	auto format = info.options.find(STORAGE_OPTION_FORMAT);
	auto byte_count = info.options.find(STORAGE_OPTION_BYTES);
	auto segment_list = info.options.find(STORAGE_OPTION_SEGMENTS);
	if (format == info.options.end() || byte_count == info.options.end() || segment_list == info.options.end()) {
		return;
	}
	if (format->second.GetValue<uint64_t>() != STORAGE_FORMAT_VERSION) {
		// Unknown layout: start empty and rebuild from the table on first use
		return;
	}
	// IndexStorageInfo is not copyable; the fields are (the WAL's transient block handles are shared)
	IndexStorageInfo copy(info.name);
	copy.root = info.root;
	copy.options = info.options;
	copy.allocator_infos = info.allocator_infos;
	copy.buffers = info.buffers;
	copy.root_block_ptr = info.root_block_ptr;
	pending_storage = std::move(copy);
	storage_pending = true;
}

void CSRIndex::EnsureLoadedLocked(IndexLock &l) const {
	if (!storage_pending) {
		return;
	}
	storage_pending = false;
	auto info = std::move(pending_storage);
	pending_storage = IndexStorageInfo();
	LoadFromStorageLocked(l, info);
}

void CSRIndex::LoadFromStorageLocked(IndexLock &l, const IndexStorageInfo &info) const {
	l.AssertHeld(*this);
	auto byte_count = info.options.find(STORAGE_OPTION_BYTES);
	auto segment_list = info.options.find(STORAGE_OPTION_SEGMENTS);
	auto &block_manager = table_io_manager.GetIndexBlockManager();
	auto &allocator_info = info.allocator_infos[0];
	auto total_bytes = byte_count->second.GetValue<uint64_t>();
	// Uninitialized staging buffer: every byte is written by the segment copy below
	auto bytes_buffer = make_unsafe_uniq_array_uninitialized<uint8_t>(total_bytes);
	auto bytes = bytes_buffer.get();
	vector<uint64_t> packed_segments;
	for (auto &segment : ListValue::GetChildren(segment_list->second)) {
		packed_segments.push_back(segment.GetValue<uint64_t>());
	}
	auto thread_count = std::max<idx_t>(1, TaskScheduler::GetScheduler(db.GetDatabase()).NumberOfThreads());
	auto segment_size = allocator_info.segment_size;
	// One segment per block: the blocks are pinned and copied in parallel (each buffer has its own lock), in rounds
	// of a bounded number of blocks. A FixedSizeAllocator keeps every block it has handed out in memory until it is
	// destroyed, so one allocator for the whole index would hold the serialized bytes twice (blocks and staging
	// buffer) on top of the deserialized structures while loading (SF300: 1.2 GB more peak RSS for the undirected
	// index). Each round gets an allocator over its own blocks only, so every block is registered, loaded and
	// released once. WAL replay hands over transient block handles that Init moves out of the shared vector, so that
	// case runs as one round over the full info.
	unordered_map<idx_t, idx_t> buffer_positions;
	for (idx_t i = 0; i < allocator_info.buffer_ids.size(); i++) {
		buffer_positions[allocator_info.buffer_ids[i]] = i;
	}
	idx_t round_segments = allocator_info.transient_block_handles ? packed_segments.size()
	                                                               : std::max<idx_t>(thread_count * 8, 256);
	for (idx_t round_begin = 0; round_begin < packed_segments.size(); round_begin += round_segments) {
		auto round_end = std::min<idx_t>(packed_segments.size(), round_begin + round_segments);
		storage_allocator = make_uniq<FixedSizeAllocator>(segment_size, block_manager, MemoryTag::EXTENSION);
		if (allocator_info.transient_block_handles) {
			storage_allocator->Init(allocator_info);
		} else {
			FixedSizeAllocatorInfo round_info;
			round_info.segment_size = segment_size;
			for (idx_t segment_idx = round_begin; segment_idx < round_end; segment_idx++) {
				auto buffer_id = SegmentBufferId(packed_segments[segment_idx]);
				auto position = buffer_positions.find(buffer_id);
				if (position == buffer_positions.end()) {
					throw SerializationException("CSR index storage refers to an unknown buffer");
				}
				auto i = position->second;
				round_info.buffer_ids.push_back(allocator_info.buffer_ids[i]);
				round_info.block_pointers.push_back(allocator_info.block_pointers[i]);
				round_info.segment_counts.push_back(allocator_info.segment_counts[i]);
				round_info.allocation_sizes.push_back(allocator_info.allocation_sizes[i]);
			}
			storage_allocator->Init(round_info);
		}
		ParallelFor(round_end - round_begin, thread_count, [&](idx_t begin, idx_t end, idx_t) {
			for (idx_t i = begin; i < end; i++) {
				auto segment_idx = round_begin + i;
				auto pointer = UnpackSegment(packed_segments[segment_idx]);
				auto offset = segment_idx * segment_size;
				if (offset >= total_bytes) {
					throw SerializationException("CSR index storage has more segments than bytes");
				}
				auto length = std::min<idx_t>(segment_size, total_bytes - offset);
				memcpy(bytes + offset, storage_allocator->Get(pointer, false), length);
			}
		});
		// The copied blocks are no longer needed in memory; they stay referenced on disk until the next serialization
		storage_allocator.reset();
	}

	ByteReader reader(bytes, total_bytes);
	// DeferLoad only accepts the current version in the options; the stream must agree
	auto stored_version = reader.Read<uint32_t>();
	if (stored_version != STORAGE_FORMAT_VERSION) {
		throw SerializationException("CSR index storage format mismatch");
	}
	auto stored_undirected = reader.Read<uint8_t>() != 0;
	if (stored_undirected != undirected) {
		throw SerializationException("CSR index direction does not match its stored state");
	}
	auto epoch = make_shared_ptr<CSRIndexEpoch>();
	epoch->vertex_count = reader.Read<uint64_t>();
	epoch->edge_count = reader.Read<uint64_t>();
	epoch->partition_width = reader.Read<uint64_t>();

	auto dictionary = make_shared_ptr<CSRIndexDictionary>();
	dictionary->dense = reader.Read<uint8_t>() != 0;
	dictionary->identity = reader.Read<uint8_t>() != 0;
	dictionary->min_key = reader.Read<int64_t>();
	dictionary->count = reader.Read<uint64_t>();
	reader.ReadVector<uint64_t>(dictionary->bitmap);
	reader.ReadVector<uint32_t>(dictionary->word_rank);
	reader.ReadVector<int64_t>(dictionary->sorted_keys);
	epoch->dictionary = std::move(dictionary);

	auto csr = make_shared_ptr<PartitionedCSRIndex>();
	csr->vertex_count = epoch->vertex_count + 2;
	csr->edge_count = epoch->edge_count;
	csr->capabilities = PartitionedCSRCapabilities::FORWARD;
	auto partition_count = reader.Read<uint64_t>();
	ReadPartitions(reader, partition_count, epoch->vertex_count, thread_count, csr->forward_partitions,
	               &epoch->partition_row_ids);
	// The reverse side (edges by source range, no row ids), present when the index stored one
	bool stored_reverse = reader.Read<uint8_t>() != 0;
	if (stored_reverse) {
		auto reverse_count = reader.Read<uint64_t>();
		ReadPartitions(reader, reverse_count, epoch->vertex_count, thread_count, csr->reverse_partitions, nullptr);
		csr->capabilities |= PartitionedCSRCapabilities::REVERSE;
	}
	epoch->csr = std::move(csr);

	// After a restart every persisted row is committed, so the epoch serves every transaction (default bound)
	auto delta_count = reader.Read<uint64_t>();
	vector<CSRIndexDeltaEdge> loaded_delta;
	for (idx_t i = 0; i < delta_count; i++) {
		auto src = reader.Read<int64_t>();
		auto dst = reader.Read<int64_t>();
		auto row = reader.Read<int64_t>();
		loaded_delta.push_back(CSRIndexDeltaEdge::Loaded(src, dst, row));
	}
	vector<row_t> loaded_tombstones;
	reader.ReadVector<row_t>(loaded_tombstones);
	// The staging buffer is returned to the system here; the blocks' pages went back to jemalloc, which keeps them
	// dirty for its decay period (SF300: 1.2 GiB of RSS over the index for seconds to minutes) unless purged
	bytes_buffer.reset();
	Allocator::FlushAll();

	if (reverse && !stored_reverse) {
		// Stored before the index had a reverse side: the first reader rebuilds the epoch from the table
		ClearStateLocked();
		return;
	}
	epoch->generation = next_generation++;
	epochs.clear();
	epochs.push_back(std::move(epoch));
	// Loaded entries carry no change group (0): readers decide their visibility per row until the next fold
	delta = std::move(loaded_delta);
	RebuildDeltaRowIndexLocked();
	tombstones.clear();
	for (auto row : loaded_tombstones) {
		tombstones.push_back({row, NumericLimits<uint32_t>::Maximum(), 0});
	}
	cached_snapshot.reset();
}

IndexStorageInfo CSRIndex::SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &) {
	return WriteSegments(SerializeState(), &context);
}

IndexStorageInfo CSRIndex::SerializeToWAL(const case_insensitive_map_t<Value> &) {
	return WriteSegments(SerializeState(), nullptr);
}

//----------------------------------------------------------------------------------------------------------------------
// Registration
//----------------------------------------------------------------------------------------------------------------------
void CorePGQIndex::Register(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.GetIndexTypes().RegisterIndexType(CSRIndex::GetCSRIndexType());
}

} // namespace duckdb
