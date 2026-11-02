//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/index/csr_index.hpp
//
// A snapshot-consistent adjacency (CSR) index on an edge table, registered as a
// DuckDB extension index type: CREATE INDEX name ON edges USING CSR (src, dst)
// [WITH (undirected = true)].
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <mutex>

#include "duckpgq/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/index_lock.hpp"
#include "duckdb/execution/index/index_type.hpp"

#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace duckdb {

class DataTable;
class DuckTransaction;
class DuckTableEntry;
class IndexEntry;
struct PropertyGraphTable;

//! Vertex keys are mapped to dense ids in key order; the dictionary of an epoch is frozen with it.
//!
//! Compact key spaces (the common case: generated ids, row-number-like keys) use a presence bitmap over
//! [min_key, max_key] with per-word rank counts, so a lookup is a popcount and construction needs no sort. Sparse
//! key spaces fall back to a sorted key array with binary search.
struct CSRIndexDictionary {
	bool dense = false;
	//! Dense and every key in [min_key, min_key + count) is present: id = key - min_key, no bitmap needed
	bool identity = false;
	int64_t min_key = 0;
	//! Dense: bit (key - min_key) is set when the key is a vertex; word_rank[w] = number of set bits before word w
	vector<uint64_t> bitmap;
	vector<uint32_t> word_rank;
	//! Sparse: all keys in ascending order, id = position
	vector<int64_t> sorted_keys;
	idx_t count = 0;

	//! Returns the dense id of a key, or DConstants::INVALID_INDEX when the key is not a vertex of the epoch.
	idx_t Lookup(int64_t key) const;
	//! The key of a dense id
	int64_t KeyOf(idx_t id) const;
	idx_t Size() const {
		return count;
	}
};

//! Edges collected by one build thread, keys already cast to BIGINT
struct CSRIndexEdgeList {
	vector<int64_t> src_keys;
	vector<int64_t> dst_keys;
	vector<row_t> row_ids;
	int64_t min_key = NumericLimits<int64_t>::Maximum();
	int64_t max_key = NumericLimits<int64_t>::Minimum();

	idx_t Size() const {
		return row_ids.size();
	}
};

//! One immutable adjacency snapshot: the edges visible at `bound`, packed into the partitioned CSR layout the
//! path-finding operator traverses. Readers share epochs through shared_ptr; an epoch is never modified.
struct CSRIndexEpoch {
	//! Rows committed before this bound are in the epoch
	VisibilityBound bound;
	//! Position in the index's sequence of epochs; later generations contain every row of earlier ones unless deleted
	uint32_t generation = 0;
	idx_t vertex_count = 0;
	idx_t edge_count = 0;
	//! Destination partition width used for the CSR layout
	idx_t partition_width = 0;
	shared_ptr<CSRIndexDictionary> dictionary;
	//! Forward partitions (both directions when the index is undirected)
	shared_ptr<PartitionedCSRIndex> csr;
	//! Edge-table row id per edge position, per partition, aligned with the partition's edge array
	vector<vector<row_t>> partition_row_ids;
	//! Epoch position (partition << 32 | offset) of each tombstoned row, DConstants::INVALID_INDEX when the row is
	//! not an edge of this epoch; filled lazily by readers, protected by the index lock
	vector<idx_t> tombstone_positions;
	//! The same rows' positions in the reverse partitions (indexes with a stored reverse side), aligned
	vector<idx_t> reverse_tombstone_positions;
	//! Undirected indexes: the position of the mirrored edge (dst -> src, same row id) in the forward partitions,
	//! aligned; INVALID_INDEX when absent (29 Oct: a deleted undirected edge stayed traversable in one direction)
	vector<idx_t> mirror_tombstone_positions;
};

//! An edge committed after the epoch was built (keys, since the vertex may be new) and its row id
struct CSRIndexDeltaEdge {
	int64_t src_key = 0;
	int64_t dst_key = 0;
	row_t row = 0;
	//! Generation of the first epoch that contains this row (UINT32_MAX while no retained epoch does); epochs are
	//! built from a superset of their predecessors' committed rows, so every later epoch contains it as well
	uint32_t covered_epoch = NumericLimits<uint32_t>::Maximum();
	//! Change group: one Append call = one commit chunk of one transaction, so every row of a group has the same
	//! visibility and a reader checks one row per group. 0 means "decide per row" (entries loaded from storage).
	uint32_t group = 0;
	//! Change group of the TryDelete call that deleted this delta row, 0 while it is not deleted
	uint32_t deleted_group = 0;
	//! `group` of an entry whose append was reverted and whose row id was reused: never visible, dropped by the
	//! next compaction, never persisted
	static constexpr uint32_t DEAD_GROUP = NumericLimits<uint32_t>::Maximum();
	//! Dense ids of the keys in the dictionary of epoch `id_generation` (UINT32_MAX when unknown to it), resolved
	//! at append time and again at every fold. A fold rebuilds the dictionary in key order, so ids are only valid
	//! for the epoch they were resolved against.
	uint32_t src_id = NumericLimits<uint32_t>::Maximum();
	uint32_t dst_id = NumericLimits<uint32_t>::Maximum();
	uint32_t id_generation = NumericLimits<uint32_t>::Maximum();

	//! An edge logged by Append: not in any epoch yet, in change group `group`, with its ids resolved (or not) against
	//! the dictionary of epoch `id_generation`
	static CSRIndexDeltaEdge Appended(int64_t src_key, int64_t dst_key, row_t row, uint32_t group, uint32_t src_id,
	                                  uint32_t dst_id, uint32_t id_generation) {
		CSRIndexDeltaEdge edge;
		edge.src_key = src_key;
		edge.dst_key = dst_key;
		edge.row = row;
		edge.group = group;
		edge.src_id = src_id;
		edge.dst_id = dst_id;
		edge.id_generation = id_generation;
		return edge;
	}
	//! An edge read from storage: committed before the restart, so no change group (readers decide per row), not
	//! deleted, in no retained epoch; ids are resolved by the next fold
	static CSRIndexDeltaEdge Loaded(int64_t src_key, int64_t dst_key, row_t row) {
		CSRIndexDeltaEdge edge;
		edge.src_key = src_key;
		edge.dst_key = dst_key;
		edge.row = row;
		return edge;
	}
};

//! A deleted row that may still be an edge of a retained epoch
struct CSRIndexTombstone {
	row_t row;
	//! Generation of the first epoch built without this row (UINT32_MAX while every retained epoch has it)
	uint32_t excluded_epoch = NumericLimits<uint32_t>::Maximum();
	//! Change group of the TryDelete call, see CSRIndexDeltaEdge::group; 0 means "decide per row"
	uint32_t group = 0;
	//! The deleted edge's keys when the delete carried them: its epoch position is then found in the source's row of
	//! the destination's partition (29 Oct; before, every reader scanned all of the epoch's row ids after a commit
	//! with deletes, 46% of the CPU of 16 readers at SF300). Entries loaded from storage have no keys and keep the scan.
	bool has_keys = false;
	int64_t src_key = 0;
	int64_t dst_key = 0;
};

//! What one reader traverses: the shared epoch plus an overlay for the changes visible to its transaction.
//! Epoch partitions are shared and immutable; the overlay is private to the snapshot.
struct CSRIndexSnapshot {
	shared_ptr<CSRIndexEpoch> epoch;
	//! Epoch vertices plus the vertices that only delta edges reference
	idx_t vertex_count = 0;
	idx_t edge_count = 0;
	idx_t partition_width = 0;
	//! The index holds both directions of every edge, so a search may run from the destination side
	bool undirected = false;
	//! One entry per destination range: the epoch partition (an empty one beyond the epoch's ranges)
	std::vector<shared_ptr<LocalCSR>> partitions;
	//! Aligned with partitions: delta edges of that destination range, or null
	std::vector<shared_ptr<LocalCSR>> delta_partitions;
	//! Aligned with partitions: bit set for epoch edge positions this reader must not traverse, or null
	std::vector<shared_ptr<vector<uint64_t>>> suppressed;
	//! The reverse direction (edges by source range), with its own companions and suppression, when the index
	//! stores one (`reverse = true`); an undirected index aliases the forward vectors; empty otherwise
	std::vector<shared_ptr<LocalCSR>> reverse_partitions;
	std::vector<shared_ptr<LocalCSR>> reverse_delta_partitions;
	std::vector<shared_ptr<vector<uint64_t>>> reverse_suppressed;
	//! Keys of vertices that only appear in delta edges, id = vertex_count_of_epoch + position
	vector<int64_t> extra_keys;
	std::unordered_map<int64_t, uint32_t> extra_key_to_id;

	idx_t Lookup(int64_t key) const;
	int64_t KeyOf(idx_t id) const;
};

//! The CSR index: one immutable epoch (the edges visible at the bound of the transaction that built it), an
//! append-only delta of edges committed after that bound, and append-only tombstones for epoch edges deleted after
//! it. No commit ids are stored: a reader decides visibility of the delta and the tombstones through the table's
//! row version information, once per query (see docs/CSR_INDEX_DESIGN.md).
class CSRIndex : public BoundIndex {
public:
	static constexpr const char *TYPE_NAME = "CSR";
	static constexpr const char *UNDIRECTED_OPTION = "undirected";
	//! `WITH (reverse = true)`: the epoch also stores every edge by its source range, without row ids, so a search
	//! can run from the destination side (many sources to one destination) on a directed graph
	static constexpr const char *REVERSE_OPTION = "reverse";

	CSRIndex(const Identifier &name, IndexConstraintType constraint_type, const vector<column_t> &column_ids,
	         TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
	         AttachedDatabase &db, const case_insensitive_map_t<Value> &options);

	//! Registration with DuckDB's index type set
	static IndexType GetCSRIndexType();
	static unique_ptr<BoundIndex> Create(CreateIndexInput &input);

	bool IsUndirected() const {
		return undirected;
	}
	bool IsReverse() const {
		return reverse;
	}

	//! Returns what a reader with this transaction may traverse, building the epoch from `table` when the index has
	//! none yet. A reader outlived by two folds gets a private epoch built from its own view of the table.
	shared_ptr<CSRIndexSnapshot> Snapshot(ClientContext &context, DuckTransaction &transaction, DataTable &table) const;

	//! Installs an epoch built by the CREATE INDEX pipeline
	void InstallEpoch(shared_ptr<CSRIndexEpoch> epoch);

	//! Builds an epoch from the edge lists of the build threads, in parallel
	static shared_ptr<CSRIndexEpoch> BuildEpoch(ClientContext &context, vector<CSRIndexEdgeList> &lists,
	                                            bool undirected, bool reverse, VisibilityBound bound);

	//! Finds a bound CSR index on the edge table of a property graph whose key columns are the edge's source and
	//! destination foreign keys and whose direction matches. Returns the index name, or an empty string.
	static string FindForEdgeTable(ClientContext &context, const PropertyGraphTable &edge_table, bool undirected);

	//! Looks up the index by name on its table and returns the snapshot for the current transaction
	static shared_ptr<CSRIndexSnapshot> Resolve(ClientContext &context, const string &catalog, const string &schema,
	                                            const string &table_name, const string &index_name);

	//! The marker cache key that tells the operator to read this index instead of building a CSR
	static string CacheKey(const PropertyGraphTable &edge_table, const string &index_name);
	static bool IsCacheKey(const string &cache_key);
	static shared_ptr<CSRIndexSnapshot> ResolveCacheKey(ClientContext &context, const string &cache_key);

	//! Sizes of the change log, for tests and the fold policy
	idx_t DeltaSize() const;
	idx_t TombstoneCount() const;
	idx_t EpochCount() const;
	//! Bytes held by the retained epochs and the change log (the fixed-costs table: bytes per edge)
	idx_t InMemoryBytes() const;
	//! Edges of the newest retained epoch
	idx_t EpochEdgeCount() const;
	//! 'folds=..;last_fold_ms=..;fold_ms=..': the folds this index ran since it was bound, for the update experiments
	string FoldStats() const;

	//! Folds the index behind `entry`: a new epoch at the transaction's bound, and the change-log entries no retained
	//! epoch needs are dropped. `ratio` > 0 is the automatic fold from Resolve: it runs only when the pending log is
	//! above ratio x epoch edges and no other fold is running. `ratio` 0 is the explicit fold (duckpgq_csr_index_fold):
	//! it waits for a running fold and requires a transaction without uncommitted changes. The table rebuild runs
	//! without the entry's read handle (see the definition). Returns whether an epoch was installed.
	static bool RunFold(ClientContext &context, const shared_ptr<IndexEntry> &entry, DuckTableEntry &table,
	                    double ratio);

	//! The DuckDB table a name refers to, or nullptr (not found, or not a DuckDB table): by a possibly qualified name
	//! string ('[catalog.][schema.]table'), by its parsed parts, or by the table of a property graph
	static optional_ptr<DuckTableEntry> FindTable(ClientContext &context, const string &table_name);
	static optional_ptr<DuckTableEntry> FindTable(ClientContext &context, const QualifiedName &name);
	static optional_ptr<DuckTableEntry> FindTable(ClientContext &context, const PropertyGraphTable &table);
	//! The bound CSR index named `index_name` on `table`, after binding the table's indexes; nullptr when the table
	//! has no index of that name or it is not a bound CSR index. Callers decide whether that is an error.
	static shared_ptr<IndexEntry> FindIndexEntry(ClientContext &context, DuckTableEntry &table,
	                                             const string &index_name);

public:
	// BoundIndex interface
	ErrorData Append(IndexLock &l, DataChunk &chunk, Vector &row_ids) override;
	ErrorData Insert(IndexLock &l, DataChunk &chunk, Vector &row_ids) override;
	idx_t TryDelete(IndexLock &state, DataChunk &entries, Vector &row_identifiers,
	                optional_ptr<SelectionVector> deleted_sel, optional_ptr<SelectionVector> non_deleted_sel) override;
	void ResetStorage(IndexLock &index_lock) override;
	bool MergeIndexes(IndexLock &state, BoundIndex &other_index) override;
	void Vacuum(IndexLock &l) override;
	idx_t GetInMemorySize(IndexLock &state) const override;
	void Verify(IndexLock &l) override;
	string ToString(IndexLock &l, bool display_ascii) override;
	void VerifyAllocations(IndexLock &l) override;
	string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index,
	                                     DataChunk &input) const override;
	IndexStorageInfo SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options) override;
	IndexStorageInfo SerializeToWAL(const case_insensitive_map_t<Value> &options) override;

private:
	shared_ptr<CSRIndexEpoch> RebuildFromTable(ClientContext &context, DuckTransaction &transaction,
	                                           DataTable &table) const;
	//! The epoch of every row the transaction sees, without touching the index object (RunFold calls it with no
	//! handle on the index entry)
	static shared_ptr<CSRIndexEpoch> BuildEpochFromTable(ClientContext &context, DuckTransaction &transaction,
	                                                     DataTable &table, const vector<column_t> &column_ids,
	                                                     bool undirected, bool reverse);
	//! Whether RunFold should build an epoch for this transaction (ordering and, for ratio > 0, the pending log)
	bool FoldDue(DuckTransaction &transaction, double ratio) const;
	//! Installs an epoch RunFold built, unless a newer one arrived meanwhile; records the fold time
	void InstallFoldedEpoch(DuckTransaction &transaction, DataTable &table, shared_ptr<CSRIndexEpoch> epoch,
	                        uint64_t fold_us) const;
	//! Stamps the change-log entries that the epoch built by `transaction` accounts for: delta rows it contains and
	//! tombstoned rows it excludes. Uses the live transaction, so it does not depend on how long DuckDB keeps row
	//! versions. Then installs the epoch and drops the entries no retained epoch needs any more.
	void InstallEpochAndCompact(DuckTransaction &transaction, DataTable &table, shared_ptr<CSRIndexEpoch> epoch) const;
	//! Drops change-log entries that every retained epoch already accounts for; the index lock must be held
	void CompactChangeLogLocked() const;
	//! Drops every epoch, the change log with its row index and cached snapshot, and a checkpointed state not loaded
	//! yet (ResetStorage, CREATE INDEX on a fresh index, a stored state the index cannot use); the lock must be held
	void ClearStateLocked() const;
	//! Runs `fn(lock)` with the index lock held and the checkpointed state loaded: the counters the stats functions
	//! read and the per-query copy of the log lengths
	template <class FN>
	auto WithLoadedState(FN &&fn) const -> decltype(fn(std::declval<IndexLock &>())) {
		IndexLock l(*this);
		EnsureLoadedLocked(l);
		return fn(l);
	}
	//! What a reader copies under the lock before building its snapshot: the epoch it may traverse and the lengths
	//! of the change log it considers (entries appended later belong to commits it cannot see)
	struct LogState {
		shared_ptr<CSRIndexEpoch> epoch;
		bool have_epochs = false;
		idx_t delta_count = 0;
		idx_t tombstone_count = 0;
		//! Entries the newest epoch does not account for (what the automatic fold trigger measures)
		idx_t pending_count = 0;
	};
	LogState LogStateFor(VisibilityBound bound) const;
	//! Log entries not accounted for by the newest epoch: the raw log also keeps entries only the retained older
	//! epoch needs, so a trigger on the raw size would fold twice per crossing (2 Nov)
	idx_t PendingLogLocked() const;
	//! Resolves the epoch positions of tombstoned rows [known_count, tombstone_count), plus the mirrored position of an
	//! undirected edge and the reverse-side position when the index stores one. A tombstone that carries its keys is
	//! looked up: the destination's partition, the source's sparse row, the row id among that row's edges. Keyless
	//! entries (loaded from storage) are found by one scan over the epoch's row ids.
	void ResolveTombstonePositions(CSRIndexEpoch &epoch, idx_t tombstone_count) const;
	//! The newest retained epoch a reader at `bound` may traverse, or nullptr; the index lock must be held
	shared_ptr<CSRIndexEpoch> EpochForLocked(VisibilityBound bound) const;
	//! Builds the reader's snapshot: the epoch plus the overlay of the change log visible to `transaction`. With
	//! `shareable` the snapshot may be served from and stored in the one-entry cache keyed by (epoch generation,
	//! newest visible change group), which identifies the visible set for every reader without own changes.
	shared_ptr<CSRIndexSnapshot> BuildSnapshot(ClientContext &context, DuckTransaction &transaction, DataTable &table,
	                                           shared_ptr<CSRIndexEpoch> epoch, idx_t delta_count,
	                                           idx_t tombstone_count, bool shareable) const;
	//! Rebuilds delta_row_index from delta; the index lock must be held
	void RebuildDeltaRowIndexLocked() const;
	//! Resolves the dense ids of every delta entry against the newest epoch's dictionary; the index lock must be held
	void ResolveDeltaIdsLocked() const;

	//! Persistence: the newest epoch and the part of the change log it does not account for are written as one byte
	//! stream into fixed-size segments; the segment list and sizes travel in the storage info's options.
	//! Version 2 adds the reverse side after the forward partitions. Only this version loads: DeferLoad ignores a
	//! stored state of any other version, so the index starts empty and rebuilds its epoch from the table on first use
	static constexpr idx_t STORAGE_FORMAT_VERSION = 2;
	vector<uint8_t> SerializeState() const;
	IndexStorageInfo WriteSegments(const vector<uint8_t> &bytes, optional_ptr<QueryContext> checkpoint_context);
	//! Remembers where the checkpointed state lives. The index is deserialized on first use (EnsureLoadedLocked):
	//! DuckDB binds every index of a table together, so an eager load would read every CSR index of the table (and
	//! hold it in memory) for a query that reads one of them
	void DeferLoad(const IndexStorageInfo &info);
	void EnsureLoadedLocked(IndexLock &l) const;
	void LoadFromStorageLocked(IndexLock &l, const IndexStorageInfo &info) const;

private:
	bool undirected = false;
	bool reverse = false;
	//! Retained epochs, oldest first; the newest serves current readers, older ones serve transactions that started
	//! before a fold. Readers take the index lock only to copy an epoch pointer and the change-log lengths.
	static constexpr idx_t MAX_RETAINED_EPOCHS = 2;
	mutable vector<shared_ptr<CSRIndexEpoch>> epochs;
	mutable uint32_t next_generation = 0;
	mutable vector<CSRIndexDeltaEdge> delta;
	mutable vector<CSRIndexTombstone> tombstones;
	//! Entries of `delta` / `tombstones` the newest epoch accounts for (recomputed by each fold; appends add
	//! unaccounted entries, so the pending log is the sizes minus these)
	mutable idx_t newest_covered_delta = 0;
	mutable idx_t newest_excluded_tombstones = 0;
	//! Next change group id handed to an Append or TryDelete call (commits are serialized, so ids follow commit order)
	mutable uint32_t next_change_group = 1;
	//! Position in `delta` of every delta row, so TryDelete can stamp the row's deleted_group without a scan
	mutable unordered_map<row_t, idx_t> delta_row_index;
	//! The last shared snapshot built, served to readers that see the same change groups of the same epoch
	mutable shared_ptr<CSRIndexSnapshot> cached_snapshot;
	mutable uint32_t cached_snapshot_generation = 0;
	mutable uint32_t cached_snapshot_group = 0;
	//! Hash of the per-entry visibility decisions of the cached snapshot's reader
	mutable uint64_t cached_snapshot_visibility = 0;
	//! One fold at a time; shared so that RunFold can hold it while no handle on the entry keeps the index alive
	mutable shared_ptr<mutex> fold_lock = make_shared_ptr<mutex>();
	//! Segments of the last checkpoint or WAL serialization, kept until the next one replaces them
	mutable unique_ptr<FixedSizeAllocator> storage_allocator;
	//! Checkpointed state not deserialized yet (see DeferLoad)
	mutable IndexStorageInfo pending_storage;
	mutable bool storage_pending = false;
	//! Fold timing (see FoldStats)
	mutable std::atomic<uint64_t> fold_count {0};
	mutable std::atomic<uint64_t> fold_total_us {0};
	mutable std::atomic<uint64_t> fold_last_us {0};
};

//! Registration entry point called from the core module
struct CorePGQIndex {
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
