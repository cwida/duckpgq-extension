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

#include "duckpgq/common.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/index/index_type.hpp"


namespace duckdb {

class DataTable;
class DuckTransaction;
class DuckTableEntry;
struct PropertyGraphTable;

//! Vertex keys are mapped to dense ids in key order; the dictionary of an epoch is frozen with it.
//!
//! Compact key spaces (the common case: generated ids, row-number-like keys) use a presence bitmap over
//! [min_key, max_key] with per-word rank counts, so a lookup is a popcount and construction needs no sort. Sparse
//! key spaces fall back to a sorted key array with binary search.
struct CSRIndexDictionary {
	bool dense = false;
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
	idx_t vertex_count = 0;
	idx_t edge_count = 0;
	//! Destination partition width used for the CSR layout
	idx_t partition_width = 0;
	shared_ptr<CSRIndexDictionary> dictionary;
	//! Forward partitions (both directions when the index is undirected)
	shared_ptr<PartitionedCSRIndex> csr;
	//! Edge-table row id per edge position, per partition, aligned with the partition's edge array
	vector<vector<row_t>> partition_row_ids;
};

//! The CSR index. This first version keeps a single epoch and treats any change to the table as invalidating it:
//! Append/Delete mark the epoch stale and the next reader rebuilds it from the table at its own snapshot. The
//! delta/tombstone overlay of the design document replaces that in a later step.
class CSRIndex : public BoundIndex {
public:
	static constexpr const char *TYPE_NAME = "CSR";
	static constexpr const char *UNDIRECTED_OPTION = "undirected";

	CSRIndex(const Identifier &name, IndexConstraintType constraint_type, const vector<column_t> &column_ids,
	         TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
	         AttachedDatabase &db, const case_insensitive_map_t<Value> &options);

	//! Registration with DuckDB's index type set
	static IndexType GetCSRIndexType();
	static unique_ptr<BoundIndex> Create(CreateIndexInput &input);

	bool IsUndirected() const {
		return undirected;
	}

	//! Returns the epoch a reader with this transaction may traverse, building it from `table` when the index has
	//! no epoch yet or the table changed since the epoch was built. Returns nullptr when the epoch was built at a
	//! newer snapshot than the reader's, in which case the caller builds the CSR for the query itself.
	shared_ptr<CSRIndexEpoch> Snapshot(ClientContext &context, DuckTransaction &transaction, DataTable &table) const;

	//! Installs an epoch built by the CREATE INDEX pipeline
	void InstallEpoch(shared_ptr<CSRIndexEpoch> epoch);

	//! Builds an epoch from the edge lists of the build threads, in parallel
	static shared_ptr<CSRIndexEpoch> BuildEpoch(ClientContext &context, vector<CSRIndexEdgeList> &lists,
	                                            bool undirected, VisibilityBound bound);

	//! Finds a bound CSR index on the edge table of a property graph whose key columns are the edge's source and
	//! destination foreign keys and whose direction matches. Returns the index name, or an empty string.
	static string FindForEdgeTable(ClientContext &context, const PropertyGraphTable &edge_table, bool undirected);

	//! Looks up the index by name on its table and returns the epoch for the current transaction (see Snapshot)
	static shared_ptr<CSRIndexEpoch> Resolve(ClientContext &context, const string &catalog, const string &schema,
	                                         const string &table_name, const string &index_name);

	//! The marker cache key that tells the operator to read this index instead of building a CSR
	static string CacheKey(const PropertyGraphTable &edge_table, const string &index_name);
	static bool IsCacheKey(const string &cache_key);
	static shared_ptr<CSRIndexEpoch> ResolveCacheKey(ClientContext &context, const string &cache_key);

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

private:
	bool undirected = false;
	//! Readers take the index lock only to copy these two fields
	mutable shared_ptr<CSRIndexEpoch> epoch;
	//! Set by Append/Delete: the epoch no longer reflects the table
	mutable bool stale = false;
};

//! Registration entry point called from the core module
struct CorePGQIndex {
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
