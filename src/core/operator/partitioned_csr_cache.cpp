#include "duckpgq/core/operator/partitioned_csr_cache.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckpgq/core/option/duckpgq_option.hpp"
#include "duckpgq/core/utils/duckpgq_sql.hpp"
#include "duckpgq/parser/property_graph_table.hpp"
#include "duckdb/storage/data_table.hpp"

#include <iomanip>
#include <limits>
#include <sstream>

namespace duckdb {

static optional_ptr<DuckTableEntry> GetDuckTableEntry(ClientContext &context, const PropertyGraphTable &table) {
	auto entry = Catalog::GetEntry<TableCatalogEntry>(
	    context, QualifiedName(table.catalog_name, table.schema_name, table.table_name), OnEntryNotFound::RETURN_NULL);
	if (!entry || !entry->IsDuckTable()) {
		return nullptr;
	}
	return &entry->Cast<DuckTableEntry>();
}

bool GetDirectedPathFindingStorageCounts(ClientContext &context, const PropertyGraphTable &edge_table,
                                         idx_t &vertex_count, idx_t &edge_count) {
	if (!edge_table.source_pg_table || !edge_table.destination_pg_table ||
	    !edge_table.source_pg_table->SameTableIdentity(*edge_table.destination_pg_table)) {
		return false;
	}
	auto vertex_entry = GetDuckTableEntry(context, *edge_table.source_pg_table);
	auto edge_entry = GetDuckTableEntry(context, edge_table);
	if (!vertex_entry || !edge_entry) {
		return false;
	}
	// Row IDs can contain gaps after deletes. The next row ID is the required CSR range.
	vertex_count = vertex_entry->GetStorage().GetNextRowId();
	edge_count = edge_entry->GetStorage().GetTotalRows();
	return true;
}

static string PathFindingEndpointJoin(const string &edge_alias, const PropertyGraphTable &vertex_table,
                                      const string &vertex_alias, const vector<Identifier> &foreign_keys,
                                      const vector<Identifier> &primary_keys) {
	if (foreign_keys.size() != primary_keys.size()) {
		throw BinderException("Vertex columns and edge columns size mismatch");
	}
	std::ostringstream result;
	result << DuckPGQSQL::TableRef(vertex_table, vertex_alias) << " ON ";
	for (idx_t key_idx = 0; key_idx < foreign_keys.size(); key_idx++) {
		if (key_idx > 0) {
			result << " AND ";
		}
		result << DuckPGQSQL::Column(foreign_keys[key_idx], edge_alias) << " = "
		       << DuckPGQSQL::Column(primary_keys[key_idx], vertex_alias);
	}
	return result.str();
}

string GetDirectedPathFindingEndpointsSQL(const PropertyGraphTable &edge_table) {
	const string edge_alias = "__duckpgq_edge";
	const string source_alias = "__duckpgq_source";
	const string destination_alias = "__duckpgq_destination";
	std::ostringstream query;
	query << "SELECT CAST(" << DuckPGQSQL::Column(string("rowid"), source_alias)
	      << " AS BIGINT) AS pathfinding_edge_src, CAST(" << DuckPGQSQL::Column(string("rowid"), destination_alias)
	      << " AS BIGINT) AS pathfinding_edge_dst FROM " << DuckPGQSQL::TableRef(edge_table, edge_alias)
	      << " INNER JOIN "
	      << PathFindingEndpointJoin(edge_alias, *edge_table.source_pg_table, source_alias, edge_table.source_fk,
	                                 edge_table.source_pk)
	      << " INNER JOIN "
	      << PathFindingEndpointJoin(edge_alias, *edge_table.destination_pg_table, destination_alias,
	                                 edge_table.destination_fk, edge_table.destination_pk);
	return query.str();
}

static idx_t GetBufferedPartitionedCSRRequiredPartitionCount(idx_t vertex_count, idx_t thread_count,
                                                             ClientContext &context) {
	auto target_partition_count =
	    std::max<idx_t>(1, thread_count) * (1 + std::max<int32_t>(1, GetLightPartitionMultiplier(context)));
	auto minimum_partition_count = std::max<idx_t>(1, (vertex_count + 2 + UINT16_MAX - 1) / UINT16_MAX);
	auto maximum_partition_count = RadixPartitioning::NumberOfPartitions(RadixPartitioning::MAX_RADIX_BITS);
	if (minimum_partition_count > maximum_partition_count) {
		throw OutOfRangeException(
		    "Radix-partitioned endpoint construction supports at most %llu vertices with uint16 destinations",
		    maximum_partition_count * UINT16_MAX);
	}
	return std::min(std::max(target_partition_count, minimum_partition_count), maximum_partition_count);
}

idx_t GetBufferedPartitionedCSRRadixBits(idx_t vertex_count, idx_t thread_count, ClientContext &context) {
	auto required_partition_count =
	    GetBufferedPartitionedCSRRequiredPartitionCount(vertex_count, thread_count, context);
	idx_t radix_bits = 0;
	idx_t partition_count = 1;
	while (partition_count < required_partition_count && radix_bits < RadixPartitioning::MAX_RADIX_BITS) {
		partition_count <<= 1;
		radix_bits++;
	}
	return radix_bits;
}

idx_t GetBufferedPartitionedCSRWidth(idx_t vertex_count, idx_t thread_count, ClientContext &context) {
	auto partition_count = GetBufferedPartitionedCSRRequiredPartitionCount(vertex_count, thread_count, context);
	return std::max<idx_t>(1, (vertex_count + 2 + partition_count - 1) / partition_count);
}

idx_t GetBufferedPartitionedCSRLogicalPartitionCount(idx_t vertex_count, idx_t thread_count, ClientContext &context) {
	auto partition_width = GetBufferedPartitionedCSRWidth(vertex_count, thread_count, context);
	return std::max<idx_t>(1, (vertex_count + 2 + partition_width - 1) / partition_width);
}

string GetBufferedPartitionedCSRCacheKey(ClientContext &context, const string &base_cache_key, idx_t vertex_count,
                                         idx_t edge_count, const string &mode, bool radix_partitioned_input) {
	if (base_cache_key.empty()) {
		return string();
	}

	bool build_forward = true;
	bool build_reverse = false;
	bool build_pull = false;
	bool finalize_sparse_rows = true;
	if (mode == "bidirectionaliterativelength") {
		build_reverse = true;
		finalize_sparse_rows = false;
	} else if (mode == "pushpulliterativelength") {
		build_pull = true;
	} else if (mode != "iterativelength" && mode != "shortestpath") {
		throw InvalidInputException("Unknown path-finding mode %s", mode);
	}

	auto thread_count = std::max<idx_t>(1, TaskScheduler::GetScheduler(context).NumberOfThreads());
	auto partition_width = GetBufferedPartitionedCSRWidth(vertex_count, thread_count, context);
	std::ostringstream key;
	key << std::setprecision(std::numeric_limits<double>::max_digits10);
	key << "partitioned-csr-v4|graph=" << base_cache_key.size() << ":" << base_cache_key;
	key << "|vertices=" << vertex_count + 2 << "|edges=" << edge_count;
	key << "|input=" << (radix_partitioned_input ? "radix-partitioned-endpoints" : "precounted-endpoints");
	key << "|partition_width=" << partition_width;
	key << "|partition_count=" << GetBufferedPartitionedCSRLogicalPartitionCount(vertex_count, thread_count, context);
	key << "|threads=" << thread_count;
	key << "|forward=" << build_forward;
	key << "|reverse=" << build_reverse;
	key << "|pull=" << build_pull;
	key << "|sparse=" << finalize_sparse_rows;
	key << "|heavy_fraction=" << GetHeavyPartitionFraction(context);
	key << "|light_multiplier=" << GetLightPartitionMultiplier(context);
	return key.str();
}

} // namespace duckdb
