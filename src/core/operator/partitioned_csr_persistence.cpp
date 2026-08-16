#include "duckpgq/core/operator/partitioned_csr_persistence.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"

#include <limits>

namespace duckdb {

static constexpr uint8_t FORWARD_CSR_KIND = 1;
static constexpr uint8_t KNOWN_CAPABILITY_MASK = static_cast<uint8_t>(PartitionedCSRCapabilities::FORWARD) |
                                                 static_cast<uint8_t>(PartitionedCSRCapabilities::REVERSE) |
                                                 static_cast<uint8_t>(PartitionedCSRCapabilities::PULL);
static constexpr const char *PERSISTED_LAYOUT = "destination-partitioned-v1";

static void ThrowOnQueryError(const QueryResult &result, const string &operation) {
	if (result.HasError()) {
		throw IOException("Failed to %s: %s", operation, result.GetError());
	}
}

static void ExecuteStatement(Connection &connection, const string &statement, const string &operation) {
	auto result = connection.Query(statement);
	ThrowOnQueryError(*result, operation);
}

static unique_ptr<MaterializedQueryResult> ExecutePrepared(PreparedStatement &statement, vector<Value> values,
                                                           const string &operation) {
	auto result = statement.Execute(values, false);
	ThrowOnQueryError(*result, operation);
	if (result->GetResultType() != QueryResultType::MATERIALIZED_RESULT) {
		throw InternalException("Expected a materialized result while attempting to %s", operation);
	}
	return unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
}

template <class T, Value (*MAKE_VALUE)(T)>
static Value NumericListValue(const LogicalType &type, const std::vector<T> &input) {
	vector<Value> values;
	values.reserve(input.size());
	for (auto value : input) {
		values.push_back(MAKE_VALUE(value));
	}
	return Value::LIST(type, std::move(values));
}

static Value UInt16ListValue(const std::vector<uint16_t> &input) {
	return NumericListValue<uint16_t, Value::USMALLINT>(LogicalType::USMALLINT, input);
}

static Value UInt32ListValue(const std::vector<uint32_t> &input) {
	return NumericListValue<uint32_t, Value::UINTEGER>(LogicalType::UINTEGER, input);
}

template <class T>
static std::vector<T> ReadNumericList(const Value &value) {
	auto children = ListValue::GetChildren(value);
	std::vector<T> result;
	result.reserve(children.size());
	for (const auto &child : children) {
		result.push_back(child.GetValue<T>());
	}
	return result;
}

static bool ValidateOffsets(const std::vector<uint32_t> &offsets, idx_t row_count, idx_t edge_count) {
	if (offsets.size() != row_count + 1 || offsets.empty() || offsets.front() != 0 || offsets.back() != edge_count) {
		return false;
	}
	for (idx_t offset_idx = 1; offset_idx < offsets.size(); offset_idx++) {
		if (offsets[offset_idx] < offsets[offset_idx - 1] || offsets[offset_idx] > edge_count) {
			return false;
		}
	}
	return true;
}

static bool ValidateSparsePayload(const std::vector<uint32_t> &sources, const std::vector<uint32_t> &offsets,
                                  const std::vector<uint16_t> &destinations, idx_t vertex_count,
                                  idx_t partition_width) {
	if (!ValidateOffsets(offsets, sources.size(), destinations.size())) {
		return false;
	}
	for (idx_t source_idx = 0; source_idx < sources.size(); source_idx++) {
		if (sources[source_idx] >= vertex_count || (source_idx > 0 && sources[source_idx] <= sources[source_idx - 1])) {
			return false;
		}
	}
	for (auto destination : destinations) {
		if (destination >= partition_width) {
			return false;
		}
	}
	return true;
}

static bool ValidateDensePayload(const LocalCSR &partition, idx_t vertex_count) {
	if (!partition.v || partition.v_array_size != vertex_count + 2 || partition.v_array_size == 0) {
		return false;
	}
	auto previous = partition.v[0].load(std::memory_order_relaxed);
	if (previous != 0) {
		return false;
	}
	for (idx_t offset_idx = 1; offset_idx < partition.v_array_size; offset_idx++) {
		auto current = partition.v[offset_idx].load(std::memory_order_relaxed);
		if (current < previous || current > partition.e.size()) {
			return false;
		}
		previous = current;
	}
	return previous == partition.e.size();
}

static bool ValidatePartitionedCSRIndex(const PartitionedCSRIndex &index) {
	auto capabilities = static_cast<uint8_t>(index.capabilities);
	if (!(capabilities & static_cast<uint8_t>(PartitionedCSRCapabilities::FORWARD)) ||
	    (capabilities & ~KNOWN_CAPABILITY_MASK) != 0 || index.forward_partitions.empty()) {
		return false;
	}

	idx_t expected_start = 0;
	idx_t total_edges = 0;
	for (const auto &partition_ptr : index.forward_partitions) {
		if (!partition_ptr) {
			return false;
		}
		auto &partition = *partition_ptr;
		if (partition.start_vertex != expected_start || partition.end_vertex <= partition.start_vertex ||
		    partition.end_vertex > index.vertex_count || partition.end_vertex - partition.start_vertex > UINT16_MAX) {
			return false;
		}
		auto partition_width = partition.end_vertex - partition.start_vertex;
		if (partition.sparse_rows_initialized) {
			auto offsets = partition.row_offsets;
			if (offsets.empty() && partition.source_vertices.empty() && partition.e.empty()) {
				offsets.push_back(0);
			}
			if (!ValidateSparsePayload(partition.source_vertices, offsets, partition.e, index.vertex_count,
			                           partition_width)) {
				return false;
			}
		} else if (!ValidateDensePayload(partition, index.vertex_count)) {
			return false;
		}
		for (auto destination : partition.e) {
			if (destination >= partition_width) {
				return false;
			}
		}
		total_edges += partition.e.size();
		for (const auto &segment : partition.segments) {
			if (!ValidateSparsePayload(segment.source_vertices, segment.row_offsets, segment.edges, index.vertex_count,
			                           partition_width)) {
				return false;
			}
			total_edges += segment.edges.size();
		}
		expected_start = partition.end_vertex;
	}
	return expected_start == index.vertex_count && total_edges == index.edge_count;
}

void InitializePartitionedCSRPersistence(ClientContext &context) {
	Connection connection(*context.db);
	ExecuteStatement(connection, "BEGIN TRANSACTION", "start CSR persistence schema transaction");
	try {
		ExecuteStatement(
		    connection,
		    "CREATE TABLE IF NOT EXISTS __duckpgq_csr_registry ("
		    "logical_key VARCHAR PRIMARY KEY, generation UBIGINT NOT NULL, valid BOOLEAN NOT NULL, "
		    "format_version UINTEGER NOT NULL, vertex_count UBIGINT NOT NULL, edge_count UBIGINT NOT NULL, "
		    "capabilities UTINYINT NOT NULL, layout VARCHAR NOT NULL, partition_count UBIGINT NOT NULL, "
		    "segment_count UBIGINT NOT NULL, created_at TIMESTAMP NOT NULL DEFAULT current_timestamp)",
		    "create CSR registry table");
		ExecuteStatement(connection,
		                 "CREATE TABLE IF NOT EXISTS __duckpgq_csr_segments ("
		                 "logical_key VARCHAR NOT NULL, generation UBIGINT NOT NULL, csr_kind UTINYINT NOT NULL, "
		                 "partition_index UBIGINT NOT NULL, segment_index UBIGINT NOT NULL, "
		                 "start_vertex UBIGINT NOT NULL, end_vertex UBIGINT NOT NULL, sparse_rows BOOLEAN NOT NULL, "
		                 "source_vertices UINTEGER[] NOT NULL, row_offsets UINTEGER[] NOT NULL, "
		                 "destinations USMALLINT[] NOT NULL, "
		                 "PRIMARY KEY (logical_key, generation, csr_kind, partition_index, segment_index))",
		                 "create CSR segment table");
		ExecuteStatement(
		    connection,
		    "CREATE TABLE IF NOT EXISTS __duckpgq_csr_dependencies ("
		    "logical_key VARCHAR NOT NULL, catalog_name VARCHAR NOT NULL, schema_name VARCHAR NOT NULL, "
		    "table_name VARCHAR NOT NULL, dependency_kind VARCHAR NOT NULL, key_columns VARCHAR[] NOT NULL, "
		    "PRIMARY KEY (logical_key, catalog_name, schema_name, table_name, dependency_kind))",
		    "create CSR dependency table");
		ExecuteStatement(connection, "COMMIT", "commit CSR persistence schema transaction");
	} catch (...) {
		connection.Query("ROLLBACK");
		throw;
	}
}

static uint64_t GetNextGeneration(Connection &connection, const string &logical_key) {
	auto statement = connection.Prepare(
	    "SELECT COALESCE((SELECT generation FROM __duckpgq_csr_registry WHERE logical_key = ?), 0)::UBIGINT");
	if (statement->HasError()) {
		throw IOException("Failed to prepare CSR generation lookup: %s", statement->GetError());
	}
	auto result = ExecutePrepared(*statement, {Value(logical_key)}, "look up CSR generation");
	auto current_generation = result->GetValue(0, 0).GetValue<uint64_t>();
	if (current_generation == std::numeric_limits<uint64_t>::max()) {
		throw OutOfRangeException("CSR generation counter exhausted for logical key '%s'", logical_key);
	}
	return current_generation + 1;
}

static void InsertSegment(PreparedStatement &statement, const string &logical_key, uint64_t generation,
                          idx_t partition_index, idx_t segment_index, idx_t start_vertex, idx_t end_vertex,
                          bool sparse_rows, const std::vector<uint32_t> &source_vertices,
                          const std::vector<uint32_t> &row_offsets, const std::vector<uint16_t> &destinations) {
	vector<Value> values;
	values.reserve(11);
	values.emplace_back(logical_key);
	values.push_back(Value::UBIGINT(generation));
	values.push_back(Value::UTINYINT(FORWARD_CSR_KIND));
	values.push_back(Value::UBIGINT(partition_index));
	values.push_back(Value::UBIGINT(segment_index));
	values.push_back(Value::UBIGINT(start_vertex));
	values.push_back(Value::UBIGINT(end_vertex));
	values.push_back(Value::BOOLEAN(sparse_rows));
	values.push_back(UInt32ListValue(source_vertices));
	values.push_back(UInt32ListValue(row_offsets));
	values.push_back(UInt16ListValue(destinations));
	ExecutePrepared(statement, std::move(values), "write CSR segment");
}

void PersistPartitionedCSR(ClientContext &context, const string &logical_key, const PartitionedCSRIndex &index) {
	if (logical_key.empty() || !ValidatePartitionedCSRIndex(index)) {
		throw InvalidInputException("Refusing to persist an invalid or unidentified partitioned CSR");
	}

	Connection connection(*context.db);
	ExecuteStatement(connection, "BEGIN TRANSACTION", "start CSR persistence transaction");
	try {
		auto generation = GetNextGeneration(connection, logical_key);
		auto insert_segment =
		    connection.Prepare("INSERT INTO __duckpgq_csr_segments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		if (insert_segment->HasError()) {
			throw IOException("Failed to prepare CSR segment insertion: %s", insert_segment->GetError());
		}

		idx_t segment_count = 0;
		for (idx_t partition_idx = 0; partition_idx < index.forward_partitions.size(); partition_idx++) {
			auto &partition = *index.forward_partitions[partition_idx];
			std::vector<uint32_t> row_offsets;
			if (partition.sparse_rows_initialized) {
				row_offsets = partition.row_offsets;
				if (row_offsets.empty()) {
					row_offsets.push_back(0);
				}
			} else {
				row_offsets.reserve(partition.v_array_size);
				for (idx_t offset_idx = 0; offset_idx < partition.v_array_size; offset_idx++) {
					row_offsets.push_back(partition.v[offset_idx].load(std::memory_order_relaxed));
				}
			}
			InsertSegment(*insert_segment, logical_key, generation, partition_idx, 0, partition.start_vertex,
			              partition.end_vertex, partition.sparse_rows_initialized, partition.source_vertices,
			              row_offsets, partition.e);
			segment_count++;
			for (idx_t segment_idx = 0; segment_idx < partition.segments.size(); segment_idx++) {
				auto &segment = partition.segments[segment_idx];
				InsertSegment(*insert_segment, logical_key, generation, partition_idx, segment_idx + 1,
				              partition.start_vertex, partition.end_vertex, true, segment.source_vertices,
				              segment.row_offsets, segment.edges);
				segment_count++;
			}
		}

		auto publish_registry = connection.Prepare(
		    "INSERT INTO __duckpgq_csr_registry "
		    "(logical_key, generation, valid, format_version, vertex_count, edge_count, capabilities, layout, "
		    "partition_count, segment_count) VALUES (?, ?, true, ?, ?, ?, ?, ?, ?, ?) "
		    "ON CONFLICT (logical_key) DO UPDATE SET generation = excluded.generation, valid = excluded.valid, "
		    "format_version = excluded.format_version, vertex_count = excluded.vertex_count, "
		    "edge_count = excluded.edge_count, capabilities = excluded.capabilities, layout = excluded.layout, "
		    "partition_count = excluded.partition_count, segment_count = excluded.segment_count, "
		    "created_at = excluded.created_at");
		if (publish_registry->HasError()) {
			throw IOException("Failed to prepare CSR registry publication: %s", publish_registry->GetError());
		}
		vector<Value> registry_values {Value(logical_key),
		                               Value::UBIGINT(generation),
		                               Value::UINTEGER(PARTITIONED_CSR_FORMAT_VERSION),
		                               Value::UBIGINT(index.vertex_count),
		                               Value::UBIGINT(index.edge_count),
		                               Value::UTINYINT(static_cast<uint8_t>(PartitionedCSRCapabilities::FORWARD)),
		                               Value(PERSISTED_LAYOUT),
		                               Value::UBIGINT(index.forward_partitions.size()),
		                               Value::UBIGINT(segment_count)};
		ExecutePrepared(*publish_registry, std::move(registry_values), "publish CSR registry generation");
		ExecuteStatement(connection, "COMMIT", "commit CSR persistence transaction");
	} catch (...) {
		connection.Query("ROLLBACK");
		throw;
	}
}

static shared_ptr<PartitionedCSRIndex> LoadPersistedPartitionedCSR(Connection &connection, const string &logical_key,
                                                                   idx_t expected_vertex_count,
                                                                   idx_t expected_edge_count,
                                                                   PartitionedCSRCapabilities required_capabilities) {
	auto registry_statement = connection.Prepare(
	    "SELECT generation, valid, format_version, vertex_count, edge_count, capabilities, layout, partition_count, "
	    "segment_count FROM __duckpgq_csr_registry WHERE logical_key = ?");
	if (registry_statement->HasError()) {
		return nullptr;
	}
	auto registry = ExecutePrepared(*registry_statement, {Value(logical_key)}, "load CSR registry generation");
	if (registry->RowCount() != 1) {
		return nullptr;
	}

	auto generation = registry->GetValue(0, 0).GetValue<uint64_t>();
	auto valid = registry->GetValue(1, 0).GetValue<bool>();
	auto format_version = registry->GetValue(2, 0).GetValue<uint32_t>();
	auto vertex_count = registry->GetValue(3, 0).GetValue<uint64_t>();
	auto edge_count = registry->GetValue(4, 0).GetValue<uint64_t>();
	auto capabilities_value = registry->GetValue(5, 0).GetValue<uint8_t>();
	auto layout = registry->GetValue(6, 0).GetValue<string>();
	auto partition_count = registry->GetValue(7, 0).GetValue<uint64_t>();
	auto expected_segment_count = registry->GetValue(8, 0).GetValue<uint64_t>();
	if (!valid || format_version != PARTITIONED_CSR_FORMAT_VERSION || vertex_count != expected_vertex_count ||
	    edge_count != expected_edge_count || layout != PERSISTED_LAYOUT || partition_count == 0 ||
	    (capabilities_value & ~KNOWN_CAPABILITY_MASK) != 0) {
		return nullptr;
	}
	auto capabilities = static_cast<PartitionedCSRCapabilities>(capabilities_value);
	if (!HasPartitionedCSRCapabilities(capabilities, required_capabilities) ||
	    !HasPartitionedCSRCapabilities(capabilities, PartitionedCSRCapabilities::FORWARD)) {
		return nullptr;
	}

	auto segment_statement = connection.Prepare(
	    "SELECT csr_kind, partition_index, segment_index, start_vertex, end_vertex, sparse_rows, source_vertices, "
	    "row_offsets, destinations FROM __duckpgq_csr_segments WHERE logical_key = ? AND generation = ? "
	    "ORDER BY csr_kind, partition_index, segment_index");
	if (segment_statement->HasError()) {
		return nullptr;
	}
	auto segments =
	    ExecutePrepared(*segment_statement, {Value(logical_key), Value::UBIGINT(generation)}, "load CSR segments");
	if (segments->RowCount() != expected_segment_count || expected_segment_count < partition_count) {
		return nullptr;
	}

	auto result = make_shared_ptr<PartitionedCSRIndex>();
	result->vertex_count = vertex_count;
	result->edge_count = edge_count;
	result->capabilities = capabilities;
	idx_t current_partition = DConstants::INVALID_INDEX;
	idx_t expected_segment_index = 0;
	for (idx_t row_idx = 0; row_idx < segments->RowCount(); row_idx++) {
		auto csr_kind = segments->GetValue(0, row_idx).GetValue<uint8_t>();
		auto partition_idx = segments->GetValue(1, row_idx).GetValue<uint64_t>();
		auto segment_idx = segments->GetValue(2, row_idx).GetValue<uint64_t>();
		auto start_vertex = segments->GetValue(3, row_idx).GetValue<uint64_t>();
		auto end_vertex = segments->GetValue(4, row_idx).GetValue<uint64_t>();
		auto sparse_rows = segments->GetValue(5, row_idx).GetValue<bool>();
		auto source_vertices = ReadNumericList<uint32_t>(segments->GetValue(6, row_idx));
		auto row_offsets = ReadNumericList<uint32_t>(segments->GetValue(7, row_idx));
		auto destinations = ReadNumericList<uint16_t>(segments->GetValue(8, row_idx));
		if (csr_kind != FORWARD_CSR_KIND || partition_idx >= partition_count || start_vertex >= end_vertex ||
		    end_vertex > vertex_count || end_vertex - start_vertex > UINT16_MAX) {
			return nullptr;
		}

		if (partition_idx != current_partition) {
			if (segment_idx != 0 || partition_idx != result->forward_partitions.size()) {
				return nullptr;
			}
			current_partition = partition_idx;
			expected_segment_index = 0;
			auto partition = make_shared_ptr<LocalCSR>(start_vertex, end_vertex, vertex_count, !sparse_rows);
			partition->initialized_e = true;
			partition->sparse_rows_initialized = sparse_rows;
			partition->source_vertices = std::move(source_vertices);
			partition->e = std::move(destinations);
			if (sparse_rows) {
				partition->row_offsets = std::move(row_offsets);
			} else {
				if (row_offsets.size() != partition->v_array_size) {
					return nullptr;
				}
				for (idx_t offset_idx = 0; offset_idx < row_offsets.size(); offset_idx++) {
					partition->v[offset_idx].store(row_offsets[offset_idx], std::memory_order_relaxed);
				}
			}
			result->forward_partitions.push_back(std::move(partition));
		} else {
			if (segment_idx != expected_segment_index + 1 || !sparse_rows) {
				return nullptr;
			}
			auto &partition = *result->forward_partitions.back();
			if (partition.start_vertex != start_vertex || partition.end_vertex != end_vertex) {
				return nullptr;
			}
			LocalCSRSegment segment;
			segment.source_vertices = std::move(source_vertices);
			segment.row_offsets = std::move(row_offsets);
			segment.edges = std::move(destinations);
			partition.segments.push_back(std::move(segment));
		}
		expected_segment_index = segment_idx;
	}
	if (result->forward_partitions.size() != partition_count || !ValidatePartitionedCSRIndex(*result)) {
		return nullptr;
	}
	return result;
}

shared_ptr<PartitionedCSRIndex> TryLoadPersistedPartitionedCSR(ClientContext &context, const string &logical_key,
                                                               idx_t expected_vertex_count, idx_t expected_edge_count,
                                                               PartitionedCSRCapabilities required_capabilities) {
	if (logical_key.empty()) {
		return nullptr;
	}
	try {
		Connection connection(*context.db);
		return LoadPersistedPartitionedCSR(connection, logical_key, expected_vertex_count, expected_edge_count,
		                                   required_capabilities);
	} catch (const Exception &) {
		return nullptr;
	} catch (const std::exception &) {
		return nullptr;
	}
}

} // namespace duckdb
