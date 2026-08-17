#include "duckpgq/core/operator/partitioned_csr_persistence.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckpgq/core/utils/duckpgq_sql.hpp"
#include "duckpgq/core/utils/duckpgq_utils.hpp"
#include "duckpgq/parser/parsed_data/create_property_graph_info.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <set>
#include <unordered_map>

namespace duckdb {

static constexpr uint8_t FORWARD_CSR_KIND = 1;
static constexpr uint8_t KNOWN_CAPABILITY_MASK = static_cast<uint8_t>(PartitionedCSRCapabilities::FORWARD) |
                                                 static_cast<uint8_t>(PartitionedCSRCapabilities::REVERSE) |
                                                 static_cast<uint8_t>(PartitionedCSRCapabilities::PULL);
static constexpr const char *PERSISTED_LAYOUT = "destination-partitioned-v1";
static constexpr const char *INVALIDATION_STATE_KEY = "duckpgq_partitioned_csr_invalidation";
static constexpr const char *INSERT_TRIGGER_NAME = "__duckpgq_csr_invalidate_insert";
static constexpr const char *DELETE_TRIGGER_NAME = "__duckpgq_csr_invalidate_delete";
static constexpr const char *UPDATE_TRIGGER_NAME = "__duckpgq_csr_invalidate_update";
static constexpr idx_t PERSISTED_LOAD_PARTITION_BATCH_SIZE = 16;

static mutex partitioned_csr_persist_lock;
static unordered_map<string, weak_ptr<mutex>> partitioned_csr_key_locks;

static shared_ptr<mutex> GetPartitionedCSRPersistLock(const string &logical_key) {
	lock_guard<mutex> guard(partitioned_csr_persist_lock);
	for (auto entry = partitioned_csr_key_locks.begin(); entry != partitioned_csr_key_locks.end();) {
		if (entry->second.expired()) {
			entry = partitioned_csr_key_locks.erase(entry);
		} else {
			entry++;
		}
	}
	auto key_lock = partitioned_csr_key_locks[logical_key].lock();
	if (!key_lock) {
		key_lock = make_shared_ptr<mutex>();
		partitioned_csr_key_locks[logical_key] = key_lock;
	}
	return key_lock;
}

class PartitionedCSRInvalidationState : public ClientContextState {
public:
	void Mark(const string &logical_key) {
		lock_guard<mutex> guard(lock);
		invalidated.insert(logical_key);
	}

	bool Contains(const string &logical_key) {
		lock_guard<mutex> guard(lock);
		return invalidated.find(logical_key) != invalidated.end();
	}

	void TransactionCommit(MetaTransaction &, ClientContext &) override {
		Clear();
	}

	void TransactionRollback(MetaTransaction &, ClientContext &) override {
		Clear();
	}

private:
	void Clear() {
		lock_guard<mutex> guard(lock);
		invalidated.clear();
	}

	mutex lock;
	unordered_set<string> invalidated;
};

void MarkPartitionedCSRInvalidated(ClientContext &context, const string &logical_key) {
	context.registered_state->GetOrCreate<PartitionedCSRInvalidationState>(INVALIDATION_STATE_KEY)->Mark(logical_key);
	for (auto &client_context : ConnectionManager::Get(*context.db).GetConnectionList()) {
		auto state = client_context->registered_state->Get<DuckPGQState>("duckpgq");
		if (state) {
			state->ErasePartitionedCSR(logical_key);
		}
	}
}

bool IsPartitionedCSRInvalidated(ClientContext &context, const string &logical_key) {
	auto state = context.registered_state->Get<PartitionedCSRInvalidationState>(INVALIDATION_STATE_KEY);
	return state && state->Contains(logical_key);
}

bool IsExplicitPartitionedCSRTransaction(ClientContext &context) {
	return !context.transaction.IsAutoCommit();
}

struct PartitionedCSRDependency {
	string catalog_name;
	string schema_name;
	string table_name;
	string dependency_kind;
	vector<string> key_columns;
};

struct PhysicalTableIdentity {
	string catalog_name;
	string schema_name;
	string table_name;

	bool operator<(const PhysicalTableIdentity &other) const {
		return std::tie(catalog_name, schema_name, table_name) <
		       std::tie(other.catalog_name, other.schema_name, other.table_name);
	}
};

static void ThrowOnQueryError(const QueryResult &result, const string &operation) {
	if (result.HasError()) {
		if (result.GetErrorType() == ExceptionType::TRANSACTION) {
			throw TransactionException("Failed to %s: %s", operation, result.GetError());
		}
		result.ThrowError("Failed to " + operation + ": ");
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

static const vector<LogicalType> PERSISTED_SEGMENT_TYPES {
    LogicalType::VARCHAR,
    LogicalType::UBIGINT,
    LogicalType::UTINYINT,
    LogicalType::UBIGINT,
    LogicalType::UBIGINT,
    LogicalType::UBIGINT,
    LogicalType::UBIGINT,
    LogicalType::BOOLEAN,
    LogicalType::LIST(LogicalType::UINTEGER),
    LogicalType::LIST(LogicalType::UINTEGER),
    LogicalType::LIST(LogicalType::USMALLINT),
};

template <class T>
static void SetNumericList(Vector &list_vector, const std::vector<T> &input) {
	auto entries = FlatVector::GetDataMutable<list_entry_t>(list_vector);
	entries[0] = list_entry_t(0, input.size());
	ListVector::Reserve(list_vector, input.size());
	auto &child = ListVector::GetChildMutable(list_vector);
	if (!input.empty()) {
		auto child_data = FlatVector::GetDataMutable<T>(child);
		memcpy(child_data, input.data(), input.size() * sizeof(T));
	}
	ListVector::SetListSize(list_vector, input.size());
}

template <class T>
static bool ReadScalar(Vector &vector, idx_t row_idx, T &result) {
	UnifiedVectorFormat format;
	vector.ToUnifiedFormat(format);
	auto vector_idx = format.sel->get_index(row_idx);
	if (!format.validity.RowIsValid(vector_idx)) {
		return false;
	}
	result = format.GetData<T>()[vector_idx];
	return true;
}

template <class T>
static bool ReadNumericList(Vector &list_vector, idx_t row_idx, std::vector<T> &result) {
	UnifiedVectorFormat list_format;
	list_vector.ToUnifiedFormat(list_format);
	auto list_idx = list_format.sel->get_index(row_idx);
	if (!list_format.validity.RowIsValid(list_idx)) {
		return false;
	}
	auto entry = list_format.GetData<list_entry_t>()[list_idx];
	auto &child = ListVector::GetChild(list_vector);
	UnifiedVectorFormat child_format;
	child.ToUnifiedFormat(child_format);
	result.resize(entry.length);
	if (entry.length == 0) {
		return true;
	}
	auto child_data = child_format.GetData<T>();
	if (child_format.sel == FlatVector::IncrementalSelectionVector() && child_format.validity.CannotHaveNull()) {
		memcpy(result.data(), child_data + entry.offset, entry.length * sizeof(T));
		return true;
	}
	for (idx_t child_idx = 0; child_idx < entry.length; child_idx++) {
		auto source_idx = child_format.sel->get_index(entry.offset + child_idx);
		if (!child_format.validity.RowIsValid(source_idx)) {
			return false;
		}
		result[child_idx] = child_data[source_idx];
	}
	return true;
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

static vector<string> IdentifierNames(const vector<Identifier> &identifiers) {
	vector<string> result;
	result.reserve(identifiers.size());
	for (const auto &identifier : identifiers) {
		result.push_back(identifier.GetIdentifierName());
	}
	return result;
}

static void MergeDependency(vector<PartitionedCSRDependency> &dependencies, const PropertyGraphTable &table,
                            const string &kind, const vector<Identifier> &columns) {
	auto catalog_name = table.catalog_name.GetIdentifierName();
	auto schema_name = table.schema_name.GetIdentifierName();
	auto table_name = table.table_name.GetIdentifierName();
	for (auto &dependency : dependencies) {
		if (dependency.catalog_name != catalog_name || dependency.schema_name != schema_name ||
		    dependency.table_name != table_name || dependency.dependency_kind != kind) {
			continue;
		}
		for (const auto &column : IdentifierNames(columns)) {
			if (std::find(dependency.key_columns.begin(), dependency.key_columns.end(), column) ==
			    dependency.key_columns.end()) {
				dependency.key_columns.push_back(column);
			}
		}
		std::sort(dependency.key_columns.begin(), dependency.key_columns.end());
		return;
	}
	PartitionedCSRDependency dependency {catalog_name, schema_name, table_name, kind, IdentifierNames(columns)};
	std::sort(dependency.key_columns.begin(), dependency.key_columns.end());
	dependencies.push_back(std::move(dependency));
}

static vector<PartitionedCSRDependency> ResolveDependencies(ClientContext &context, const string &base_cache_key) {
	vector<PartitionedCSRDependency> result;
	if (base_cache_key.empty()) {
		return result;
	}
	auto state = GetDuckPGQState(context);
	for (const auto &property_graph_entry : state->registered_property_graphs) {
		auto &property_graph = property_graph_entry.second->Cast<CreatePropertyGraphInfo>();
		for (const auto &edge_table : property_graph.edge_tables) {
			auto key_prefix = property_graph.property_graph_name + "|" + edge_table->FullTableName() + "|";
			if (base_cache_key != key_prefix + "directed" && base_cache_key != key_prefix + "undirected") {
				continue;
			}
			MergeDependency(result, *edge_table, "edge", edge_table->source_fk);
			MergeDependency(result, *edge_table, "edge", edge_table->destination_fk);
			if (edge_table->source_pg_table) {
				MergeDependency(result, *edge_table->source_pg_table, "vertex", edge_table->source_pk);
			}
			if (edge_table->destination_pg_table) {
				MergeDependency(result, *edge_table->destination_pg_table, "vertex", edge_table->destination_pk);
			}
			auto default_catalog = DatabaseManager::GetDefaultDatabase(context).GetIdentifierName();
			for (auto &dependency : result) {
				if (dependency.catalog_name.empty()) {
					dependency.catalog_name = default_catalog;
				}
				if (dependency.schema_name.empty()) {
					dependency.schema_name = "main";
				}
			}
			return result;
		}
	}
	return result;
}

static string PersistenceTable(ClientContext &context, const string &table_name) {
	return DuckPGQSQL::QualifiedTableName(DatabaseManager::GetDefaultDatabase(context).GetIdentifierName(), "main",
	                                      table_name);
}

static vector<string> ReadStringList(const Value &value) {
	vector<string> result;
	if (value.IsNull()) {
		return result;
	}
	for (const auto &child : ListValue::GetChildren(value)) {
		result.push_back(child.GetValue<string>());
	}
	return result;
}

struct TriggerRecord {
	string event;
	vector<string> columns;
	string for_each;
	string sql;
};

static optional<TriggerRecord> FindTrigger(Connection &connection, const PhysicalTableIdentity &table,
                                           const string &trigger_name) {
	auto statement =
	    connection.Prepare("SELECT event_manipulation, columns, for_each, sql FROM duckdb_triggers() "
	                       "WHERE database_name = ? AND schema_name = ? AND table_name = ? AND trigger_name = ?");
	if (statement->HasError()) {
		throw IOException("Failed to prepare owned CSR trigger lookup: %s", statement->GetError());
	}
	auto result = ExecutePrepared(
	    *statement, {Value(table.catalog_name), Value(table.schema_name), Value(table.table_name), Value(trigger_name)},
	    "look up owned CSR trigger");
	if (result->RowCount() == 0) {
		return optional<TriggerRecord>();
	}
	if (result->RowCount() != 1) {
		throw InternalException("Expected one trigger named '%s' on table '%s.%s.%s'", trigger_name, table.catalog_name,
		                        table.schema_name, table.table_name);
	}
	return TriggerRecord {result->GetValue(0, 0).GetValue<string>(), ReadStringList(result->GetValue(1, 0)),
	                      result->GetValue(2, 0).GetValue<string>(), result->GetValue(3, 0).GetValue<string>()};
}

static optional<TriggerRecord> FindTriggerOwner(Connection &connection, const PhysicalTableIdentity &table,
                                                const string &trigger_name) {
	auto statement = connection.Prepare(
	    "SELECT event_manipulation, key_columns, 'STATEMENT', trigger_sql "
	    "FROM __duckpgq_csr_trigger_owners WHERE catalog_name = ? AND schema_name = ? AND table_name = ? "
	    "AND trigger_name = ?");
	if (statement->HasError()) {
		throw IOException("Failed to prepare CSR trigger owner lookup: %s", statement->GetError());
	}
	auto result = ExecutePrepared(
	    *statement, {Value(table.catalog_name), Value(table.schema_name), Value(table.table_name), Value(trigger_name)},
	    "look up CSR trigger owner");
	if (result->RowCount() == 0) {
		return optional<TriggerRecord>();
	}
	return TriggerRecord {result->GetValue(0, 0).GetValue<string>(), ReadStringList(result->GetValue(1, 0)),
	                      result->GetValue(2, 0).GetValue<string>(), result->GetValue(3, 0).GetValue<string>()};
}

static void VerifyOwnedTrigger(const PhysicalTableIdentity &table, const string &trigger_name,
                               const TriggerRecord &owner, const optional<TriggerRecord> &actual) {
	if (!actual || owner.event != actual->event || owner.columns != actual->columns ||
	    actual->for_each != "STATEMENT" || !StringUtil::Contains(actual->sql, "duckpgq_mark_csr_invalid") ||
	    !StringUtil::Contains(actual->sql, "__duckpgq_csr_registry") ||
	    !StringUtil::Contains(actual->sql, "__duckpgq_csr_dependencies")) {
		throw InvalidInputException(
		    "Owned CSR invalidation trigger '%s' on table '%s.%s.%s' is missing or was modified "
		    "(owner event '%s', actual event '%s', owner columns %llu, actual columns %llu, for-each '%s'); "
		    "refusing to replace it automatically",
		    trigger_name, table.catalog_name, table.schema_name, table.table_name, owner.event,
		    actual ? actual->event : "<missing>", owner.columns.size(), actual ? actual->columns.size() : 0,
		    actual ? actual->for_each : "<missing>");
	}
}

static string CreateTriggerSQL(ClientContext &context, const PhysicalTableIdentity &table, const string &trigger_name,
                               const string &event, const vector<string> &columns) {
	auto table_sql = DuckPGQSQL::QualifiedTableName(table.catalog_name, table.schema_name, table.table_name);
	auto changed_rows = string("__duckpgq_changed_rows");
	std::ostringstream sql;
	sql << "CREATE TRIGGER " << DuckPGQSQL::Identifier(trigger_name) << " AFTER " << event;
	if (event == "UPDATE") {
		sql << " OF ";
		for (idx_t column_idx = 0; column_idx < columns.size(); column_idx++) {
			if (column_idx > 0) {
				sql << ", ";
			}
			sql << DuckPGQSQL::Identifier(columns[column_idx]);
		}
	}
	sql << " ON " << table_sql;
	if (event != "UPDATE") {
		sql << " REFERENCING " << (event == "DELETE" ? "OLD" : "NEW") << " TABLE AS "
		    << DuckPGQSQL::Identifier(changed_rows);
	}
	sql << " FOR EACH STATEMENT UPDATE " << PersistenceTable(context, "__duckpgq_csr_registry")
	    << " AS registry SET valid = duckpgq_mark_csr_invalid(registry.logical_key), "
	       "generation = registry.generation + 1 FROM "
	    << PersistenceTable(context, "__duckpgq_csr_dependencies")
	    << " AS dependency WHERE registry.logical_key = dependency.logical_key AND dependency.catalog_name = "
	    << DuckPGQSQL::StringLiteral(table.catalog_name)
	    << " AND dependency.schema_name = " << DuckPGQSQL::StringLiteral(table.schema_name)
	    << " AND dependency.table_name = " << DuckPGQSQL::StringLiteral(table.table_name);
	if (event != "UPDATE") {
		sql << " AND EXISTS (SELECT 1 FROM " << DuckPGQSQL::Identifier(changed_rows) << ")";
	}
	return sql.str();
}

static void StoreTriggerOwner(Connection &connection, const PhysicalTableIdentity &table, const string &trigger_name,
                              const string &event, const vector<string> &columns, const string &trigger_sql) {
	auto statement =
	    connection.Prepare("INSERT INTO __duckpgq_csr_trigger_owners VALUES (?, ?, ?, ?, ?, ?, ?) "
	                       "ON CONFLICT (catalog_name, schema_name, table_name, trigger_name) DO UPDATE SET "
	                       "event_manipulation = excluded.event_manipulation, key_columns = excluded.key_columns, "
	                       "trigger_sql = excluded.trigger_sql");
	if (statement->HasError()) {
		throw IOException("Failed to prepare CSR trigger owner write: %s", statement->GetError());
	}
	vector<Value> column_values;
	for (const auto &column : columns) {
		column_values.emplace_back(column);
	}
	ExecutePrepared(*statement,
	                {Value(table.catalog_name), Value(table.schema_name), Value(table.table_name), Value(trigger_name),
	                 Value(event), Value::LIST(LogicalType::VARCHAR, std::move(column_values)), Value(trigger_sql)},
	                "store CSR trigger ownership");
}

static void EnsureTrigger(ClientContext &context, Connection &connection, const PhysicalTableIdentity &table,
                          const string &trigger_name, const string &event, const vector<string> &columns) {
	auto owner = FindTriggerOwner(connection, table, trigger_name);
	auto actual = FindTrigger(connection, table, trigger_name);
	if (!owner) {
		if (actual) {
			throw InvalidInputException(
			    "Trigger name '%s' on table '%s.%s.%s' is reserved for CSR invalidation but is already user-owned",
			    trigger_name, table.catalog_name, table.schema_name, table.table_name);
		}
		auto trigger_sql = CreateTriggerSQL(context, table, trigger_name, event, columns);
		ExecuteStatement(connection, trigger_sql, "create CSR invalidation trigger");
		StoreTriggerOwner(connection, table, trigger_name, event, columns, trigger_sql);
		return;
	}
	VerifyOwnedTrigger(table, trigger_name, *owner, actual);
	auto trigger_sql = CreateTriggerSQL(context, table, trigger_name, event, columns);
	if (owner->event == event && owner->columns == columns && owner->sql == trigger_sql) {
		return;
	}
	auto table_sql = DuckPGQSQL::QualifiedTableName(table.catalog_name, table.schema_name, table.table_name);
	ExecuteStatement(connection, "DROP TRIGGER " + DuckPGQSQL::Identifier(trigger_name) + " ON " + table_sql,
	                 "replace owned CSR invalidation trigger");
	ExecuteStatement(connection, trigger_sql, "recreate owned CSR invalidation trigger");
	StoreTriggerOwner(connection, table, trigger_name, event, columns, trigger_sql);
}

static void DropOwnedTrigger(Connection &connection, const PhysicalTableIdentity &table, const string &trigger_name) {
	auto owner = FindTriggerOwner(connection, table, trigger_name);
	if (!owner) {
		return;
	}
	auto actual = FindTrigger(connection, table, trigger_name);
	VerifyOwnedTrigger(table, trigger_name, *owner, actual);
	auto table_sql = DuckPGQSQL::QualifiedTableName(table.catalog_name, table.schema_name, table.table_name);
	ExecuteStatement(connection, "DROP TRIGGER " + DuckPGQSQL::Identifier(trigger_name) + " ON " + table_sql,
	                 "drop owned CSR invalidation trigger");
	auto statement = connection.Prepare(
	    "DELETE FROM __duckpgq_csr_trigger_owners WHERE catalog_name = ? AND schema_name = ? AND table_name = ? "
	    "AND trigger_name = ?");
	ExecutePrepared(*statement,
	                {Value(table.catalog_name), Value(table.schema_name), Value(table.table_name), Value(trigger_name)},
	                "delete CSR trigger ownership");
}

static void RefreshOwnedTriggers(ClientContext &context, Connection &connection, const PhysicalTableIdentity &table) {
	auto statement = connection.Prepare(
	    "SELECT key_columns FROM __duckpgq_csr_dependencies WHERE catalog_name = ? AND schema_name = ? "
	    "AND table_name = ?");
	if (statement->HasError()) {
		throw IOException("Failed to prepare CSR dependency refcount lookup: %s", statement->GetError());
	}
	auto dependencies =
	    ExecutePrepared(*statement, {Value(table.catalog_name), Value(table.schema_name), Value(table.table_name)},
	                    "look up CSR dependency refcount");
	if (dependencies->RowCount() == 0) {
		DropOwnedTrigger(connection, table, INSERT_TRIGGER_NAME);
		DropOwnedTrigger(connection, table, DELETE_TRIGGER_NAME);
		DropOwnedTrigger(connection, table, UPDATE_TRIGGER_NAME);
		return;
	}
	set<string> update_column_set;
	for (idx_t row_idx = 0; row_idx < dependencies->RowCount(); row_idx++) {
		for (const auto &column : ReadStringList(dependencies->GetValue(0, row_idx))) {
			update_column_set.insert(column);
		}
	}
	vector<string> update_columns(update_column_set.begin(), update_column_set.end());
	if (update_columns.empty()) {
		throw InternalException("CSR dependency on table '%s.%s.%s' has no key columns", table.catalog_name,
		                        table.schema_name, table.table_name);
	}
	EnsureTrigger(context, connection, table, INSERT_TRIGGER_NAME, "INSERT", {});
	EnsureTrigger(context, connection, table, DELETE_TRIGGER_NAME, "DELETE", {});
	EnsureTrigger(context, connection, table, UPDATE_TRIGGER_NAME, "UPDATE", update_columns);
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
		ExecuteStatement(
		    connection,
		    "CREATE TABLE IF NOT EXISTS __duckpgq_csr_trigger_owners ("
		    "catalog_name VARCHAR NOT NULL, schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, "
		    "trigger_name VARCHAR NOT NULL, event_manipulation VARCHAR NOT NULL, key_columns VARCHAR[] NOT NULL, "
		    "trigger_sql VARCHAR NOT NULL, PRIMARY KEY (catalog_name, schema_name, table_name, trigger_name))",
		    "create CSR trigger ownership table");
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

uint64_t GetPartitionedCSRGeneration(ClientContext &context, const string &logical_key) {
	if (logical_key.empty()) {
		return 0;
	}
	try {
		Connection connection(*context.db);
		auto statement = connection.Prepare("SELECT generation FROM __duckpgq_csr_registry WHERE logical_key = ?");
		if (statement->HasError()) {
			return 0;
		}
		auto result = ExecutePrepared(*statement, {Value(logical_key)}, "read CSR generation token");
		return result->RowCount() == 1 ? result->GetValue(0, 0).GetValue<uint64_t>() : 0;
	} catch (const Exception &) {
		return 0;
	} catch (const std::exception &) {
		return 0;
	}
}

static void AppendSegment(Appender &appender, const string &logical_key, uint64_t generation, idx_t partition_index,
                          idx_t segment_index, idx_t start_vertex, idx_t end_vertex, bool sparse_rows,
                          const std::vector<uint32_t> &source_vertices, const std::vector<uint32_t> &row_offsets,
                          const std::vector<uint16_t> &destinations) {
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), PERSISTED_SEGMENT_TYPES, 1);
	FlatVector::GetDataMutable<string_t>(chunk.data[0])[0] = StringVector::AddString(chunk.data[0], logical_key);
	FlatVector::GetDataMutable<uint64_t>(chunk.data[1])[0] = generation;
	FlatVector::GetDataMutable<uint8_t>(chunk.data[2])[0] = FORWARD_CSR_KIND;
	FlatVector::GetDataMutable<uint64_t>(chunk.data[3])[0] = partition_index;
	FlatVector::GetDataMutable<uint64_t>(chunk.data[4])[0] = segment_index;
	FlatVector::GetDataMutable<uint64_t>(chunk.data[5])[0] = start_vertex;
	FlatVector::GetDataMutable<uint64_t>(chunk.data[6])[0] = end_vertex;
	FlatVector::GetDataMutable<bool>(chunk.data[7])[0] = sparse_rows;
	SetNumericList<uint32_t>(chunk.data[8], source_vertices);
	SetNumericList<uint32_t>(chunk.data[9], row_offsets);
	SetNumericList<uint16_t>(chunk.data[10], destinations);
	chunk.SetChildCardinality(1);
	appender.AppendDataChunk(chunk);
}

uint64_t PersistPartitionedCSR(ClientContext &context, const string &logical_key, const string &base_cache_key,
                               uint64_t expected_generation, const PartitionedCSRIndex &index) {
	if (logical_key.empty() || !ValidatePartitionedCSRIndex(index)) {
		throw InvalidInputException("Refusing to persist an invalid or unidentified partitioned CSR");
	}
	auto resolved_dependencies = ResolveDependencies(context, base_cache_key);
	auto key_lock = GetPartitionedCSRPersistLock(logical_key);
	lock_guard<mutex> persist_guard(*key_lock);

	Connection connection(*context.db);
	ExecuteStatement(connection, "BEGIN TRANSACTION", "start CSR persistence transaction");
	try {
		auto current_generation = GetNextGeneration(connection, logical_key) - 1;
		if (current_generation != expected_generation) {
			ExecuteStatement(connection, "ROLLBACK", "discard superseded CSR persistence transaction");
			return 0;
		}
		auto generation = current_generation + 1;
		set<PhysicalTableIdentity> affected_tables;
		auto old_dependencies =
		    connection.Prepare("SELECT DISTINCT catalog_name, schema_name, table_name FROM __duckpgq_csr_dependencies "
		                       "WHERE logical_key = ?");
		if (old_dependencies->HasError()) {
			throw IOException("Failed to prepare old CSR dependency lookup: %s", old_dependencies->GetError());
		}
		auto old_dependency_rows =
		    ExecutePrepared(*old_dependencies, {Value(logical_key)}, "look up old CSR dependencies");
		for (idx_t row_idx = 0; row_idx < old_dependency_rows->RowCount(); row_idx++) {
			affected_tables.insert({old_dependency_rows->GetValue(0, row_idx).GetValue<string>(),
			                        old_dependency_rows->GetValue(1, row_idx).GetValue<string>(),
			                        old_dependency_rows->GetValue(2, row_idx).GetValue<string>()});
		}
		Appender segment_appender(connection, "__duckpgq_csr_segments", 16ULL * 1024ULL * 1024ULL);

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
			AppendSegment(segment_appender, logical_key, generation, partition_idx, 0, partition.start_vertex,
			              partition.end_vertex, partition.sparse_rows_initialized, partition.source_vertices,
			              row_offsets, partition.e);
			segment_count++;
			for (idx_t segment_idx = 0; segment_idx < partition.segments.size(); segment_idx++) {
				auto &segment = partition.segments[segment_idx];
				AppendSegment(segment_appender, logical_key, generation, partition_idx, segment_idx + 1,
				              partition.start_vertex, partition.end_vertex, true, segment.source_vertices,
				              segment.row_offsets, segment.edges);
				segment_count++;
			}
		}
		segment_appender.Close();

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

		auto delete_dependencies = connection.Prepare("DELETE FROM __duckpgq_csr_dependencies WHERE logical_key = ?");
		if (delete_dependencies->HasError()) {
			throw IOException("Failed to prepare CSR dependency replacement: %s", delete_dependencies->GetError());
		}
		ExecutePrepared(*delete_dependencies, {Value(logical_key)}, "replace CSR dependencies");
		auto insert_dependency = connection.Prepare("INSERT INTO __duckpgq_csr_dependencies VALUES (?, ?, ?, ?, ?, ?)");
		if (insert_dependency->HasError()) {
			throw IOException("Failed to prepare CSR dependency insertion: %s", insert_dependency->GetError());
		}
		for (const auto &dependency : resolved_dependencies) {
			vector<Value> key_columns;
			for (const auto &column : dependency.key_columns) {
				key_columns.emplace_back(column);
			}
			ExecutePrepared(*insert_dependency,
			                {Value(logical_key), Value(dependency.catalog_name), Value(dependency.schema_name),
			                 Value(dependency.table_name), Value(dependency.dependency_kind),
			                 Value::LIST(LogicalType::VARCHAR, std::move(key_columns))},
			                "insert CSR dependency");
			affected_tables.insert({dependency.catalog_name, dependency.schema_name, dependency.table_name});
		}
		for (const auto &table : affected_tables) {
			RefreshOwnedTriggers(context, connection, table);
		}
		ExecuteStatement(connection, "COMMIT", "commit CSR persistence transaction");
		return generation;
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
	    "AND partition_index >= ? AND partition_index < ?");
	if (segment_statement->HasError()) {
		return nullptr;
	}
	if (expected_segment_count < partition_count) {
		return nullptr;
	}
	struct DecodedSegment {
		uint8_t csr_kind;
		uint64_t partition_idx;
		uint64_t segment_idx;
		uint64_t start_vertex;
		uint64_t end_vertex;
		bool sparse_rows;
		std::vector<uint32_t> source_vertices;
		std::vector<uint32_t> row_offsets;
		std::vector<uint16_t> destinations;
	};
	auto result = make_shared_ptr<PartitionedCSRIndex>();
	result->vertex_count = vertex_count;
	result->edge_count = edge_count;
	result->persisted_generation = generation;
	result->capabilities = capabilities;
	idx_t current_partition = DConstants::INVALID_INDEX;
	idx_t expected_segment_index = 0;
	idx_t loaded_segment_count = 0;
	for (idx_t partition_begin = 0; partition_begin < partition_count;
	     partition_begin += PERSISTED_LOAD_PARTITION_BATCH_SIZE) {
		auto partition_end = MinValue<idx_t>(partition_count, partition_begin + PERSISTED_LOAD_PARTITION_BATCH_SIZE);
		vector<DecodedSegment> decoded_batch;
		vector<Value> segment_parameters {Value(logical_key), Value::UBIGINT(generation),
		                                  Value::UBIGINT(partition_begin), Value::UBIGINT(partition_end)};
		auto segments = segment_statement->Execute(segment_parameters, true);
		ThrowOnQueryError(*segments, "load CSR segments");
		while (auto chunk = segments->Fetch()) {
			for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
				if (loaded_segment_count + decoded_batch.size() >= expected_segment_count) {
					return nullptr;
				}
				DecodedSegment decoded;
				if (!ReadScalar<uint8_t>(chunk->data[0], row_idx, decoded.csr_kind) ||
				    !ReadScalar<uint64_t>(chunk->data[1], row_idx, decoded.partition_idx) ||
				    !ReadScalar<uint64_t>(chunk->data[2], row_idx, decoded.segment_idx) ||
				    !ReadScalar<uint64_t>(chunk->data[3], row_idx, decoded.start_vertex) ||
				    !ReadScalar<uint64_t>(chunk->data[4], row_idx, decoded.end_vertex) ||
				    !ReadScalar<bool>(chunk->data[5], row_idx, decoded.sparse_rows) ||
				    !ReadNumericList<uint32_t>(chunk->data[6], row_idx, decoded.source_vertices) ||
				    !ReadNumericList<uint32_t>(chunk->data[7], row_idx, decoded.row_offsets) ||
				    !ReadNumericList<uint16_t>(chunk->data[8], row_idx, decoded.destinations)) {
					return nullptr;
				}
				if (decoded.csr_kind != FORWARD_CSR_KIND || decoded.partition_idx < partition_begin ||
				    decoded.partition_idx >= partition_end || decoded.start_vertex >= decoded.end_vertex ||
				    decoded.end_vertex > vertex_count || decoded.end_vertex - decoded.start_vertex > UINT16_MAX) {
					return nullptr;
				}
				decoded_batch.push_back(std::move(decoded));
			}
		}
		ThrowOnQueryError(*segments, "load CSR segments");
		std::sort(decoded_batch.begin(), decoded_batch.end(),
		          [](const DecodedSegment &left, const DecodedSegment &right) {
			          return std::tie(left.csr_kind, left.partition_idx, left.segment_idx) <
			                 std::tie(right.csr_kind, right.partition_idx, right.segment_idx);
		          });
		for (auto &decoded : decoded_batch) {
			if (decoded.partition_idx != current_partition) {
				if (decoded.segment_idx != 0 || decoded.partition_idx != result->forward_partitions.size()) {
					return nullptr;
				}
				current_partition = decoded.partition_idx;
				expected_segment_index = 0;
				auto partition = make_shared_ptr<LocalCSR>(decoded.start_vertex, decoded.end_vertex, vertex_count,
				                                           !decoded.sparse_rows);
				partition->initialized_e = true;
				partition->sparse_rows_initialized = decoded.sparse_rows;
				partition->source_vertices = std::move(decoded.source_vertices);
				partition->e = std::move(decoded.destinations);
				if (decoded.sparse_rows) {
					partition->row_offsets = std::move(decoded.row_offsets);
				} else {
					if (decoded.row_offsets.size() != partition->v_array_size) {
						return nullptr;
					}
					for (idx_t offset_idx = 0; offset_idx < decoded.row_offsets.size(); offset_idx++) {
						partition->v[offset_idx].store(decoded.row_offsets[offset_idx], std::memory_order_relaxed);
					}
				}
				result->forward_partitions.push_back(std::move(partition));
			} else {
				if (decoded.segment_idx != expected_segment_index + 1 || !decoded.sparse_rows) {
					return nullptr;
				}
				auto &partition = *result->forward_partitions.back();
				if (partition.start_vertex != decoded.start_vertex || partition.end_vertex != decoded.end_vertex) {
					return nullptr;
				}
				LocalCSRSegment segment;
				segment.source_vertices = std::move(decoded.source_vertices);
				segment.row_offsets = std::move(decoded.row_offsets);
				segment.edges = std::move(decoded.destinations);
				partition.segments.push_back(std::move(segment));
			}
			expected_segment_index = decoded.segment_idx;
		}
		loaded_segment_count += decoded_batch.size();
	}
	if (loaded_segment_count != expected_segment_count || result->forward_partitions.size() != partition_count ||
	    !ValidatePartitionedCSRIndex(*result)) {
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

bool IsPersistedPartitionedCSRCurrent(ClientContext &context, const string &logical_key,
                                      const PartitionedCSRIndex &index) {
	if (logical_key.empty() || index.persisted_generation == 0 || IsPartitionedCSRInvalidated(context, logical_key)) {
		return false;
	}
	try {
		Connection connection(*context.db);
		auto statement = connection.Prepare(
		    "SELECT valid, generation, format_version, vertex_count, edge_count, capabilities, layout "
		    "FROM __duckpgq_csr_registry WHERE logical_key = ?");
		if (statement->HasError()) {
			return false;
		}
		auto result = ExecutePrepared(*statement, {Value(logical_key)}, "validate cached CSR generation");
		if (result->RowCount() != 1) {
			return false;
		}
		auto capabilities = result->GetValue(5, 0).GetValue<uint8_t>();
		return result->GetValue(0, 0).GetValue<bool>() &&
		       result->GetValue(1, 0).GetValue<uint64_t>() == index.persisted_generation &&
		       result->GetValue(2, 0).GetValue<uint32_t>() == PARTITIONED_CSR_FORMAT_VERSION &&
		       result->GetValue(3, 0).GetValue<uint64_t>() == index.vertex_count &&
		       result->GetValue(4, 0).GetValue<uint64_t>() == index.edge_count &&
		       result->GetValue(6, 0).GetValue<string>() == PERSISTED_LAYOUT &&
		       (capabilities & ~KNOWN_CAPABILITY_MASK) == 0 &&
		       HasPartitionedCSRCapabilities(static_cast<PartitionedCSRCapabilities>(capabilities),
		                                     PartitionedCSRCapabilities::FORWARD);
	} catch (const Exception &) {
		return false;
	} catch (const std::exception &) {
		return false;
	}
}

static bool LogicalKeyBelongsToPropertyGraph(const string &logical_key, const string &property_graph_name) {
	static const string prefix = "partitioned-csr-logical-v1|graph=";
	if (!StringUtil::StartsWith(logical_key, prefix)) {
		return false;
	}
	auto length_end = logical_key.find(':', prefix.size());
	if (length_end == string::npos) {
		return false;
	}
	idx_t base_key_length;
	try {
		base_key_length = std::stoull(logical_key.substr(prefix.size(), length_end - prefix.size()));
	} catch (...) {
		return false;
	}
	auto base_key_start = length_end + 1;
	if (base_key_length > logical_key.size() - base_key_start) {
		return false;
	}
	auto base_key = logical_key.substr(base_key_start, base_key_length);
	return StringUtil::StartsWith(base_key, property_graph_name + "|");
}

void DropPartitionedCSRArtifacts(ClientContext &context, const CreatePropertyGraphInfo &property_graph) {
	Connection connection(*context.db);
	ExecuteStatement(connection, "BEGIN TRANSACTION", "start property-graph CSR cleanup transaction");
	vector<string> logical_keys;
	try {
		auto key_result = connection.Query("SELECT logical_key FROM __duckpgq_csr_registry UNION "
		                                   "SELECT logical_key FROM __duckpgq_csr_dependencies UNION "
		                                   "SELECT logical_key FROM __duckpgq_csr_segments");
		ThrowOnQueryError(*key_result, "list property-graph CSR artifacts");
		auto &materialized = key_result->Cast<MaterializedQueryResult>();
		for (idx_t row_idx = 0; row_idx < materialized.RowCount(); row_idx++) {
			auto logical_key = materialized.GetValue(0, row_idx).GetValue<string>();
			if (LogicalKeyBelongsToPropertyGraph(logical_key, property_graph.property_graph_name)) {
				logical_keys.push_back(std::move(logical_key));
			}
		}

		set<PhysicalTableIdentity> affected_tables;
		auto find_dependencies =
		    connection.Prepare("SELECT DISTINCT catalog_name, schema_name, table_name FROM __duckpgq_csr_dependencies "
		                       "WHERE logical_key = ?");
		auto delete_dependencies = connection.Prepare("DELETE FROM __duckpgq_csr_dependencies WHERE logical_key = ?");
		auto delete_segments = connection.Prepare("DELETE FROM __duckpgq_csr_segments WHERE logical_key = ?");
		auto delete_registry = connection.Prepare("DELETE FROM __duckpgq_csr_registry WHERE logical_key = ?");
		for (const auto &logical_key : logical_keys) {
			auto dependencies = ExecutePrepared(*find_dependencies, {Value(logical_key)}, "find CSR cleanup tables");
			for (idx_t row_idx = 0; row_idx < dependencies->RowCount(); row_idx++) {
				affected_tables.insert({dependencies->GetValue(0, row_idx).GetValue<string>(),
				                        dependencies->GetValue(1, row_idx).GetValue<string>(),
				                        dependencies->GetValue(2, row_idx).GetValue<string>()});
			}
			ExecutePrepared(*delete_dependencies, {Value(logical_key)}, "delete CSR dependencies");
			ExecutePrepared(*delete_segments, {Value(logical_key)}, "delete CSR segments");
			ExecutePrepared(*delete_registry, {Value(logical_key)}, "delete CSR registry row");
		}
		for (const auto &table : affected_tables) {
			RefreshOwnedTriggers(context, connection, table);
		}
		ExecuteStatement(connection, "COMMIT", "commit property-graph CSR cleanup transaction");
	} catch (...) {
		connection.Query("ROLLBACK");
		throw;
	}
	for (auto &client_context : ConnectionManager::Get(*context.db).GetConnectionList()) {
		auto state = client_context->registered_state->Get<DuckPGQState>("duckpgq");
		if (!state) {
			continue;
		}
		for (const auto &logical_key : logical_keys) {
			state->ErasePartitionedCSR(logical_key);
		}
	}
}

} // namespace duckdb
