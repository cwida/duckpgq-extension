#include "duckpgq/core/functions/scalar.hpp"
#include "duckpgq/core/index/csr_index.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

namespace {

struct ResolvedCSRIndex {
	optional_ptr<DuckTableEntry> table;
	shared_ptr<IndexEntry> entry;
};

ResolvedCSRIndex ResolveCSRIndex(ClientContext &context, const string &table_name, const string &index_name) {
	auto table = CSRIndex::FindTable(context, table_name);
	if (!table) {
		throw CatalogException("Table '%s' does not exist", table_name);
	}
	auto &storage = table->GetStorage();
	storage.BindIndexes(context);
	auto entry = storage.GetDataTableInfo()->GetIndexes().FindEntry(Identifier(index_name));
	if (!entry || entry->GetIndexType() != CSRIndex::TYPE_NAME) {
		throw CatalogException("Table '%s' has no CSR index named '%s'", table_name, index_name);
	}
	return {table, entry};
}

//! duckpgq_csr_index_fold(table, index): builds a new epoch now and compacts the change log
void CSRIndexFoldFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto table_name = args.data[0].GetValue(0).GetValue<string>();
	auto index_name = args.data[1].GetValue(0).GetValue<string>();
	auto resolved = ResolveCSRIndex(context, table_name, index_name);
	auto handle = resolved.entry->GetReadHandle<CSRIndex>();
	auto &transaction = DuckTransaction::Get(context, resolved.table->catalog);
	handle->Fold(context, transaction, resolved.table->GetStorage());
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value::BOOLEAN(true));
}

//! duckpgq_csr_index_stats(table, index): 'epochs=..;delta=..;tombstones=..'
void CSRIndexStatsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto table_name = args.data[0].GetValue(0).GetValue<string>();
	auto index_name = args.data[1].GetValue(0).GetValue<string>();
	auto resolved = ResolveCSRIndex(context, table_name, index_name);
	auto handle = resolved.entry->GetReadHandle<CSRIndex>();
	auto stats = StringUtil::Format("epochs=%llu;delta=%llu;tombstones=%llu", handle->EpochCount(),
	                                handle->DeltaSize(), handle->TombstoneCount());
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value(stats));
}

} // namespace

void CoreScalarFunctions::RegisterCSRIndexScalarFunctions(ExtensionLoader &loader) {
	auto fold = ScalarFunction("duckpgq_csr_index_fold", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                           LogicalType::BOOLEAN, CSRIndexFoldFunction);
	fold.SetStability(FunctionStability::VOLATILE);
	fold.SetFallible();
	loader.RegisterFunction(fold);
	auto stats = ScalarFunction("duckpgq_csr_index_stats", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                            LogicalType::VARCHAR, CSRIndexStatsFunction);
	stats.SetStability(FunctionStability::VOLATILE);
	stats.SetFallible();
	loader.RegisterFunction(stats);
}

} // namespace duckdb
