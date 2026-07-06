#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckpgq/third_party/duckdb_peg_parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {
namespace duckpgq_peg {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformDeallocateStatement(PEGTransformer &transformer,
                                                                             const optional<bool> &deallocate_prepare,
                                                                             const Identifier &identifier) {
	auto result = make_uniq<DropStatement>();
	result->info->type = CatalogType::PREPARED_STATEMENT;
	result->info->SetName(identifier);
	return std::move(result);
}

bool PEGTransformerFactory::TransformDeallocatePrepare(PEGTransformer &transformer) {
	return true;
}

} // namespace duckpgq_peg
} // namespace duckdb
