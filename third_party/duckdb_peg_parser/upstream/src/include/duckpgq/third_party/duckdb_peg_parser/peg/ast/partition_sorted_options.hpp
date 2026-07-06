#pragma once
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
namespace duckpgq_peg {
struct PartitionSortedOptions {
	vector<unique_ptr<ParsedExpression>> partition_keys;
	vector<unique_ptr<ParsedExpression>> sort_keys;
};
} // namespace duckpgq_peg
} // namespace duckdb
