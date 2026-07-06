#pragma once
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
namespace duckpgq_peg {
struct DistinctClause {
	bool is_distinct;
	vector<unique_ptr<ParsedExpression>> distinct_targets;
};
} // namespace duckpgq_peg
} // namespace duckdb
