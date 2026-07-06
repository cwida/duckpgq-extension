#pragma once
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
namespace duckpgq_peg {

struct LimitPercentResult {
	bool is_percent = false;
	unique_ptr<ParsedExpression> expression;
};
} // namespace duckpgq_peg
} // namespace duckdb
