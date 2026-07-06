#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
namespace duckpgq_peg {

struct CastArguments {
	unique_ptr<ParsedExpression> expression;
	LogicalType type;
};

} // namespace duckpgq_peg
} // namespace duckdb
