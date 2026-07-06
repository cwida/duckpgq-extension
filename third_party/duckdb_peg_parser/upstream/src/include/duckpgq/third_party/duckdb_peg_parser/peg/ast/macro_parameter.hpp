#pragma once
#include "duckdb/parser/parsed_expression.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {
struct MacroParameter {
	unique_ptr<ParsedExpression> expression;
	Identifier name;
	LogicalType type = LogicalType::UNKNOWN;
	bool is_default = false;
};

} // namespace duckpgq_peg
} // namespace duckdb
