#pragma once
#include "duckdb/parser/parsed_expression.hpp"

#include "duckpgq/parser/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {
struct JoinQualifier {
	unique_ptr<ParsedExpression> on_clause;
	vector<Identifier> using_columns;
};

} // namespace duckpgq_peg
} // namespace duckdb
