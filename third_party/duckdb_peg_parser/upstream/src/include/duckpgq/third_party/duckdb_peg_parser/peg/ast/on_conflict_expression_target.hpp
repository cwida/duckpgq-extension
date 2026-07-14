#pragma once

#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

#include "duckpgq/parser/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {

struct OnConflictExpressionTarget {
	vector<Identifier> indexed_columns;
	unique_ptr<ParsedExpression> where_clause; // Default value is defined here
};

} // namespace duckpgq_peg
} // namespace duckdb
