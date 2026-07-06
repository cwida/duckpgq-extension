#pragma once
#include "duckdb/common/common.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {
namespace duckpgq_peg {
struct GeneratedColumnDefinition {
	unique_ptr<ParsedExpression> expr;
	bool virtual_column = false;
	bool default_column = false;
};

struct ConstraintColumnDefinition {
	ColumnDefinition column_definition;
	vector<pair<bool, ConstraintType>> constraint_types;
	vector<unique_ptr<Constraint>> constraints;
};
} // namespace duckpgq_peg
} // namespace duckdb
