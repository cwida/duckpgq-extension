#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"

#include "duckpgq/parser/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {

struct AddColumnEntry {
	LogicalType type;
	vector<Identifier> column_path;
	unique_ptr<ParsedExpression> default_value;
};

} // namespace duckpgq_peg
} // namespace duckdb
