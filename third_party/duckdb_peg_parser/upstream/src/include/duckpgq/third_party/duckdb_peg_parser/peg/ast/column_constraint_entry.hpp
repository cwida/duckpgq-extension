#pragma once

#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
namespace duckpgq_peg {

struct ColumnConstraintEntry {
	string constraint_name;
	pair<bool, ConstraintType> constraint_type_info;
	unique_ptr<ParsedExpression> expression;
	unique_ptr<Constraint> constraint;
	CompressionType compression_type;

	ColumnConstraintEntry()
	    : constraint_type_info(false, ConstraintType::INVALID), compression_type(CompressionType::COMPRESSION_AUTO) {
	}
};

} // namespace duckpgq_peg
} // namespace duckdb
