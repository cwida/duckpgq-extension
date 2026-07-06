#pragma once

#include "duckpgq/third_party/duckdb_peg_parser/peg/ast/generated_column_definition.hpp"

namespace duckdb {
namespace duckpgq_peg {
struct CreateTableColumnElement {
	unique_ptr<ConstraintColumnDefinition> column_definition;
	unique_ptr<Constraint> constraint;
};
} // namespace duckpgq_peg
} // namespace duckdb
