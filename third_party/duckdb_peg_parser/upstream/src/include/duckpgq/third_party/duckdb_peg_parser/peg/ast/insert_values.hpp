#pragma once
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {
namespace duckpgq_peg {
struct InsertValues {
	bool default_values = false;
	unique_ptr<SelectStatement> select_statement;
};
} // namespace duckpgq_peg
} // namespace duckdb
