#pragma once
#include "duckdb/common/common.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {
struct TableAlias {
	Identifier name;
	vector<Identifier> column_name_alias;
};
} // namespace duckpgq_peg
} // namespace duckdb
