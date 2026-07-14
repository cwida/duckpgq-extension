#pragma once

#include "duckpgq/parser/identifier.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {
namespace duckpgq_peg {

struct DescribeTarget {
	bool is_table_name = false;
	Identifier table_name;
	unique_ptr<BaseTableRef> table_ref;
};

} // namespace duckpgq_peg
} // namespace duckdb
