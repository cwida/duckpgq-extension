#pragma once
#include "duckdb/common/string.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {
struct TriggerTableReferencingInfo {
	Identifier new_table;
	Identifier old_table;
};
} // namespace duckpgq_peg
} // namespace duckdb
