#pragma once
#include "duckdb/common/enums/joinref_type.hpp"

namespace duckdb {
namespace duckpgq_peg {
struct KeyActions {
	string update_action;
	string delete_action;
};
} // namespace duckpgq_peg
} // namespace duckdb
