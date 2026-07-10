#pragma once

#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/common/string.hpp"

#include "duckpgq/parser/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {

struct SettingInfo {
	Identifier name;
	SetScope scope = SetScope::AUTOMATIC; // Default value is defined here
};

} // namespace duckpgq_peg
} // namespace duckdb
