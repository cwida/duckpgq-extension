#pragma once
#include "duckdb/common/string.hpp"

#include "duckpgq/parser/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {
struct ExtensionRepositoryInfo {
	Identifier name;
	bool repository_is_alias = false;
};
} // namespace duckpgq_peg
} // namespace duckdb
