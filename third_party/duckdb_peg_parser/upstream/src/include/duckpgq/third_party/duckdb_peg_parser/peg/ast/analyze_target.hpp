#pragma once
#include "duckdb/common/common.hpp"

#include "duckdb/common/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {
struct AnalyzeTarget {
	unique_ptr<TableRef> ref;
	vector<Identifier> columns;
};
} // namespace duckpgq_peg
} // namespace duckdb
