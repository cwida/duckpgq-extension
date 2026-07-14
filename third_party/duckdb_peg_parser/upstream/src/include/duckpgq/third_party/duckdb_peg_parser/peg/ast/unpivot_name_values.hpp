#pragma once
#include "duckdb/parser/tableref/pivotref.hpp"

#include "duckpgq/parser/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {
struct UnpivotNameValues {
	vector<Identifier> unpivot_names;
	PivotColumn column;
};
} // namespace duckpgq_peg
} // namespace duckdb
