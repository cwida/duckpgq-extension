#pragma once
#include "duckdb/parser/query_node/select_node.hpp"

namespace duckdb {
namespace duckpgq_peg {

struct CreatePivotEntry {
	string enum_name;
	unique_ptr<SelectNode> base;
	unique_ptr<ParsedExpression> column;
	unique_ptr<QueryNode> subquery;
	bool has_parameters;
};

} // namespace duckpgq_peg
} // namespace duckdb
