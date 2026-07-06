#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
namespace duckpgq_peg {

struct TrimArguments {
	optional<string> trim_direction;
	vector<unique_ptr<ParsedExpression>> expressions;
};

} // namespace duckpgq_peg
} // namespace duckdb
