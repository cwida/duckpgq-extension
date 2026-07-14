#pragma once

#include "duckdb/parser/parsed_expression.hpp"
#include <optional>

namespace duckdb {
using std::optional;
namespace duckpgq_peg {

struct TrimArguments {
	optional<string> trim_direction;
	vector<unique_ptr<ParsedExpression>> expressions;
};

} // namespace duckpgq_peg
} // namespace duckdb
