#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/grammar_change.hpp"
#include "duckpgq/parser/tableref/matchref.hpp"

namespace duckdb {

//! Carries a parsed GRAPH_TABLE clause from the grammar transform to the duckpgq_match bind-replace through
//! TableFunctionRef::bind_info. The pattern therefore never has to be bound as a scalar function argument, and it
//! survives statement copies made for prepared statements and rebinds.
struct DuckPGQMatchInfo : TableFunctionInfo {
	explicit DuckPGQMatchInfo(unique_ptr<MatchExpression> match_p) : match(std::move(match_p)) {
	}

	unique_ptr<MatchExpression> match;
};

//! Rules and transforms for CREATE PROPERTY GRAPH.
void DuckPGQAddCreatePropertyGraphGrammar(vector<GrammarChange> &changes);
//! Rules and transforms for GRAPH_TABLE path patterns.
void DuckPGQAddGraphTableGrammar(vector<GrammarChange> &changes);

} // namespace duckdb
