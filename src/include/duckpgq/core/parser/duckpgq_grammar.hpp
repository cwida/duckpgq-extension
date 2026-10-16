#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/grammar_change.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"
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
//! Rules and transforms for DROP, DESCRIBE and SUMMARIZE PROPERTY GRAPH.
void DuckPGQAddGraphManagementGrammar(vector<GrammarChange> &changes);

//! A rule whose body is a single choice parses to a list holding that choice; return the chosen alternative.
ParseResult &DuckPGQGetChoice(ParseResult &parse_result);
//! Upper-cased concatenation of the keyword/operator tokens below a parse result, e.g. "<-" for '<' '-'.
string DuckPGQLiteralText(ParseResult &parse_result);

} // namespace duckdb
