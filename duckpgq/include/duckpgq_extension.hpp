#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/simplified_token.hpp"
#include "duckdb/parser/parser_options.hpp"
#include "duckdb.hpp"

namespace duckdb {

class DuckpgqExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

struct DuckPGQParserExtensionInfo : public ParserExtensionInfo {
public:
    DuckPGQParserExtensionInfo() : ParserExtensionInfo() {};
    ~DuckPGQParserExtensionInfo() override = default;
};

ParserExtensionParseResult duckpgq_parse(ParserExtensionInfo *info,
                                         const std::string &query);

ParserExtensionPlanResult duckpgq_plan(ParserExtensionInfo *info, ClientContext &,
                                       unique_ptr<ParserExtensionParseData>);

struct DuckPGQParserExtension : public ParserExtension {
    DuckPGQParserExtension() : ParserExtension() {
        parse_function = duckpgq_parse;
        plan_function = duckpgq_plan;
        parser_info = make_shared<DuckPGQParserExtensionInfo>();
    }
};

class DuckPGQState : public ClientContextState {
public:
    explicit DuckPGQState(unique_ptr<ParserExtensionParseData> parse_data)
        : parse_data(std::move(parse_data)) {}
    void QueryEnd() override { parse_data.reset(); }
    unique_ptr<ParserExtensionParseData> parse_data;
};

} // namespace duckdb
