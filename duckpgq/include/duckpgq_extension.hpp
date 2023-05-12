#pragma once

#include "duckdb.hpp"

namespace duckdb {

class DuckpgqExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

ParserExtensionParseResult duckpgq_parse(ParserExtensionInfo *,
                                         const std::string &query);

ParserExtensionPlanResult duckpgq_plan(ParserExtensionInfo *, ClientContext &,
                                       unique_ptr<ParserExtensionParseData>);

struct DuckPGQParserExtension : public ParserExtension {
    DuckPGQParserExtension() : ParserExtension() {
        parse_function = duckpgq_parse;
        plan_function = duckpgq_plan;
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
