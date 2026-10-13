#pragma once
#include "duckpgq/common.hpp"

#include <duckpgq_state.hpp>

namespace duckdb {

struct CorePGQParser {
	static void Register(ExtensionLoader &loader) {
		RegisterPGQParserExtension(loader);
		RegisterPGQGrammarExtension(loader);
	}

private:
	static void RegisterPGQParserExtension(ExtensionLoader &loader);
	static void RegisterPGQGrammarExtension(ExtensionLoader &loader);
};

struct DuckPGQParserExtensionInfo : ParserExtensionInfo {
	DuckPGQParserExtensionInfo() : ParserExtensionInfo() {};
	~DuckPGQParserExtensionInfo() override = default;
};

ParserExtensionPlanResult duckpgq_plan(ParserExtensionInfo *info, ClientContext &,
                                       unique_ptr<ParserExtensionParseData>);

struct DuckPGQParserExtension : ParserExtension {
	DuckPGQParserExtension() : ParserExtension() {
		plan_function = duckpgq_plan;
		parser_info = make_shared_ptr<DuckPGQParserExtensionInfo>();
	}
};

struct DuckPGQParseData : ParserExtensionParseData {
	unique_ptr<SQLStatement> statement;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(statement->Copy());
	}

	string ToString() const override {
		return statement->ToString();
	};

	explicit DuckPGQParseData(unique_ptr<SQLStatement> statement) : statement(std::move(statement)) {
	}
};

} // namespace duckdb
