#pragma once
#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

struct CorePGQParser {
  static void Register(DatabaseInstance &db) {
    RegisterPGQParserExtension(db);
  }

private:
  static void RegisterPGQParserExtension(DatabaseInstance &db);
};

struct DuckPGQParserExtensionInfo : ParserExtensionInfo {
  DuckPGQParserExtensionInfo() : ParserExtensionInfo(){};
  ~DuckPGQParserExtensionInfo() override = default;
};

ParserExtensionParseResult duckpgq_parse(ParserExtensionInfo *info,
                                         const std::string &query);

ParserExtensionPlanResult duckpgq_plan(ParserExtensionInfo *info,
                                       ClientContext &,
                                       unique_ptr<ParserExtensionParseData>);

ParserExtensionPlanResult
duckpgq_handle_statement(unique_ptr<SQLStatement> &statement);

struct DuckPGQParserExtension : ParserExtension {
  DuckPGQParserExtension() : ParserExtension() {
    parse_function = duckpgq_parse;
    plan_function = duckpgq_plan;
    parser_info = make_shared_ptr<DuckPGQParserExtensionInfo>();
  }
};

struct DuckPGQParseData : ParserExtensionParseData {
  unique_ptr<SQLStatement> statement;

  unique_ptr<ParserExtensionParseData> Copy() const override {
    return make_uniq_base<ParserExtensionParseData, DuckPGQParseData>(
        statement->Copy());
  }

  string ToString() const override { return statement->ToString(); };

  explicit DuckPGQParseData(unique_ptr<SQLStatement> statement)
      : statement(std::move(statement)) {}
};


} // namespace core

} // namespace duckpgq