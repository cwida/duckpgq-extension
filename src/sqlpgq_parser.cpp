#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "sqlpgq_parser.hpp"
#include "../duckdb-pgq/third_party/libpg_query/include/postgres_parser.hpp"

namespace duckdb {

SQLPGQFunction::SQLPGQFunction() {
    name = "duckpgq";
    arguments.push_back(LogicalType::BIGINT);
    bind = SQLPGQBind;
    init_global = SQLPGQInit;
    function = SQLPGQFunc;
}

unique_ptr<FunctionData> SQLPGQFunction::SQLPGQBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    names.emplace_back("duckpgq");
    return_types.emplace_back(LogicalType::VARCHAR);
    return make_unique<SQLPGQBindData>(BigIntValue::Get(input.inputs[0]));
}

unique_ptr<GlobalTableFunctionState> SQLPGQFunction::SQLPGQInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_unique<SQLPGQGlobalData>();
}

void SQLPGQFunction::SQLPGQFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = (SQLPGQBindData &)*data_p.bind_data;
    auto &data = (SQLPGQGlobalData &)*data_p.global_state;
    if (data.offset >= bind_data.number_of_elements) {
        // finished returning values
        return;
    }
    // start returning values
    // either fill up the chunk or return all the remaining columns
    idx_t count = 0;
    while (data.offset < bind_data.number_of_elements && count < STANDARD_VECTOR_SIZE) {
        output.SetValue(0, count, Value("QUACK"));
        data.offset++;
        count++;
    }
    output.SetCardinality(count);
}


SQLPGQParserExtension::SQLPGQParserExtension() {
    parse_function = SQLPGQParseFunction;
    plan_function = SQLPGQPlanFunction;
}

ParserExtensionParseResult SQLPGQParserExtension::SQLPGQParseFunction(ParserExtensionInfo *info, const string &query) {
    PostgresParser parser;
    parser.Parse(query);
    if (parser.success) {
        if (!parser.parse_tree) {
            // empty statement
            return {parser.error_message};
        }
        // successful and non-empty so handle the query result here
        return {make_unique<SQLPGQExtensionData>(0)};
    } else {
        return { parser.error_message};
    }

}

ParserExtensionPlanResult SQLPGQParserExtension::SQLPGQPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                    unique_ptr<ParserExtensionParseData> parse_data) {
    auto &sqlpgq_data = (SQLPGQExtensionData &)*parse_data;

    ParserExtensionPlanResult result;
    result.function = SQLPGQFunction();
    result.parameters.push_back(Value::BIGINT(sqlpgq_data.number_of_elements));
    result.requires_valid_transaction = false;
    result.return_type = StatementReturnType::QUERY_RESULT;
    return result;
}
}
