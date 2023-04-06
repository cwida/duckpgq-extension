#pragma once

namespace duckdb {

class SQLPGQFunction : public TableFunction {
public:
    SQLPGQFunction();
    static unique_ptr<GlobalTableFunctionState> SQLPGQInit(ClientContext &context, TableFunctionInitInput &input);

    static unique_ptr<FunctionData> SQLPGQBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names);

    static void SQLPGQFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    struct SQLPGQBindData : public TableFunctionData {
        explicit SQLPGQBindData(idx_t number_of_elements) : number_of_elements(number_of_elements) {
        }

        idx_t number_of_elements;
    };

    struct SQLPGQGlobalData : public GlobalTableFunctionState {
        SQLPGQGlobalData() : offset(0) {
        }

        idx_t offset;
    };
};

struct SQLPGQExtensionData : public ParserExtensionParseData {
    SQLPGQExtensionData(idx_t number_of_elements) : number_of_elements(number_of_elements) {
    }

    idx_t number_of_elements;

    unique_ptr<ParserExtensionParseData> Copy() const override {
        return make_unique<SQLPGQExtensionData>(number_of_elements);
    }
};


class SQLPGQParserExtension : public ParserExtension {
public:
    SQLPGQParserExtension();

    static ParserExtensionParseResult SQLPGQParseFunction(ParserExtensionInfo *info, const string &query);

    static ParserExtensionPlanResult SQLPGQPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                    unique_ptr<ParserExtensionParseData> parse_data);
};


}