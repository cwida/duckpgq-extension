//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/tablefunctions/drop_property_graph.hpp
//
//
//===----------------------------------------------------------------------===//


#pragma once
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"

namespace duckdb {

    class DropPropertyGraphFunction : public TableFunction {
    public:
        DropPropertyGraphFunction() {
            name = "drop_property_graph";
            bind = DropPropertyGraphBind;
            init_global = DropPropertyGraphInit;
            function = DropPropertyGraphFunc;
        }

        struct DropPropertyGraphBindData : public TableFunctionData {
            explicit DropPropertyGraphBindData(DropInfo* pg_info) : drop_pg_info(pg_info) {
            }

            DropInfo* drop_pg_info;
        };

        struct DropPropertyGraphGlobalData : public GlobalTableFunctionState {
            DropPropertyGraphGlobalData() = default;
        };

        static duckdb::unique_ptr<FunctionData> DropPropertyGraphBind(ClientContext &context, TableFunctionBindInput &input,
                                                                      vector<LogicalType> &return_types, vector<string> &names);


        static duckdb::unique_ptr<GlobalTableFunctionState> DropPropertyGraphInit(ClientContext &context,
                                                                                  TableFunctionInitInput &input);

        static void DropPropertyGraphFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);
    };


}
