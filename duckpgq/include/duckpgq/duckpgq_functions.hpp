//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckpgq_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

#define LANE_LIMIT         512
#define VISIT_SIZE_DIVISOR 2

class DuckPGQFunctions {
public:
    static vector<CreateScalarFunctionInfo> GetFunctions() {
        vector<CreateScalarFunctionInfo> functions;

        // Create functions
        functions.push_back(GetCsrVertexFunction());
        functions.push_back(GetCsrEdgeFunction());
        functions.push_back(GetCheapestPathLengthFunction());
        functions.push_back(GetShortestPathFunction());
        functions.push_back(GetReachabilityFunction());
        functions.push_back(GetIterativeLengthFunction());
        functions.push_back(GetIterativeLengthBidirectionalFunction());
        functions.push_back(GetIterativeLength2Function());
        functions.push_back(GetDeleteCsrFunction());
        functions.push_back(GetGetCsrWTypeFunction());

        return functions;
    }

    static vector<CreateTableFunctionInfo> GetTableFunctions() {
        vector<CreateTableFunctionInfo> functions;

        functions.push_back(GetScanCSREFunction());
        functions.push_back(GetScanCSRWFunction());
        functions.push_back(GetScanCSRVFunction());
        functions.push_back(GetScanPGVTableFunction());
        functions.push_back(GetScanPGVColFunction());
        functions.push_back(GetScanPGETableFunction());
        functions.push_back(GetScanPGEColFunction());

        return functions;
    }

private:
    static CreateScalarFunctionInfo GetCsrVertexFunction();
    static CreateScalarFunctionInfo GetCsrEdgeFunction();
    static CreateScalarFunctionInfo GetCheapestPathLengthFunction();
    static CreateScalarFunctionInfo GetShortestPathFunction();
    static CreateScalarFunctionInfo GetReachabilityFunction();
    static CreateScalarFunctionInfo GetIterativeLengthFunction();
    static CreateScalarFunctionInfo GetIterativeLengthBidirectionalFunction();
    static CreateScalarFunctionInfo GetIterativeLength2Function();
    static CreateScalarFunctionInfo GetDeleteCsrFunction();
    static CreateScalarFunctionInfo GetGetCsrWTypeFunction();

    static void AddAliases(vector<string> names, CreateScalarFunctionInfo fun,
                           vector<CreateScalarFunctionInfo> &functions) {
        for (auto &name : names) {
            fun.name = name;
            functions.push_back(fun);
        }
    }

    // table functions
    static CreateTableFunctionInfo GetScanCSRVFunction();
    static CreateTableFunctionInfo GetScanCSREFunction();
    static CreateTableFunctionInfo GetScanCSRWFunction();
    static CreateTableFunctionInfo GetScanPGVTableFunction();
    static CreateTableFunctionInfo GetScanPGVColFunction();
    static CreateTableFunctionInfo GetScanPGETableFunction();
    static CreateTableFunctionInfo GetScanPGEColFunction();
};

} // namespace duckdb
