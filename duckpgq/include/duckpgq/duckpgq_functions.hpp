//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlpgq_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

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
        //        functions.push_back(GetBidirectionalIterativeLengthFunction());
        functions.push_back(GetIterativeLengthBidirectionalFunction());
        functions.push_back(GetIterativeLength2Function());
        functions.push_back(GetDeleteCsrFunction());
        functions.push_back(GetGetCsrWTypeFunction());

        //		AddAliases({"to_json", "json_quote"}, GetToJSONFunction(), functions);
        //		functions.push_back(GetArrayToJSONFunction());
        //		functions.push_back(GetRowToJSONFunction());

        // Structure/Transform
        //		functions.push_back(GetStructureFunction());
        //		AddAliases({"json_transform", "from_json"}, GetTransformFunction(), functions);
        //		AddAliases({"json_transform_strict", "from_json_strict"}, GetTransformStrictFunction(), functions);

        // Other
        //		functions.push_back(GetArrayLengthFunction());
        //		functions.push_back(GetTypeFunction());
        //		functions.push_back(GetValidFunction());

        return functions;
    }

private:
    static CreateScalarFunctionInfo GetCsrVertexFunction();
    static CreateScalarFunctionInfo GetCsrEdgeFunction();
    static CreateScalarFunctionInfo GetCheapestPathLengthFunction();
    static CreateScalarFunctionInfo GetShortestPathFunction();
    static CreateScalarFunctionInfo GetReachabilityFunction();
    static CreateScalarFunctionInfo GetIterativeLengthFunction();
    //	static CreateScalarFunctionInfo GetStructureFunction();
    //
    //    static CreateScalarFunctionInfo GetBidirectionalIterativeLengthFunction();
    static CreateScalarFunctionInfo GetIterativeLengthBidirectionalFunction();
    static CreateScalarFunctionInfo GetIterativeLength2Function();
    static CreateScalarFunctionInfo GetDeleteCsrFunction();
    static CreateScalarFunctionInfo GetGetCsrWTypeFunction();
    //	static CreateScalarFunctionInfo GetTransformFunction();
    //	static CreateScalarFunctionInfo GetTransformStrictFunction();
    //
    //	static CreateScalarFunctionInfo GetArrayLengthFunction();
    //	static CreateScalarFunctionInfo GetTypeFunction();
    //	static CreateScalarFunctionInfo GetValidFunction();

    static void AddAliases(vector<string> names, CreateScalarFunctionInfo fun,
                           vector<CreateScalarFunctionInfo> &functions) {
        for (auto &name : names) {
            fun.name = name;
            functions.push_back(fun);
        }
    }
};

} // namespace duckdb
