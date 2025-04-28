//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/functions/tablefunctions/summarize_property_graph.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckpgq/common.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckpgq {

namespace core {

class SummarizePropertyGraphFunction : public TableFunction {
public:
  SummarizePropertyGraphFunction() {
    name = "summarize_property_graph";
    bind = SummarizePropertyGraphBind;
    init_global = SummarizePropertyGraphInit;
    function = SummarizePropertyGraphFunc;
  }

  struct SummarizePropertyGraphBindData : public TableFunctionData {
    explicit SummarizePropertyGraphBindData(CreatePropertyGraphInfo *pg_info)
        : summarize_pg_info(pg_info) {}
    CreatePropertyGraphInfo *summarize_pg_info;
  };

  struct SummarizePropertyGraphGlobalData : public GlobalTableFunctionState {
    SummarizePropertyGraphGlobalData() = default;
    bool done = false;
  };

  static unique_ptr<FunctionData> SummarizePropertyGraphBind(
      ClientContext &context, TableFunctionBindInput &input,
      vector<LogicalType> &return_types, vector<string> &names);

  static unique_ptr<GlobalTableFunctionState>
  SummarizePropertyGraphInit(ClientContext &context,
                            TableFunctionInitInput &input);

  static void SummarizePropertyGraphFunc(ClientContext &context,
                                        TableFunctionInput &data_p,
                                        DataChunk &output);
};

} // namespace core

} // namespace duckpgq