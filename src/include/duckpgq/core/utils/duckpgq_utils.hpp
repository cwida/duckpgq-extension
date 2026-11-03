#pragma once
#include "duckpgq_state.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/common_table_expression_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

// MS-BFS lanes per wave: 256 (3 Nov; 96 B per vertex per working set, one 256-bit or per edge on an AVX2 build).
// Measured on one commit at SF100 and SF300 against 128 and 512: 256 is fastest on random pairs (11-21% ahead of 512 at
// SF300) and halves 512's peak memory; 128 is fastest on reach and leanest but 5-10% behind on pairs; 512 is slowest
// on the lane kernel (the sweep is memory bound, so bytes per vertex are the lever). A build may override it
// (cmake -DDUCKPGQ_LANE_LIMIT=128|512) for the lane-width microbenchmark
#ifndef LANE_LIMIT
#define LANE_LIMIT         256
#endif
#define VISIT_SIZE_DIVISOR 2
#define BUCKET_COUNT       256U
#define BUCKET_MASK        (BUCKET_COUNT - 1)

// Function to get DuckPGQState from ClientContext
shared_ptr<DuckPGQState> GetDuckPGQState(ClientContext &context, bool throw_error_not_found = false);
CreatePropertyGraphInfo *GetPropertyGraphInfo(const shared_ptr<DuckPGQState> &duckpgq_state, const string &pg_name);
shared_ptr<PropertyGraphTable> ValidateSourceNodeAndEdgeTable(CreatePropertyGraphInfo *pg_info,
                                                              const std::string &node_table,
                                                              const std::string &edge_table);
unique_ptr<SelectNode> CreateSelectNode(const shared_ptr<PropertyGraphTable> &edge_pg_entry,
                                        const string &function_name, const string &function_alias);
unique_ptr<TableRef> CreateTableFunctionSubquery(unique_ptr<SelectNode> select_node,
                                                 unique_ptr<CommonTableExpressionInfo> cte, const string &cte_name,
                                                 const string &alias);
unique_ptr<BaseTableRef> CreateBaseTableRef(const string &table_name, const string &alias = "");
unique_ptr<ColumnRefExpression> CreateColumnRefExpression(const string &column_name, const string &table_name = "",
                                                          const string &alias = "");

} // namespace duckdb
