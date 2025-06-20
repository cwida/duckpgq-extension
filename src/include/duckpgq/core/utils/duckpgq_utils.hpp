#pragma once
#include "duckpgq_state.hpp"

namespace duckpgq {

namespace core {

#define LANE_LIMIT 512
#define VISIT_SIZE_DIVISOR 2

// Function to get DuckPGQState from ClientContext
shared_ptr<DuckPGQState> GetDuckPGQState(ClientContext &context, bool throw_error_not_found = false);
CreatePropertyGraphInfo *GetPropertyGraphInfo(const shared_ptr<DuckPGQState> &duckpgq_state,
                     const string &pg_name);
shared_ptr<PropertyGraphTable> ValidateSourceNodeAndEdgeTable(CreatePropertyGraphInfo *pg_info,
                               const std::string &node_table,
                               const std::string &edge_table);
unique_ptr<SelectNode> CreateSelectNode(const shared_ptr<PropertyGraphTable> &edge_pg_entry,
                 const string &function_name, const string &function_alias);
unique_ptr<BaseTableRef> CreateBaseTableRef(const string &table_name,
                                            const string &alias = "");
unique_ptr<ColumnRefExpression>
CreateColumnRefExpression(const string &column_name,
                          const string &table_name = "",
                          const string &alias = "");
} // namespace core

} // namespace duckpgq
