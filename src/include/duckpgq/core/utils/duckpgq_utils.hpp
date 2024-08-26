#pragma once
#include "duckpgq_state.hpp"

namespace duckpgq {

namespace core {

#define LANE_LIMIT 512
#define VISIT_SIZE_DIVISOR 2

// Function to get DuckPGQState from ClientContext
DuckPGQState *GetDuckPGQState(ClientContext &context);
std::string ToLowerCase(const std::string &input);
CreatePropertyGraphInfo* GetPropertyGraphInfo(DuckPGQState *duckpgq_state, const std::string &pg_name);
shared_ptr<PropertyGraphTable> ValidateSourceNodeAndEdgeTable(CreatePropertyGraphInfo *pg_info, const std::string &node_table, const std::string &edge_table);
unique_ptr<SelectNode> CreateSelectNode(const shared_ptr<PropertyGraphTable> &edge_pg_entry, const string& function_name, const string& function_alias);

} // namespace core

} // namespace duckpgq
