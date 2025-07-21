#pragma once
#include "duckpgq_state.hpp"

#include <duckdb/parser/path_element.hpp>

namespace duckpgq {

namespace core {

#define LANE_LIMIT 256U
#define VISIT_SIZE_DIVISOR 2
#define BUCKET_COUNT 256u
#define BUCKET_MASK (BUCKET_COUNT - 1)


class GraphUtils {
public:
  static std::string ToString(PGQMatchType matchType) {
    switch (matchType) {
    case PGQMatchType::MATCH_VERTEX:
      return "MATCH_VERTEX";
    case PGQMatchType::MATCH_EDGE_ANY:
      return "MATCH_EDGE_ANY";
    case PGQMatchType::MATCH_EDGE_LEFT:
      return "MATCH_EDGE_LEFT";
    case PGQMatchType::MATCH_EDGE_RIGHT:
      return "MATCH_EDGE_RIGHT";
    case PGQMatchType::MATCH_EDGE_LEFT_RIGHT:
      return "MATCH_EDGE_LEFT_RIGHT";
    default:
      return "UNKNOWN_MATCH_TYPE";
    }
  }
};

shared_ptr<DuckPGQState> GetDuckPGQState(ClientContext &context);
CreatePropertyGraphInfo *
GetPropertyGraphInfo(const shared_ptr<DuckPGQState> &duckpgq_state,
                     const string &pg_name);
shared_ptr<PropertyGraphTable>
ValidateSourceNodeAndEdgeTable(CreatePropertyGraphInfo *pg_info,
                               const std::string &node_table,
                               const std::string &edge_table);
unique_ptr<SelectNode>
CreateSelectNode(const shared_ptr<PropertyGraphTable> &edge_pg_entry,
                 const string &function_name, const string &function_alias);
unique_ptr<BaseTableRef> CreateBaseTableRef(const string &table_name,
                                            const string &alias = "");
unique_ptr<ColumnRefExpression>
CreateColumnRefExpression(const string &column_name,
                          const string &table_name = "",
                          const string &alias = "");
} // namespace core

} // namespace duckpgq
