#pragma once
#include "duckpgq_state.hpp"

#include <duckdb/parser/path_element.hpp>

namespace duckpgq {

namespace core {

#define LANE_LIMIT 512
#define VISIT_SIZE_DIVISOR 2

// Function to get DuckPGQState from ClientContext
DuckPGQState *GetDuckPGQState(ClientContext &context);

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

} // namespace core

} // namespace duckpgq
