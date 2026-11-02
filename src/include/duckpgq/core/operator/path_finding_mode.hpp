#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

//! What the path-finding operator computes: the marker function the optimizer rule replaced decides it
enum class PathFindingOperatorMode {
	ITERATIVE_LENGTH,
	PUSH_PULL_ITERATIVE_LENGTH,
	BIDIRECTIONAL_ITERATIVE_LENGTH,
	SHORTEST_PATH,
	//! One row per vertex reachable from each source (dst, len), no destination input
	REACH
};

//! Whether `function_name` is one of the planning markers (iterativelengthoperator, shortestpathoperator, ...)
bool IsPathFindingMarkerName(const string &function_name);
//! The mode a marker function stands for; throws for a name that is not a marker
PathFindingOperatorMode PathFindingModeFromMarkerName(const string &function_name);
//! The mode's name as EXPLAIN and duckpgq_path_finding_strategy() print it (iterativelength, shortestpath, reach, ...)
const char *PathFindingModeName(PathFindingOperatorMode mode);
//! The type of the marker's result column: BIGINT lengths, a BIGINT list for paths and reach rows
LogicalType PathFindingResultType(PathFindingOperatorMode mode);

} // namespace duckdb
