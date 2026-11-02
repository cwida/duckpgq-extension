#include "duckpgq/core/operator/path_finding_mode.hpp"

#include "duckpgq/core/functions/scalar.hpp"

namespace duckdb {

namespace {

struct PathFindingModeEntry {
	const char *marker_name;
	const char *mode_name;
	PathFindingOperatorMode mode;
	LogicalType result_type;
};

//! One row per marker function; the table is the only place that ties a marker name to a mode and a result type
const vector<PathFindingModeEntry> &PathFindingModes() {
	static const vector<PathFindingModeEntry> modes = {
	    {"iterativelengthoperator", "iterativelength", PathFindingOperatorMode::ITERATIVE_LENGTH, LogicalType::BIGINT},
	    {"pushpulliterativelengthoperator", "pushpulliterativelength",
	     PathFindingOperatorMode::PUSH_PULL_ITERATIVE_LENGTH, LogicalType::BIGINT},
	    {"bidirectionaliterativelengthoperator", "bidirectionaliterativelength",
	     PathFindingOperatorMode::BIDIRECTIONAL_ITERATIVE_LENGTH, LogicalType::BIGINT},
	    {"shortestpathoperator", "shortestpath", PathFindingOperatorMode::SHORTEST_PATH,
	     LogicalType::LIST(LogicalType::BIGINT)},
	    {"reachoperator", "reach", PathFindingOperatorMode::REACH, ReachOperatorResultType()},
	};
	return modes;
}

const PathFindingModeEntry *FindMarker(const string &function_name) {
	for (auto &entry : PathFindingModes()) {
		if (function_name == entry.marker_name) {
			return &entry;
		}
	}
	return nullptr;
}

const PathFindingModeEntry &Entry(PathFindingOperatorMode mode) {
	for (auto &entry : PathFindingModes()) {
		if (entry.mode == mode) {
			return entry;
		}
	}
	throw InternalException("Unhandled path-finding operator mode");
}

} // namespace

bool IsPathFindingMarkerName(const string &function_name) {
	return FindMarker(function_name) != nullptr;
}

PathFindingOperatorMode PathFindingModeFromMarkerName(const string &function_name) {
	auto entry = FindMarker(function_name);
	if (!entry) {
		throw InternalException("%s is not a path-finding marker function", function_name);
	}
	return entry->mode;
}

const char *PathFindingModeName(PathFindingOperatorMode mode) {
	return Entry(mode).mode_name;
}

LogicalType PathFindingResultType(PathFindingOperatorMode mode) {
	return Entry(mode).result_type;
}

} // namespace duckdb
