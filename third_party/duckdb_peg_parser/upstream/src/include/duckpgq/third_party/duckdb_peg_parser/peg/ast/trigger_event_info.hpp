#pragma once
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/common/vector.hpp"

#include "duckpgq/parser/identifier.hpp"
namespace duckdb {
namespace duckpgq_peg {
struct TriggerEventInfo {
	TriggerEventType event_type;
	vector<Identifier> columns;
};
} // namespace duckpgq_peg
} // namespace duckdb
