#pragma once
#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/path_finding_input.hpp"
#include "duckpgq/core/operator/path_finding_mode.hpp"
#include <duckdb/planner/operator/logical_extension_operator.hpp>

namespace duckdb {

//! The path-finding operator in the logical plan: the pairs (children[0]) and the edge input of the route, with the
//! marker function as its only expression (the source and destination arguments are evaluated over the pairs)
class LogicalPathFindingOperator : public LogicalExtensionOperator {
public:
	LogicalPathFindingOperator(vector<unique_ptr<LogicalOperator>> children_p, unique_ptr<Expression> marker,
	                           PathFindingOperatorMode mode_p, TableIndex table_index_p, idx_t result_offset_p,
	                           PathFindingInput input_p)
	    : mode(mode_p), table_index(table_index_p), result_offset(result_offset_p), input(std::move(input_p)) {
		children = std::move(children_p);
		expressions.push_back(std::move(marker));
	}

	void Serialize(Serializer &serializer) const override {
		throw InternalException("Path Finding Operator should not be serialized");
	}

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override;

	vector<ColumnBinding> GetColumnBindings() override;

	std::string GetName() const override {
		return "PATH_FINDING";
	}

	void ResolveTypes() override;
	void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) override;

	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	PathFindingOperatorMode mode;
	//! The replaced projection's table index and the SELECT-list offset of the marker: the result column's binding
	TableIndex table_index;
	idx_t result_offset;
	PathFindingInput input;
};
} // namespace duckdb
