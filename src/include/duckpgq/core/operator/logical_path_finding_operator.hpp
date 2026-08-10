#pragma once
#include "duckpgq/common.hpp"
#include <duckdb/planner/operator/logical_extension_operator.hpp>

namespace duckdb {

class LogicalPathFindingOperator : public LogicalExtensionOperator {
public:
	explicit LogicalPathFindingOperator(vector<unique_ptr<LogicalOperator>> &children_,
	                                    vector<unique_ptr<Expression>> &expressions_, const string &mode_,
	                                    TableIndex table_index_, vector<idx_t> &offsets_, string cache_key_)
	    : LogicalExtensionOperator(std::move(expressions_)) {
		children = std::move(children_);
		mode = mode_;
		table_index = table_index_;
		offsets = offsets_;
		cache_key = std::move(cache_key_);
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

	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	string mode;
	TableIndex table_index;
	vector<idx_t> offsets;
	string cache_key;
};
} // namespace duckdb
