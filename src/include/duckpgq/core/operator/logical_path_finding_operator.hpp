#pragma once
#include "duckpgq/common.hpp"
#include <duckdb/planner/operator/logical_extension_operator.hpp>

namespace duckdb {

class LogicalPathFindingOperator : public LogicalExtensionOperator {
public:
	explicit LogicalPathFindingOperator(vector<unique_ptr<LogicalOperator>> &children_,
	                                    vector<unique_ptr<Expression>> &expressions_, const string &mode_,
	                                    TableIndex table_index_, vector<idx_t> &offsets_, string cache_key_,
	                                    bool edge_input_, bool precounted_edge_input_ = false,
	                                    idx_t precounted_vertex_count_ = 0, idx_t precounted_edge_count_ = 0,
	                                    bool cached_partitioned_csr_input_ = false)
	    : LogicalExtensionOperator(std::move(expressions_)) {
		children = std::move(children_);
		mode = mode_;
		table_index = table_index_;
		offsets = offsets_;
		cache_key = std::move(cache_key_);
		edge_input = edge_input_;
		precounted_edge_input = precounted_edge_input_;
		precounted_vertex_count = precounted_vertex_count_;
		precounted_edge_count = precounted_edge_count_;
		cached_partitioned_csr_input = cached_partitioned_csr_input_;
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
	string mode;
	TableIndex table_index;
	vector<idx_t> offsets;
	string cache_key;
	bool edge_input;
	bool precounted_edge_input;
	idx_t precounted_vertex_count;
	idx_t precounted_edge_count;
	bool cached_partitioned_csr_input;
};
} // namespace duckdb
