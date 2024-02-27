#pragma once
#include <duckdb/planner/operator/logical_extension_operator.hpp>

namespace duckdb {

	class LogicalPathFindingOperator : public LogicalExtensionOperator {
	public:
		explicit LogicalPathFindingOperator(unique_ptr<LogicalOperator> plan) {
			children.emplace_back(std::move(plan));
		}

		void Serialize(Serializer &serializer) const override {
			throw InternalException("Path Finding Operator should not be serialized");
		}

		unique_ptr<PhysicalOperator> CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override;

		vector<ColumnBinding> GetColumnBindings() override;

		std::string GetName() const override {
			return "PATH_FINDING";
		}
	};


}
