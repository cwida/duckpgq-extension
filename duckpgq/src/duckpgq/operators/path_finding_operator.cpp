
#include "duckpgq/operators/path_finding_operator.hpp"
#include <duckpgq_extension.hpp>

namespace duckdb {
	unique_ptr<PhysicalOperator> PathFindingOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
		return unique_ptr<PhysicalOperator>(); // TODO IMPLEMENT ME
	}

	duckdb::unique_ptr<duckdb::PhysicalOperator> CreatePlan(duckdb::ClientContext &,
																												duckdb::PhysicalPlanGenerator &generator) override {
		auto result = duckdb::make_uniq_base<duckdb::PhysicalOperator, PhysicalPathFinding>(bridge_id, types,
																																											 estimated_cardinality);
		D_ASSERT(children.size() == 2);
		auto plan = generator.CreatePlan(std::move(children[0]));
		result->children.emplace_back(std::move(plan));
		return result;
	}

}