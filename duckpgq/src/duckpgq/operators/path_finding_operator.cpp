
#include "duckpgq/operators/path_finding_operator.hpp"
#include <duckpgq_extension.hpp>

namespace duckdb {
	unique_ptr<PhysicalOperator> PathFindingOperator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
		return unique_ptr<PhysicalOperator>(); // TODO IMPLEMENT ME
	}

}