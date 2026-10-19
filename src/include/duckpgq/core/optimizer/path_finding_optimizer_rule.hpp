#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include <duckpgq/core/operator/logical_path_finding_operator.hpp>

namespace duckdb {

class DuckpgqOptimizerExtension : public OptimizerExtension {
public:
	DuckpgqOptimizerExtension() {
		// Markers emitted by MATCH are matched on the binder's plan shape, before DuckDB's optimizer reorders joins
		// and filters. The post-optimize pass remains for the legacy global-CSR form, whose CSR builder lives in a
		// CTE that is only inlined by the optimizer.
		pre_optimize_function = DuckpgqOptimizeFunction;
		optimize_function = DuckpgqOptimizeFunction;
	}

	static bool InsertPathFindingOperator(LogicalOperator &op, ClientContext &context);

	static void DuckpgqOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

	static unique_ptr<LogicalPathFindingOperator> FindCSRAndPairs(unique_ptr<LogicalOperator> &first_child,
	                                                              unique_ptr<LogicalOperator> &second_child,
	                                                              LogicalProjection &op_proj, ClientContext &context);
};

} // namespace duckdb
