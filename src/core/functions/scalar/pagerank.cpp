#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/functions/function_data/pagerank_function_data.hpp"
#include <duckpgq/core/functions/scalar.hpp>
#include <duckpgq/core/functions/table/pagerank.hpp>
#include <duckpgq/core/utils/duckpgq_bitmap.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckdb {

static void PageRankFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<PageRankFunctionData>();
	auto duckpgq_state = GetDuckPGQState(info.context);

	// Locate the CSR representation of the graph
	auto csr_entry = duckpgq_state->csr_list.find((uint64_t)info.csr_id);
	if (csr_entry == duckpgq_state->csr_list.end()) {
		throw ConstraintException("CSR not found. Is the graph populated?");
	}

	if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e)) {
		throw ConstraintException("Need to initialize CSR before running PageRank.");
	}

	int64_t *v = reinterpret_cast<int64_t *>(duckpgq_state->csr_list[info.csr_id]->v);
	vector<int64_t> &e = duckpgq_state->csr_list[info.csr_id]->e;
	size_t v_size = duckpgq_state->csr_list[info.csr_id]->vsize;

	// State initialization (only once)
	if (!info.state_initialized) {
		info.rank.resize(v_size, 1.0 / v_size); // Initial rank for each node
		info.temp_rank.resize(v_size,
		                      0.0);        // Temporary storage for ranks during iteration
		info.damping_factor = 0.85;        // Typical damping factor
		info.convergence_threshold = 1e-6; // Convergence threshold
		info.state_initialized = true;
		info.converged = false;
		info.iteration_count = 0;
	}

	// Check if already converged
	if (!info.converged) {
		std::lock_guard<std::mutex> guard(info.state_lock); // Thread safety

		bool continue_iteration = true;
		while (continue_iteration) {
			fill(info.temp_rank.begin(), info.temp_rank.end(), 0.0);

			double total_dangling_rank = 0.0; // For dangling nodes

			for (size_t i = 0; i < v_size; i++) {
				int64_t start_edge = v[i];
				int64_t end_edge = (i + 1 < v_size) ? v[i + 1] : e.size(); // Adjust end_edge
				if (end_edge > start_edge) {
					double rank_contrib = info.rank[i] / (end_edge - start_edge);
					for (int64_t j = start_edge; j < end_edge; j++) {
						int64_t neighbor = e[j];
						info.temp_rank[neighbor] += rank_contrib;
					}
				} else {
					total_dangling_rank += info.rank[i];
				}
			}

			// Apply damping factor and handle dangling node ranks
			double correction_factor = total_dangling_rank / v_size;
			double max_delta = 0.0;
			for (size_t i = 0; i < v_size; i++) {
				info.temp_rank[i] =
				    (1 - info.damping_factor) / v_size + info.damping_factor * (info.temp_rank[i] + correction_factor);
				max_delta = std::max(max_delta, std::abs(info.temp_rank[i] - info.rank[i]));
			}

			info.rank.swap(info.temp_rank);
			info.iteration_count++;
			if (max_delta < info.convergence_threshold) {
				info.converged = true;
				continue_iteration = false;
			}
		}
	}

	// Get the source vector for the current DataChunk
	auto &src = args.data[1];
	UnifiedVectorFormat vdata_src;
	src.ToUnifiedFormat(args.size(), vdata_src);
	auto src_data = (int64_t *)vdata_src.data;

	// Create result vector
	ValidityMask &result_validity = FlatVector::Validity(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<double_t>(result);

	// Output the PageRank value corresponding to each source ID in the DataChunk
	for (idx_t i = 0; i < args.size(); i++) {
		auto id_pos = vdata_src.sel->get_index(i);
		if (!vdata_src.validity.RowIsValid(id_pos)) {
			result_validity.SetInvalid(i);
			continue; // Skip invalid rows
		}
		auto node_id = src_data[id_pos];
		if (node_id < 0 || node_id >= (int64_t)v_size) {
			result_validity.SetInvalid(i);
			continue;
		}
		result_data[i] = info.rank[node_id];
	}

	duckpgq_state->csr_to_delete.insert(info.csr_id);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterPageRankScalarFunction(DatabaseInstance &db) {
	ExtensionUtil::RegisterFunction(db, ScalarFunction("pagerank", {LogicalType::INTEGER, LogicalType::BIGINT},
	                                                   LogicalType::DOUBLE, PageRankFunction,
	                                                   PageRankFunctionData::PageRankBind));
}

} // namespace duckdb
