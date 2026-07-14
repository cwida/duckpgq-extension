#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"
#include <cmath>
#include <duckpgq/core/functions/scalar.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <duckpgq_extension.hpp>
#include <mutex>

namespace duckdb {

static void CsrInitializeVertex(DuckPGQState &context, int32_t id, int64_t v_size) {
	lock_guard<mutex> csr_init_lock(context.csr_lock);

	auto csr_entry = context.csr_list.find(id);
	if (csr_entry != context.csr_list.end()) {
		if (csr_entry->second->initialized_v) {
			return;
		}
	}
	try {
		auto csr = make_uniq<CSR>();
		// extra 2 spaces required for CSR padding
		// data contains a vector of elements so will need an anonymous function to
		// apply the first element id is repeated across, can I access the value
		// directly?
		csr->v = new std::atomic<int64_t>[v_size + 2];
		csr->vsize = v_size + 2;

		for (idx_t i = 0; i < (idx_t)v_size + 2; i++) {
			csr->v[i] = 0;
		}
		csr->initialized_v = true;
		context.csr_list[id] = std::move(csr);
	} catch (std::bad_alloc const &) {
		throw Exception(ExceptionType::INTERNAL, "Unable to initialize vector of size for csr vertex table "
		                                         "representation");
	}
}

static void CsrInitializeEdge(DuckPGQState &context, int32_t id, int64_t v_size, int64_t e_size) {
	const lock_guard<mutex> csr_init_lock(context.csr_lock);

	auto csr_entry = context.csr_list.find(id);
	if (csr_entry->second->initialized_e) {
		return;
	}
	try {
		csr_entry->second->e.resize(e_size, 0);
		csr_entry->second->edge_ids.resize(e_size, 0);
	} catch (std::bad_alloc const &) {
		throw Exception(ExceptionType::INTERNAL, "Unable to initialize vector of size for csr edge table "
		                                         "representation");
	}
	for (auto i = 1; i < v_size + 2; i++) {
		csr_entry->second->v[i] += csr_entry->second->v[i - 1];
	}
	csr_entry->second->initialized_e = true;
}

static void CsrInitializeWeight(DuckPGQState &context, int32_t id, int64_t e_size, PhysicalType weight_type) {
	const lock_guard<mutex> csr_init_lock(context.csr_lock);
	auto csr_entry = context.csr_list.find(id);

	if (csr_entry->second->initialized_w) {
		return;
	}
	try {
		if (weight_type == PhysicalType::INT64) {
			csr_entry->second->w.resize(e_size, 0);
		} else if (weight_type == PhysicalType::DOUBLE) {
			csr_entry->second->w_double.resize(e_size, 0);
		} else {
			throw NotImplementedException("Unrecognized weight type detected.");
		}
	} catch (std::bad_alloc const &) {
		throw Exception(ExceptionType::INTERNAL, "Unable to initialize vector of size for csr weight table "
		                                         "representation");
	}

	csr_entry->second->initialized_w = true;
}

static void CreateCsrVertexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.BindInfo()->Cast<CSRFunctionData>();

	auto duckpgq_state = GetDuckPGQState(info.context);
	int64_t input_size = args.data[1].GetValue(0).GetValue<int64_t>();
	auto csr_entry = duckpgq_state->csr_list.find(info.id);

	if (csr_entry == duckpgq_state->csr_list.end()) {
		CsrInitializeVertex(*duckpgq_state, info.id, input_size);
		csr_entry = duckpgq_state->csr_list.find(info.id);
	} else {
		if (!csr_entry->second->initialized_v) {
			CsrInitializeVertex(*duckpgq_state, info.id, input_size);
		}
	}

	BinaryExecutor::Execute<int64_t, int64_t, int64_t>(args.data[2], args.data[3], result, args.size(),
	                                                   [&](int64_t src, int64_t cnt) {
		                                                   int64_t edge_count = 0;
		                                                   csr_entry->second->v[src + 2] = cnt;
		                                                   edge_count = edge_count + cnt;
		                                                   return edge_count;
	                                                   });
}

static void CreateCsrEdgeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.BindInfo()->Cast<CSRFunctionData>();

	auto duckpgq_state = GetDuckPGQState(info.context, true);

	int64_t vertex_size = args.data[1].GetValue(0).GetValue<int64_t>();
	int64_t edge_size = args.data[2].GetValue(0).GetValue<int64_t>();
	int64_t edge_size_count = args.data[3].GetValue(0).GetValue<int64_t>();
	if (edge_size != edge_size_count) {
		duckpgq_state->csr_to_delete.insert(info.id);
		throw ConstraintException("Non-existent/non-unique vertices detected. Make sure all "
		                          "vertices referred by edge tables exist and are unique for path-finding queries.");
	}

	auto csr_entry = duckpgq_state->csr_list.find(info.id);
	if (!csr_entry->second->initialized_e) {
		CsrInitializeEdge(*duckpgq_state, info.id, vertex_size, edge_size);
	}
	if (info.weight_type == LogicalType::SQLNULL) {
		TernaryExecutor::Execute<int64_t, int64_t, int64_t, int32_t>(
		    args.data[4], args.data[5], args.data[6], result,
		    [&](int64_t src, int64_t dst, int64_t edge_id) {
			    auto pos = ++csr_entry->second->v[src + 1];
			    csr_entry->second->e[(int64_t)pos - 1] = dst;
			    csr_entry->second->edge_ids[(int64_t)pos - 1] = edge_id;
			    return 1;
		    });
		return;
	}
	auto weight_type = args.data[7].GetType().InternalType();
	if (!csr_entry->second->initialized_w) {
		CsrInitializeWeight(*duckpgq_state, info.id, edge_size, weight_type);
	}
	UnifiedVectorFormat src_data, dst_data, edge_id_data, weight_data;
	args.data[4].ToUnifiedFormat(src_data);
	args.data[5].ToUnifiedFormat(dst_data);
	args.data[6].ToUnifiedFormat(edge_id_data);
	args.data[7].ToUnifiedFormat(weight_data);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<int32_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);
	auto src_values = reinterpret_cast<const int64_t *>(src_data.data);
	auto dst_values = reinterpret_cast<const int64_t *>(dst_data.data);
	auto edge_id_values = reinterpret_cast<const int64_t *>(edge_id_data.data);
	if (weight_type == PhysicalType::INT64) {
		auto weight_values = reinterpret_cast<const int64_t *>(weight_data.data);
		for (idx_t i = 0; i < args.size(); i++) {
			auto src_idx = src_data.sel->get_index(i);
			auto dst_idx = dst_data.sel->get_index(i);
			auto edge_id_idx = edge_id_data.sel->get_index(i);
			auto weight_idx = weight_data.sel->get_index(i);
			if (!src_data.validity.RowIsValid(src_idx) || !dst_data.validity.RowIsValid(dst_idx) ||
			    !edge_id_data.validity.RowIsValid(edge_id_idx) || !weight_data.validity.RowIsValid(weight_idx)) {
				result_validity.SetInvalid(i);
				continue;
			}
			auto src = src_values[src_idx];
			auto pos = ++csr_entry->second->v[src + 1];
			auto weight = weight_values[weight_idx];
			csr_entry->second->e[(int64_t)pos - 1] = dst_values[dst_idx];
			csr_entry->second->edge_ids[(int64_t)pos - 1] = edge_id_values[edge_id_idx];
			csr_entry->second->w[(int64_t)pos - 1] = weight;
			result_data[i] = static_cast<int32_t>(weight);
		}
		return;
	}

	auto weight_values = reinterpret_cast<const double_t *>(weight_data.data);
	for (idx_t i = 0; i < args.size(); i++) {
		auto src_idx = src_data.sel->get_index(i);
		auto dst_idx = dst_data.sel->get_index(i);
		auto edge_id_idx = edge_id_data.sel->get_index(i);
		auto weight_idx = weight_data.sel->get_index(i);
		if (!src_data.validity.RowIsValid(src_idx) || !dst_data.validity.RowIsValid(dst_idx) ||
		    !edge_id_data.validity.RowIsValid(edge_id_idx) || !weight_data.validity.RowIsValid(weight_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}
		auto src = src_values[src_idx];
		auto pos = ++csr_entry->second->v[src + 1];
		auto weight = weight_values[weight_idx];
		csr_entry->second->e[(int64_t)pos - 1] = dst_values[dst_idx];
		csr_entry->second->edge_ids[(int64_t)pos - 1] = edge_id_values[edge_id_idx];
		csr_entry->second->w_double[(int64_t)pos - 1] = weight;
		result_data[i] = static_cast<int32_t>(weight);
	}
}

ScalarFunctionSet GetCSRVertexFunction() {
	ScalarFunctionSet set("create_csr_vertex");

	set.AddFunction(ScalarFunction(
	    "create_csr_vertex", {LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	    LogicalType::BIGINT, CreateCsrVertexFunction, CSRFunctionData::CSRVertexBind));

	return set;
}

ScalarFunctionSet GetCSREdgeFunction() {
	ScalarFunctionSet set("create_csr_edge");
	/* 1. CSR ID
	 * 2. Vertex size
	 * 3. Sum of the edges (assuming all unique vertices)
	 * 4. Edge size (to ensure all vertices are unique this should equal point 3)
	 * 4. source rowid
	 * 5. destination rowid
	 * 6. edge rowid
	 * 7. <optional> edge weight (INT OR DOUBLE)
	 */

	//! No edge weight
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                               LogicalType::INTEGER, CreateCsrEdgeFunction, CSRFunctionData::CSREdgeBind));

	//! Integer for edge weight
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                               LogicalType::INTEGER, CreateCsrEdgeFunction, CSRFunctionData::CSREdgeBind));

	//! Double for edge weight
	set.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE},
	                               LogicalType::INTEGER, CreateCsrEdgeFunction, CSRFunctionData::CSREdgeBind));

	return set;
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterCSRCreationScalarFunctions(ExtensionLoader &loader) {
	loader.RegisterFunction(GetCSREdgeFunction());
	loader.RegisterFunction(GetCSRVertexFunction());
}

} // namespace duckdb
