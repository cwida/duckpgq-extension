#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/functions/function_data/pagerank_function_data.hpp"
#include <duckpgq/core/functions/function_data/pagerank_function_data.hpp>
#include <duckpgq/core/functions/scalar.hpp>
#include <duckpgq/core/functions/table/pagerank.hpp>
#include <duckpgq/core/utils/duckpgq_bitmap.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {
namespace core {


static void PageRankFunction(DataChunk &args,
                                             ExpressionState &state,
                                             Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (PageRankFunctionData &)*func_expr.bind_info;
  auto duckpgq_state = GetDuckPGQState(info.context);

  auto csr_entry = duckpgq_state->csr_list.find((uint64_t)info.csr_id);
  if (csr_entry == duckpgq_state->csr_list.end()) {
    throw ConstraintException("CSR not found. Is the graph populated?");
  }

  if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e)) {
    throw ConstraintException(
        "Need to initialize CSR before doing local clustering coefficient.");
  }
  int64_t *v = (int64_t *)duckpgq_state->csr_list[info.csr_id]->v;
  vector<int64_t> &e = duckpgq_state->csr_list[info.csr_id]->e;
  size_t v_size = duckpgq_state->csr_list[info.csr_id]->vsize;
  // get src vector for searches
  auto &src = args.data[1];
  UnifiedVectorFormat vdata_src;
  src.ToUnifiedFormat(args.size(), vdata_src);
  auto src_data = (int64_t *)vdata_src.data;
  ValidityMask &result_validity = FlatVector::Validity(result);
  // create result vector
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<int64_t>(result);



  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterPageRankScalarFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(
      db,
      ScalarFunction(
          "pagerank",
          {LogicalType::INTEGER, LogicalType::BIGINT},
          LogicalType::BIGINT, PageRankFunction,
          PageRankFunctionData::PageRankBind));
}

} // namespace core
} // namespace duckpgq