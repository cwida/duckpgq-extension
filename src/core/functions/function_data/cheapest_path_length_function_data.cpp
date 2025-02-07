#include "duckpgq/core/functions/function_data/cheapest_path_length_function_data.hpp"
#include "duckpgq/core/utils/duckpgq_utils.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckpgq {

namespace core {

unique_ptr<FunctionData> CheapestPathLengthFunctionData::CheapestPathLengthBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {

  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }

  auto duckpgq_state = GetDuckPGQState(context);

  int32_t csr_id = ExpressionExecutor::EvaluateScalar(context, *arguments[0])
                       .GetValue<int32_t>();
  CSR *csr = duckpgq_state->GetCSR(csr_id);
  duckpgq_state->csr_to_delete.insert(csr_id);

  if (!(csr->initialized_v && csr->initialized_e && csr->initialized_w)) {
    throw ConstraintException(
        "Need to initialize CSR before doing cheapest path");
  }

  if (csr->w.empty()) {
    bound_function.return_type = LogicalType::DOUBLE;
  } else {
    bound_function.return_type = LogicalType::BIGINT;
  }

  return make_uniq<CheapestPathLengthFunctionData>(context, csr_id);
}

unique_ptr<FunctionData> CheapestPathLengthFunctionData::Copy() const {
  return make_uniq<CheapestPathLengthFunctionData>(context, csr_id);
}

bool CheapestPathLengthFunctionData::Equals(const FunctionData &other_p) const {
  auto &other = (const CheapestPathLengthFunctionData &)other_p;
  return other.csr_id == csr_id;
}

} // namespace core

} // namespace duckpgq