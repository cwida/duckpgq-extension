#include "duckpgq/core/functions/function_data/cheapest_path_length_function_data.hpp"
#include "duckpgq/core/utils/duckpgq_utils.hpp"

namespace duckpgq {

namespace core {

unique_ptr<FunctionData> CheapestPathLengthFunctionData::CheapestPathLengthBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {

  auto csr_id = GetCSRId(arguments[0], context);
  auto duckpgq_state = GetDuckPGQState(context);
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