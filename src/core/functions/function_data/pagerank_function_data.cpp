#include "duckpgq/core/functions/function_data/pagerank_function_data.hpp"

#include <duckpgq/core/utils/duckpgq_utils.hpp>

namespace duckpgq {

namespace core {

// Constructor
PageRankFunctionData::PageRankFunctionData(ClientContext &ctx, int32_t csr)
    : context(ctx), csr_id(csr), damping_factor(0.85),
      convergence_threshold(1e-6), iteration_count(0), state_initialized(false),
      converged(false) {}

unique_ptr<FunctionData>
PageRankFunctionData::PageRankBind(ClientContext &context,
                                   ScalarFunction &bound_function,
                                   vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }

  int32_t csr_id = ExpressionExecutor::EvaluateScalar(context, *arguments[0])
                       .GetValue<int32_t>();
  auto duckpgq_state = GetDuckPGQState(context);
  duckpgq_state->csr_to_delete.insert(csr_id);

  return make_uniq<PageRankFunctionData>(context, csr_id);
}

// Copy method
unique_ptr<FunctionData> PageRankFunctionData::Copy() const {
  auto result = make_uniq<PageRankFunctionData>(context, csr_id);
  result->rank = rank;           // Deep copy of rank vector
  result->temp_rank = temp_rank; // Deep copy of temp_rank vector
  result->damping_factor = damping_factor;
  result->convergence_threshold = convergence_threshold;
  result->iteration_count = iteration_count;
  result->state_initialized = state_initialized;
  result->converged = converged;
  // Note: state_lock is not copied as mutexes are not copyable
  return std::move(result);
}

// Equals method
bool PageRankFunctionData::Equals(const FunctionData &other_p) const {
  auto &other = (const PageRankFunctionData &)other_p;
  if (csr_id != other.csr_id) {
    return false;
  }
  if (rank != other.rank) {
    return false;
  }
  if (temp_rank != other.temp_rank) {
    return false;
  }
  if (damping_factor != other.damping_factor) {
    return false;
  }
  if (convergence_threshold != other.convergence_threshold) {
    return false;
  }
  if (iteration_count != other.iteration_count) {
    return false;
  }
  if (state_initialized != other.state_initialized) {
    return false;
  }
  if (converged != other.converged) {
    return false;
  }
  return true;
}
} // namespace core

} // namespace duckpgq