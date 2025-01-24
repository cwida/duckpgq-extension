#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/functions/function_data/local_clustering_coefficient_function_data.hpp"
#include <duckpgq/core/functions/function_data/weakly_connected_component_function_data.hpp>
#include <duckpgq/core/functions/scalar.hpp>
#include <duckpgq/core/functions/table/weakly_connected_component.hpp>
#include <duckpgq/core/utils/duckpgq_bitmap.hpp>
#include <duckpgq/core/utils/duckpgq_utils.hpp>
#include <atomic>
#include <vector>

namespace duckpgq {
namespace core {

// Helper function to find the root of a node with path compression
static int64_t FindTreeRoot(std::vector<std::atomic<int64_t>> &forest,
                            int64_t node) {
  while (true) {
    int64_t parent = forest[node].load(std::memory_order_relaxed);
    if (parent == node) {
      return node; // Found the root
    }
    // Path compression: update parent to grandparent
    int64_t grandparent = forest[parent].load(std::memory_order_relaxed);
    forest[node].compare_exchange_weak(parent, grandparent,
                                       std::memory_order_relaxed);
    node = parent;
  }
}

// Helper function to link two nodes in the same connected component
static void Link(std::vector<std::atomic<int64_t>> &forest, int64_t nodeA,
                 int64_t nodeB) {
  int64_t rootA = FindTreeRoot(forest, nodeA);
  int64_t rootB = FindTreeRoot(forest, nodeB);

  if (rootA != rootB) {
    forest[rootA].store(rootB, std::memory_order_relaxed);
  }
}

static void WeaklyConnectedComponentFunction(DataChunk &args,
                                             ExpressionState &state,
                                             Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (WeaklyConnectedComponentFunctionData &)*func_expr.bind_info;
  auto duckpgq_state = GetDuckPGQState(info.context);

  auto csr_entry = duckpgq_state->csr_list.find((uint64_t)info.csr_id);
  if (csr_entry == duckpgq_state->csr_list.end()) {
    throw ConstraintException("CSR not found. Is the graph populated?");
  }

  if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e)) {
    throw ConstraintException(
        "Need to initialize CSR before doing weakly connected components.");
  }

  // Retrieve CSR data
  int64_t *v = (int64_t *)duckpgq_state->csr_list[info.csr_id]->v;
  vector<int64_t> &e = duckpgq_state->csr_list[info.csr_id]->e;
  size_t v_size = duckpgq_state->csr_list[info.csr_id]->vsize;

  // Get source vector for searches
  auto &src = args.data[1];
  UnifiedVectorFormat vdata_src;
  src.ToUnifiedFormat(args.size(), vdata_src);
  auto src_data = (int64_t *)vdata_src.data;
  ValidityMask &result_validity = FlatVector::Validity(result);

  // Create result vector
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<int64_t>(result);

  std::vector<std::atomic<int64_t>> forest(v_size);

  // Check if already converged
  if (!info.state_converged) {
    std::lock_guard<std::mutex> guard(info.wcc_lock); // Thread safety
    if (!info.state_converged) {
      // Initialize the forest for connected components
      for (int64_t i = 0; i < v_size - 1; ++i) {
        forest[i].store(i, std::memory_order_relaxed); // Each node points to itself
      }
      // Process edges to link nodes
      for (int64_t i = 0; i < v_size - 1; i++) {
        for (int64_t edge_idx = v[i]; edge_idx < v[i + 1]; edge_idx++) {
          int64_t neighbor = e[edge_idx];
          Link(forest, i, neighbor);
        }
      }
      info.state_converged = true;
    }
  }
  // Assign component IDs for the source nodes
  for (size_t i = 0; i < args.size(); i++) {
    int64_t src_node = src_data[i];
    if (src_node >= 0 && src_node < v_size) {
      result_data[i] =
          FindTreeRoot(forest, src_node); // Assign component ID to the result
    } else {
      result_validity.SetInvalid(i);
    }
  }

  // Mark CSR for deletion
  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreScalarFunctions::RegisterWeaklyConnectedComponentScalarFunction(
    DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(
      db,
      ScalarFunction(
          "weakly_connected_component",
          {LogicalType::INTEGER, LogicalType::BIGINT}, LogicalType::BIGINT,
          WeaklyConnectedComponentFunction,
          WeaklyConnectedComponentFunctionData::WeaklyConnectedComponentBind));
}

} // namespace core
} // namespace duckpgq