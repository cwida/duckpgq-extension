#include "duckpgq/functions/tablefunctions/local_clustering_coefficient.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckpgq_extension.hpp"
#include "duckpgq/duckpgq_functions.hpp"

namespace duckdb {

unique_ptr<TableRef> LocalClusteringCoefficientBindReplace(ClientContext &context,
                                             TableFunctionBindInput &input) {
  return nullptr;
}


} // namespace duckdb

