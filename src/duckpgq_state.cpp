#include "duckpgq_state.hpp"

namespace duckdb {

DuckPGQState::DuckPGQState() = default;

void DuckPGQState::QueryEnd() {
  parse_data.reset();
  transform_expression.clear();
  match_index = 0;              // Reset the index
  unnamed_graphtable_index = 1; // Reset the index
  for (const auto &csr_id : csr_to_delete) {
    csr_list.erase(csr_id);
  }
}

CreatePropertyGraphInfo *DuckPGQState::GetPropertyGraph(const string &pg_name) {
  auto pg_table_entry = registered_property_graphs.find(pg_name);
  if (pg_table_entry == registered_property_graphs.end()) {
    throw BinderException("Property graph %s does not exist", pg_name);
  }
  return reinterpret_cast<CreatePropertyGraphInfo *>(
      pg_table_entry->second.get());
}

duckpgq::core::CSR *DuckPGQState::GetCSR(int32_t id) {
  auto csr_entry = csr_list.find(id);
  if (csr_entry == csr_list.end()) {
    throw ConstraintException("CSR not found with ID %d", id);
  }
  return csr_entry->second.get();
}


} // namespace duckdb