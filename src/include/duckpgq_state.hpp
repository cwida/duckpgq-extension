#pragma once

#include "duckpgq/common.hpp"

#include <duckpgq/core/utils/compressed_sparse_row.hpp>

namespace duckdb {

class DuckPGQState : public ClientContextState {
public:
  explicit DuckPGQState(shared_ptr<ClientContext> context);

  void QueryEnd() override;
  CreatePropertyGraphInfo *GetPropertyGraph(const string &pg_name);
  duckpgq::core::CSR *GetCSR(int32_t id);

  void RetrievePropertyGraphs(shared_ptr<ClientContext> context);

public:
  unique_ptr<ParserExtensionParseData> parse_data;

  unordered_map<int32_t, unique_ptr<ParsedExpression>> transform_expression;
  int32_t match_index = 0;
  int32_t unnamed_graphtable_index = 1; // Used to generate unique names for
  // unnamed graph tables

  //! Property graphs that are registered
  std::unordered_map<string, unique_ptr<CreateInfo>> registered_property_graphs;

  //! Used to build the CSR data structures required for path-finding queries
  std::unordered_map<int32_t, unique_ptr<duckpgq::core::CSR>> csr_list;
  std::mutex csr_lock;
  std::unordered_set<int32_t> csr_to_delete;
};

} // namespace duckdb
