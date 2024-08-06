#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"

namespace duckdb {

class DuckpgqExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

} // namespace duckpgq
