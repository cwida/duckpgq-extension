#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

class DuckpgqExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

} // namespace duckpgq
