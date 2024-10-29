#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include <duckpgq_state.hpp>

namespace duckdb {

class DuckpgqExtension : public Extension {
public:
  void Load(DuckDB &db) override;
  std::string Name() override;
};

class DuckpgqLoadExtension : public ExtensionCallback {
  void OnConnectionOpened(ClientContext &context) override {
    auto duckpgq_state = context.registered_state->Get<DuckPGQState>("duckpgq");
    if (duckpgq_state == nullptr) {
      auto state = make_shared_ptr<DuckPGQState>();
      context.registered_state->Insert("duckpgq", state);
      std::cout << "initialized duckpgq state" << std::endl;
    }

  }
};

} // namespace duckpgq
