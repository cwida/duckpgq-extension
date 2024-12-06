#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include <duckpgq_state.hpp>

namespace duckdb {
class DuckpgqExtensionCallback : public ExtensionCallback {
  void OnConnectionOpened(ClientContext &context) override {
    context.registered_state->Insert(
        "duckpgq", make_shared_ptr<DuckPGQState>(context.shared_from_this()));
  }
};
} // namespace duckdb