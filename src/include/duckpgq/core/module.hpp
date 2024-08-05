#pragma once

namespace duckpgq {

namespace core {

struct CoreModule {
public:
  static void Register(DatabaseInstance &db);
}

} // namespace core

} // namespace duckpgq
