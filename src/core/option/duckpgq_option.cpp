#include "duckpgq/core/option/duckpgq_option.hpp"
#include "duckpgq/common.hpp"

namespace duckpgq {
namespace core {


//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CorePGQOptions::Register(
    DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);
  DuckPGQParserExtension pgq_parser;
  config.parser_extensions.push_back(pgq_parser);
}

} // namespace core
} // namespace duckpgq

