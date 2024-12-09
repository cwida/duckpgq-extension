//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/utils/duckpgq_bitmap.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckpgq/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckpgq {
namespace core {

class DuckPGQBitmap {
public:
  explicit DuckPGQBitmap(size_t size);
  void set(size_t index);
  bool test(size_t index) const;
  void reset();

private:
  vector<uint64_t> bitmap;
};

} // namespace core
} // namespace duckpgq
