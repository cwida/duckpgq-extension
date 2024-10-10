#pragma once

#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

struct ve {
  // higher 30 bits for v, lower 34 bits for e
  const uint8_t v_bits = 30;
  const uint8_t e_bits = 34;
  uint64_t value;
  const uint64_t v_mask = UINT64_MAX << e_bits;
  const uint64_t e_mask = UINT64_MAX >> v_bits;
  ve() : value(UINT64_MAX) {}
  explicit ve(uint64_t value) : value(value) {}
  ve(int64_t v, int64_t e) {
    uint64_t new_value = 0;
    new_value |= v < 0 ? v_mask : (v << e_bits);
    new_value |= e < 0 ? e_mask : (e & e_mask);
    value = new_value;
  }
  ve &operator=(std::initializer_list<int64_t> list) {
    uint64_t new_value = 0;
    auto it = list.begin();
    new_value |= *it < 0 ? v_mask : (*it << e_bits);
    new_value |= *(++it) < 0 ? e_mask : (*it & e_mask);
    value = new_value;
    return *this;
  }
  inline int64_t GetV() {
    return (value & v_mask) == v_mask ? -1
                                      : static_cast<int64_t>(value >> e_bits);
  }
  inline int64_t GetE() {
    return (value & e_mask) == e_mask ? -1
                                      : static_cast<int64_t>(value & e_mask);
  }
};


} // namespace core

} // namespace duckpgq
