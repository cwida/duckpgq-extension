#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {
class CSR {
public:
  CSR() {}
  ~CSR() { delete[] v; }

  atomic<int64_t> *v;

  vector<int64_t> e;
  vector<int64_t> edge_ids;

  vector<int64_t> w;
  vector<double> w_double;

  bool initialized_v = false;
  bool initialized_e = false;
  bool initialized_w = false;

  size_t vsize;

  string ToString() {
    string result;
    if (initialized_v) {
      result += "v: ";
      for (size_t i = 0; i < vsize; i++) {
        result += to_string(v[i].load()) + " ";
      }
    }
    result += "\n";
    if (initialized_e) {
      result += "e: ";
      for (size_t i = 0; i < e.size(); i++) {
        result += to_string(e[i]) + " ";
      }
    }
    result += "\n";
    if (initialized_w) {
      result += "w: ";
      for (size_t i = 0; i < w.size(); i++) {
        result += to_string(w[i]) + " ";
      }
    }
    return result;
  };

};
} // namespace duckdb
