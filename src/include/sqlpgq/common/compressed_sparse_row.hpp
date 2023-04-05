

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/execution/executor.hpp"

#include <math.h>

namespace duckdb {

using namespace std;

class ClientContext;

class CSR {
public:
	CSR() {
	}
	~CSR() {
		delete[] v;
	}

	atomic<int64_t> *v;

	vector<int64_t> e;
	vector<int64_t> edge_ids;

	vector<int64_t> w;
	vector<double_t> w_double;

	bool initialized_v = false;
	bool initialized_e = false;
	bool initialized_w = false;
};

} // namespace duckdb
