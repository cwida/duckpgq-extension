#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

class PathFindingBatch {
public:
	PathFindingBatch(shared_ptr<DataChunk> pairs_p, idx_t output_index_p)
	    : pairs(std::move(pairs_p)), output_index(output_index_p) {
	}

	idx_t Size() const {
		return pairs->size();
	}

public:
	shared_ptr<DataChunk> pairs;
	idx_t output_index;
};

} // namespace duckdb
