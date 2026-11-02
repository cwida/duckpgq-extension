#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

class PathFindingBatch {
public:
	PathFindingBatch(shared_ptr<DataChunk> output_pairs_p, shared_ptr<DataChunk> search_pairs_p, idx_t output_index_p)
	    : output_pairs(std::move(output_pairs_p)), search_pairs(std::move(search_pairs_p)), output_index(output_index_p) {
	}

	shared_ptr<DataChunk> output_pairs;
	shared_ptr<DataChunk> search_pairs;
	idx_t output_index;
};

} // namespace duckdb
