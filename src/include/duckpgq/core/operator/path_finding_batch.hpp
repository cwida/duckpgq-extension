#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

class PathFindingBatch {
public:
	PathFindingBatch(shared_ptr<DataChunk> output_pairs_p, shared_ptr<DataChunk> search_pairs_p,
	                 vector<idx_t> output_to_search_p, idx_t output_index_p)
	    : output_pairs(std::move(output_pairs_p)), search_pairs(std::move(search_pairs_p)),
	      output_to_search(std::move(output_to_search_p)), output_index(output_index_p) {
	}

	idx_t SearchSize() const {
		return search_pairs->size();
	}

	idx_t OutputSize() const {
		return output_pairs->size();
	}

	bool HasIdentityMapping() const {
		return output_to_search.empty();
	}

public:
	shared_ptr<DataChunk> output_pairs;
	shared_ptr<DataChunk> search_pairs;
	vector<idx_t> output_to_search;
	idx_t output_index;
};

} // namespace duckdb
