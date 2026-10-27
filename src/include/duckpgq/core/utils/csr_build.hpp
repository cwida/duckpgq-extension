//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/utils/csr_build.hpp
//
// Building a destination-range partition of the partitioned CSR from its edges
// without a per-vertex counter array: sort the partition's edges by source in
// cache and emit the sparse rows in one sweep. Shared by the CSR index build
// and the operator's per-query build.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

#include <algorithm>

namespace duckdb {

//! Number of bits needed for a source id in a graph of vertex_count vertices (plus the two sentinel slots)
inline idx_t CSRSourceBits(idx_t vertex_count) {
	idx_t bits = 1;
	while (bits < 32 && (idx_t(1) << bits) < vertex_count + 2) {
		bits++;
	}
	return bits;
}

//! LSD radix sort of entries by (`src`, `local_dst`): the rows of a partition come out grouped by source with the
//! destinations of a row in ascending order, so a destination sub-range of a row is found by binary search (W32:
//! sub-range tasks on coarse partitions read only their own edges). `scratch` and `histogram` are reused across
//! calls.
template <class ENTRY>
void CSRRadixSortBySource(vector<ENTRY> &entries, vector<ENTRY> &scratch, vector<idx_t> &histogram, idx_t src_bits) {
	constexpr idx_t RADIX_BITS = 11;
	constexpr idx_t RADIX_BUCKETS = idx_t(1) << RADIX_BITS;
	scratch.resize(entries.size());
	histogram.resize(RADIX_BUCKETS);
	auto pass = [&](auto key_of) {
		std::fill(histogram.begin(), histogram.end(), 0);
		for (auto &entry : entries) {
			histogram[key_of(entry)]++;
		}
		idx_t offset = 0;
		for (auto &count : histogram) {
			auto next = offset + count;
			count = offset;
			offset = next;
		}
		for (auto &entry : entries) {
			scratch[histogram[key_of(entry)]++] = entry;
		}
		std::swap(entries, scratch);
	};
	// Least significant first: the destination (16 bits, two passes), then the source
	for (idx_t shift = 0; shift < 16; shift += RADIX_BITS) {
		pass([shift](const ENTRY &entry) { return (static_cast<idx_t>(entry.local_dst) >> shift) & (RADIX_BUCKETS - 1); });
	}
	auto src_passes = (src_bits + RADIX_BITS - 1) / RADIX_BITS;
	for (idx_t src_pass = 0; src_pass < src_passes; src_pass++) {
		auto shift = src_pass * RADIX_BITS;
		pass([shift](const ENTRY &entry) { return (static_cast<idx_t>(entry.src) >> shift) & (RADIX_BUCKETS - 1); });
	}
}

//! Emits a partition with sparse rows from entries sorted by source. Each entry has `src` (global source id) and
//! `local_dst` (destination offset within the partition); on_edge(position, entry) stores any per-edge payload.
template <class ENTRY, class ON_EDGE>
shared_ptr<LocalCSR> CSREmitSparsePartition(idx_t start_vertex, idx_t end_vertex, idx_t vertex_count,
                                            const vector<ENTRY> &sorted, ON_EDGE &&on_edge) {
	auto partition = make_shared_ptr<LocalCSR>(start_vertex, end_vertex, vertex_count, false);
	partition->e.resize(sorted.size());
	if (!sorted.empty()) {
		partition->row_offsets.push_back(0);
	}
	uint32_t previous_src = 0;
	bool have_row = false;
	for (idx_t position = 0; position < sorted.size(); position++) {
		auto &entry = sorted[position];
		if (!have_row || entry.src != previous_src) {
			partition->source_vertices.push_back(entry.src);
			partition->row_offsets.back() = NumericCast<uint32_t>(position);
			partition->row_offsets.push_back(NumericCast<uint32_t>(sorted.size()));
			previous_src = entry.src;
			have_row = true;
		}
		partition->e[position] = entry.local_dst;
		on_edge(position, entry);
	}
	partition->expected_sparse_row_count = partition->source_vertices.size();
	partition->v_array_size = 0;
	partition->initialized_e = true;
	partition->sparse_rows_initialized = true;
	partition->BuildRowBuckets(vertex_count);
	return partition;
}

} // namespace duckdb
