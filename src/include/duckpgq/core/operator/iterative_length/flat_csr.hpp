#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

namespace duckdb {

//! How the EXPLORE phase of the MS-BFS lane kernel writes the lane words of the destinations
//! (experimental_path_finding_operator_explore_variant). PARTITIONED is the operator's design and the default; the
//! FLAT_* variants exist for the ablation that isolates what the partitioned CSR buys (17 Nov 2026). They traverse
//! one unpartitioned CSR with 32-bit destinations (FlatCSR) and differ in how workers share the `next` array:
//!   FLAT_SERIAL  one partition, one owner: a single worker expands the level (CHECK stays parallel)
//!   FLAT_RANGES  every worker owns a destination range and reads every active row for its slice of it: ownership
//!                without physical partitioning, no synchronisation
//!   FLAT_ATOMIC  workers take morsels of sources and OR into shared lane words with fetch_or (after testing whether
//!                the word would change)
//!   FLAT_LOCKED  as FLAT_ATOMIC with a striped spinlock around a plain OR
enum class ExploreVariant : uint8_t { PARTITIONED, FLAT_SERIAL, FLAT_RANGES, FLAT_ATOMIC, FLAT_LOCKED };

ExploreVariant ParseExploreVariant(const string &name);

//! The edges of a partitioned CSR as one CSR: offsets[v] .. offsets[v + 1] index the destinations of source v, sorted
//! ascending. Built once per partition set and cached for the process (one entry), so that a repeated query on a CSR
//! index pays the conversion once (the benchmark harness runs a warm-up query in the same process).
struct FlatCSR {
	vector<uint64_t> offsets;
	vector<uint32_t> destinations;

	static shared_ptr<FlatCSR> Get(const std::vector<shared_ptr<LocalCSR>> &partitions, idx_t vertex_count);
};

//! One spinlock per cache line; FLAT_LOCKED guards the lane words of a destination with the lock of its stripe
struct alignas(64) LaneStripeLock {
	std::atomic<bool> locked {false};

	void Lock() {
		while (true) {
			if (!locked.exchange(true, std::memory_order_acquire)) {
				return;
			}
			while (locked.load(std::memory_order_relaxed)) {
#if defined(__x86_64__) || defined(__i386__)
				__builtin_ia32_pause();
#elif defined(__aarch64__)
				__asm__ __volatile__("yield");
#endif
			}
		}
	}
	void Unlock() {
		locked.store(false, std::memory_order_release);
	}
};

} // namespace duckdb
