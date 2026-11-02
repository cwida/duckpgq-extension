#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

#include <chrono>
#include <thread>

namespace duckdb {

//! The plumbing of the optional benchmark output (experimental_path_finding_operator_benchmark), shared by the
//! kernels and the partitioned CSR build: every writer of <prefix>_phase_timing.csv goes through one lock, and the
//! per-partition explore samples have one record and one writer. scripts/pathfinding_benchmark.py parses the files,
//! so the columns and the file name suffixes are fixed.

//! The run id of one state: local time to the microsecond, so that the rows of concurrent states stay apart
string CreateBenchmarkRunId();

//! Milliseconds since `start`, or between two recorded points
double ElapsedMs(std::chrono::steady_clock::time_point start);
double ElapsedMs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end);

//! The core the calling thread runs on (-1 when unknown)
int CurrentCoreId();

//! Edges over all partitions
size_t GetLocalCSREdgeCount(const std::vector<shared_ptr<LocalCSR>> &partition_csrs);

//! Appends one row to <prefix>_phase_timing.csv, writing the header for a new file. The columns are
//! Phase,RunID,ThreadCount,PairCount,VertexCount,EdgeCount,PartitionCount,Time_ms,MemoryBytes; writers that have
//! no value for a column pass 0.
void AppendPhaseTimingRow(const string &prefix, const string &phase, const string &run_id, idx_t thread_count,
                          idx_t pair_count, idx_t vertex_count, idx_t edge_count, idx_t partition_count, double time_ms,
                          idx_t memory_bytes);

//! One explore phase of one partition on one worker
struct ExploreTiming {
	std::thread::id thread_id;
	int core_id;
	double time_ms;
	idx_t thread_count;
	size_t vertex_count;
	size_t edge_count;
	size_t partition_count;
	int64_t iteration;
};

//! Appends a sample for the calling thread to `into`; the caller serialises access to `into`
void RecordExploreTiming(vector<ExploreTiming> &into, double time_ms, idx_t thread_count, size_t vertex_count,
                         size_t edge_count, size_t partition_count, int64_t iteration);

//! Writes the samples to `filename` (replacing it) under the header ThreadID,CoreID,Time_ms,ThreadCount,vsize,
//! esize,numPartitions,Iter
void WriteExploreTimings(const string &filename, const vector<ExploreTiming> &timings);

//! A stopwatch for one phase of a kernel. It reads the clock only when the benchmark is enabled, so the kernels can
//! time every phase without paying for the clock in production runs; ElapsedMs() is 0 when disabled.
class PhaseTimer {
public:
	explicit PhaseTimer(bool enabled_p) : enabled(enabled_p) {
		if (enabled) {
			start = std::chrono::steady_clock::now();
		}
	}

	double ElapsedMs() const {
		return enabled ? duckdb::ElapsedMs(start) : 0.0;
	}

private:
	bool enabled;
	std::chrono::steady_clock::time_point start;
};

} // namespace duckdb
