#include "duckpgq/core/operator/bfs_benchmark.hpp"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <sstream>
#ifdef __linux__
#include <sched.h>
#elif defined(__APPLE__)
#include <pthread.h>
#endif

namespace duckdb {

//! One lock for the phase timing file: the kernels, the operator and the CSR build append to the same file
static mutex phase_timing_file_lock;

string CreateBenchmarkRunId() {
	auto now = std::chrono::system_clock::now();
	auto time_t_now = std::chrono::system_clock::to_time_t(now);
	auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count() % 1000000;
	std::stringstream ss;
	ss << std::put_time(std::localtime(&time_t_now), "%Y-%m-%d_%H-%M-%S") << "_" << std::setfill('0') << std::setw(6)
	   << micros;
	return ss.str();
}

double ElapsedMs(std::chrono::steady_clock::time_point start) {
	return ElapsedMs(start, std::chrono::steady_clock::now());
}

double ElapsedMs(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end) {
	return std::chrono::duration<double, std::milli>(end - start).count();
}

int CurrentCoreId() {
#ifdef __linux__
	return sched_getcpu();
#elif defined(__APPLE__)
	uint64_t tid;
	pthread_threadid_np(nullptr, &tid);
	auto hardware_threads = std::thread::hardware_concurrency();
	return hardware_threads > 0 ? static_cast<int>(tid % hardware_threads) : -1;
#else
	return -1;
#endif
}

size_t GetLocalCSREdgeCount(const std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	size_t edge_count = 0;
	for (const auto &local_csr : partition_csrs) {
		edge_count += local_csr->GetEdgeSize();
	}
	return edge_count;
}

void AppendPhaseTimingRow(const string &prefix, const string &phase, const string &run_id, idx_t thread_count,
                          idx_t pair_count, idx_t vertex_count, idx_t edge_count, idx_t partition_count, double time_ms,
                          idx_t memory_bytes) {
	auto file_name = prefix + "_phase_timing.csv";
	lock_guard<mutex> lock(phase_timing_file_lock);
	bool write_header = !std::filesystem::exists(file_name);
	std::ofstream outfile(file_name, std::ios::app);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding phase benchmark file \"%s\"", file_name);
	}
	if (write_header) {
		outfile << "Phase,RunID,ThreadCount,PairCount,VertexCount,EdgeCount,PartitionCount,Time_ms,MemoryBytes\n";
	}
	outfile << phase << "," << run_id << "," << thread_count << "," << pair_count << "," << vertex_count << ","
	        << edge_count << "," << partition_count << "," << time_ms << "," << memory_bytes << "\n";
}

void RecordExploreTiming(vector<ExploreTiming> &into, double time_ms, idx_t thread_count, size_t vertex_count,
                         size_t edge_count, size_t partition_count, int64_t iteration) {
	into.push_back({std::this_thread::get_id(), CurrentCoreId(), time_ms, thread_count, vertex_count, edge_count,
	                partition_count, iteration});
}

void WriteExploreTimings(const string &filename, const vector<ExploreTiming> &timings) {
	std::ofstream file(filename);
	if (!file.is_open()) {
		return;
	}
	file << "ThreadID,CoreID,Time_ms,ThreadCount,vsize,esize,numPartitions,Iter\n";
	for (const auto &entry : timings) {
		file << entry.thread_id << "," << entry.core_id << "," << entry.time_ms << "," << entry.thread_count << ","
		     << entry.vertex_count << "," << entry.edge_count << "," << entry.partition_count << "," << entry.iteration
		     << "\n";
	}
}

} // namespace duckdb
