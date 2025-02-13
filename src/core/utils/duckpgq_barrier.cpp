
#include "duckpgq/core/utils/duckpgq_barrier.hpp"

#include <duckpgq/common.hpp>
#include <duckdb/common/typedefs.hpp>
#include <iostream>
#include <mutex>
#include <fstream>
#include <thread>

namespace duckpgq {
namespace core {

Barrier::Barrier(std::size_t iCount)
    : mThreshold(iCount), mCount(iCount), mGeneration(0) {}

// Adds a custom log message
void Barrier::LogMessage(idx_t worker_id, const std::string &message) {
  std::lock_guard<std::mutex> logLock(logMutex);
  std::ostringstream log;
  log << "Thread " << worker_id << ": " << message;
  timingLogs.push_back(log.str());
}

// Writes collected timing logs to a file with a timestamp
void Barrier::PrintTimingLogs() {
  std::lock_guard<std::mutex> lock(logMutex);

  if (timingLogs.empty()) {
    return; // No logs to write
  }

  // Get the current timestamp for the filename
  auto now = std::chrono::system_clock::now();
  auto now_time_t = std::chrono::system_clock::to_time_t(now);
  std::ostringstream timestamp;
  timestamp << std::put_time(std::localtime(&now_time_t), "%Y-%m-%d_%H-%M-%S");

  // Construct the log filename
  std::string log_filename = "barrier_logs_" + timestamp.str() + ".log";

  // Open file stream for writing
  std::ofstream log_file(log_filename, std::ios::out | std::ios::app);
  if (!log_file) {
    std::cerr << "Error: Could not open log file: " << log_filename << std::endl;
    return;
  }

  // Write logs to file
  for (const auto &entry : timingLogs) {
    log_file << entry << std::endl;
  }

  log_file.close();
  timingLogs.clear(); // Clear logs after writing
}

void Barrier::Wait(idx_t worker_id) {
  auto start_time = std::chrono::high_resolution_clock::now(); // Start timing

  std::unique_lock<std::mutex> lLock{mMutex};
  auto lGen = mGeneration.load();

  auto thread_id_str = std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));

  if (!--mCount) {
    // Last thread to reach the barrier
    mGeneration++;
    mCount = mThreshold;
    {
      std::lock_guard<std::mutex> logLock(logMutex);
      std::ostringstream log;
      log << "Thread " << worker_id << " releasing threads";
      timingLogs.push_back(log.str());
    }

    mCond.notify_all();  // Wake up all waiting threads
  } else {
    // Other threads wait for the generation to change
    mCond.wait(lLock, [this, lGen] { return lGen != mGeneration; });
  }

  auto end_time = std::chrono::high_resolution_clock::now(); // End timing
  double duration = std::chrono::duration<double, std::micro>(end_time - start_time).count();

  // Store the timing information instead of printing immediately
  {
    std::lock_guard<std::mutex> logLock(logMutex);
    std::ostringstream log;
    log << "Thread " << worker_id << " waited for " << duration << " µs";
    timingLogs.push_back(log.str());
  }
}

} // namespace core
} // namespace duckpgq
