//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/utils/duckpgq_barrier.hpp
//
//
//===----------------------------------------------------------------------===//


#pragma once
#include <functional>
#include <condition_variable>
#include <duckpgq/common.hpp>

namespace duckpgq {
namespace core {

class Barrier {
public:
  explicit Barrier(std::size_t iCount);

  void Wait(idx_t worker_id);

  // Prints collected timing logs at the end
  // void PrintTimingLogs();

  void LogMessage(idx_t worker_id, const std::string &message);

private:
  std::mutex mMutex;
  std::condition_variable mCond;
  std::size_t mThreshold;
  std::atomic<std::size_t> mCount;
  std::atomic<std::size_t> mGeneration;

  std::mutex logMutex;
  std::vector<std::string> timingLogs;
};

} // namespace core
} // namespace duckpgq