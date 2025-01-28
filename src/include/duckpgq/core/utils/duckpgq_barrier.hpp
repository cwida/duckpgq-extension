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
#include <atomic>
#include "mutex"

namespace duckpgq {
namespace core {

class Barrier {
public:
  explicit Barrier(std::size_t iCount);

  size_t Wait(std::function<void()> resetAction = nullptr);

private:
  std::mutex mMutex;
  std::condition_variable mCond;
  std::size_t mThreshold;
  std::atomic<std::size_t> mCount;
  std::atomic<std::size_t> mGeneration;
};

} // namespace core
} // namespace duckpgq