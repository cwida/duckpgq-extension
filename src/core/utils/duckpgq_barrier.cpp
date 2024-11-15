
#include "duckpgq/core/utils/duckpgq_barrier.hpp"
#include <mutex>
#include <iostream>
#include <thread>

namespace duckpgq {
namespace core {

Barrier::Barrier(std::size_t iCount)
    : mThreshold(iCount), mCount(iCount), mGeneration(0) {}

void Barrier::Wait(std::function<void()> resetAction) {
  std::unique_lock<std::mutex> lLock{mMutex};
  auto lGen = mGeneration.load();

  // Convert thread ID to a string representation (hash)
  auto thread_id_str = std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));

  std::cout << "Thread " << thread_id_str << " entering barrier: Current generation = "
            << lGen << ", mCount = " << mCount << ", mThreshold = " << mThreshold << std::endl;

  if (!--mCount) {
    // Last thread to reach the barrier
    mGeneration++;
    mCount = mThreshold;

    std::cout << "Thread " << thread_id_str
              << " is the last thread. Performing reset action and updating state." << std::endl;

    if (resetAction) {
      resetAction();  // Perform the reset action
    }

    std::cout << "Notifying all threads: New generation = " << mGeneration << ", mCount reset to "
              << mCount << "." << std::endl;

    mCond.notify_all();  // Wake up all waiting threads
  } else {
    // Other threads wait for the generation to change
    std::cout << "Thread " << thread_id_str
              << " waiting: Current generation = " << lGen << ", mCount = " << mCount << "." << std::endl;

    mCond.wait(lLock, [this, lGen] { return lGen != mGeneration; });

    std::cout << "Thread " << thread_id_str << " resumed: Current generation = "
              << mGeneration << ", mCount = " << mCount << "." << std::endl;
  }
}

} // namespace core
} // namespace duckpgq
