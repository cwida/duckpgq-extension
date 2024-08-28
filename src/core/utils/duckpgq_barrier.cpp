
#include "duckpgq/core/utils/duckpgq_barrier.hpp"

#include <mutex>

namespace duckpgq {
namespace core {

Barrier::Barrier(std::size_t iCount)
    : mThreshold(iCount), mCount(iCount), mGeneration(0) {}

void Barrier::Wait(std::function<void()> resetAction) {
  std::unique_lock<std::mutex> lLock{mMutex};
  auto lGen = mGeneration.load();

  if (!--mCount) {
    // Last thread to reach the barrier
    mGeneration++;
    mCount = mThreshold;

    // If a reset action is provided, execute it
    if (resetAction) {
      resetAction();  // Perform the reset action
    }

    mCond.notify_all();  // Wake up all waiting threads
  } else {
    // Other threads wait for the generation to change
    mCond.wait(lLock, [this, lGen] { return lGen != mGeneration; });
  }
}

} // namespace core
} // namespace duckpgq
