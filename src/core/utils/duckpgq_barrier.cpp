
#include "duckpgq/core/utils/duckpgq_barrier.hpp"

#include <mutex>

namespace duckpgq {
namespace core {

Barrier::Barrier(std::size_t iCount)
    : mThreshold(iCount), mCount(iCount), mGeneration(0) {}

void Barrier::Wait() {
  std::unique_lock<std::mutex> lLock{mMutex};
  auto lGen = mGeneration.load();
  if (!--mCount) {
    mGeneration++;
    mCount = mThreshold;
    mCond.notify_all();
  } else {
    mCond.wait(lLock, [this, lGen] { return lGen != mGeneration; });
  }
}

} // namespace core
} // namespace duckpgq
