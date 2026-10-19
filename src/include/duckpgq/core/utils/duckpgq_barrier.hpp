//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/utils/duckpgq_barrier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include <condition_variable>
#include <functional>
#include <mutex>
#include <duckpgq/common.hpp>

namespace duckdb {

//! A reusable barrier for lock-step worker tasks that can be abandoned.
//!
//! Workers of one event wait for each other at every phase boundary. If one worker fails, or the query is
//! cancelled, the remaining workers must not wait for siblings that will never arrive: Wait throws
//! InterruptException once Abort has been called or the should_abort callback reports that the query is over.
class Barrier {
public:
	explicit Barrier(std::size_t count, std::function<bool()> should_abort = nullptr);

	//! Blocks until all workers have arrived. Throws InterruptException when the barrier is aborted while waiting.
	void Wait(idx_t worker_id);
	//! Wakes every waiter and makes every current and future Wait throw.
	void Abort();
	bool IsAborted() const;

private:
	std::mutex mutex;
	std::condition_variable condition;
	std::size_t threshold;
	std::size_t count;
	std::size_t generation = 0;
	bool aborted = false;
	std::function<bool()> should_abort;
};

//! A barrier whose waiters give up when the query that owns `context` has failed or was interrupted.
unique_ptr<Barrier> MakeQueryBarrier(std::size_t count, ClientContext &context);

//! Aborts a barrier when a task body unwinds with an exception, so that the sibling tasks waiting at the barrier
//! stop instead of waiting for a worker that has already failed. Call Disarm() on the success path.
class BarrierAbortOnUnwind {
public:
	explicit BarrierAbortOnUnwind(Barrier &barrier_p) : barrier(barrier_p) {
	}
	~BarrierAbortOnUnwind() {
		if (armed) {
			barrier.Abort();
		}
	}
	void Disarm() {
		armed = false;
	}

private:
	Barrier &barrier;
	bool armed = true;
};

} // namespace duckdb
