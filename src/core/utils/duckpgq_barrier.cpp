#include "duckpgq/core/utils/duckpgq_barrier.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"

#include <chrono>

namespace duckdb {

//! How long a waiter sleeps between checks of the abort condition
static constexpr std::chrono::milliseconds BARRIER_POLL_INTERVAL {20};

Barrier::Barrier(std::size_t count_p, std::function<bool()> should_abort_p)
    : threshold(count_p), count(count_p), should_abort(std::move(should_abort_p)) {
}

void Barrier::Wait(idx_t) {
	std::unique_lock<std::mutex> lock(mutex);
	if (aborted) {
		throw InterruptException();
	}
	auto arrival_generation = generation;
	if (--count == 0) {
		// Last worker to arrive: release everyone and reset for the next phase
		generation++;
		count = threshold;
		condition.notify_all();
		return;
	}
	while (true) {
		condition.wait_for(lock, BARRIER_POLL_INTERVAL,
		                   [&] { return arrival_generation != generation || aborted; });
		if (aborted) {
			throw InterruptException();
		}
		if (arrival_generation != generation) {
			return;
		}
		if (should_abort && should_abort()) {
			aborted = true;
			condition.notify_all();
			throw InterruptException();
		}
	}
}

void Barrier::Abort() {
	std::lock_guard<std::mutex> lock(mutex);
	aborted = true;
	condition.notify_all();
}

bool Barrier::IsAborted() const {
	return aborted;
}

unique_ptr<Barrier> MakeQueryBarrier(std::size_t count, ClientContext &context) {
	auto &executor = Executor::Get(context);
	return make_uniq<Barrier>(count, [&executor, &context]() {
		return executor.HasError() || context.interrupt_state == ClientInterruptState::INTERRUPTED;
	});
}

} // namespace duckdb
