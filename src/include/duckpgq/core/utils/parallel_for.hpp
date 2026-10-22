#pragma once

#include "duckpgq/common.hpp"

#include <exception>
#include <thread>

namespace duckdb {

//! Runs fn(begin, end, thread_idx) over [0, count) on up to thread_count threads and rethrows the first exception.
//! For serial steps of an operator (Finalize, event scheduling) that have the machine to themselves.
template <class FUNC>
void ParallelFor(idx_t count, idx_t thread_count, FUNC &&fn) {
	thread_count = std::max<idx_t>(1, std::min(thread_count, count));
	if (thread_count == 1) {
		fn(idx_t(0), count, idx_t(0));
		return;
	}
	auto chunk = (count + thread_count - 1) / thread_count;
	vector<std::thread> threads;
	vector<std::exception_ptr> errors(thread_count);
	for (idx_t thread_idx = 0; thread_idx < thread_count; thread_idx++) {
		auto begin = thread_idx * chunk;
		auto end = std::min(count, begin + chunk);
		if (begin >= end) {
			break;
		}
		threads.emplace_back([&fn, &errors, thread_idx, begin, end]() {
			try {
				fn(begin, end, thread_idx);
			} catch (...) {
				errors[thread_idx] = std::current_exception();
			}
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	for (auto &error : errors) {
		if (error) {
			std::rethrow_exception(error);
		}
	}
}

} // namespace duckdb
