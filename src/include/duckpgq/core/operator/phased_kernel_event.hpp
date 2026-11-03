#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

//! A parallel algorithm written as a sequence of phases.
//!
//! Each phase runs WorkerCount() tasks that call RunPhase(worker_id) concurrently and independently; when all of them
//! have finished, AdvancePhase() runs once, serially, and says whether another phase follows. The phases are chained as
//! events, so no task ever waits for another task: the scheduler is free to interleave the tasks of concurrent queries,
//! and it cannot deadlock on tasks that wait for peers it never schedules, which barriers across tasks did once the
//! barrier groups of concurrent queries exceeded the thread count.
class PhasedKernel {
public:
	virtual ~PhasedKernel() = default;
	//! Serial, once before the first phase; false when there is nothing to run (Finished() still follows)
	virtual bool Start() = 0;
	//! Tasks per phase, valid after Start()
	virtual idx_t WorkerCount() const = 0;
	//! The parallel part of the current phase, on each worker
	virtual void RunPhase(idx_t worker_id) = 0;
	//! Serial, after every worker of the phase has finished; true when another phase follows
	virtual bool AdvancePhase() = 0;
	//! Serial, once after the last phase
	virtual void Finished() {
	}
};

//! Runs one phase of a kernel; its FinishEvent advances the kernel and inserts the event of the next phase, so the
//! events that depended on the first event of the chain wait for the whole kernel.
class PhasedKernelEvent : public BasePipelineEvent {
public:
	PhasedKernelEvent(shared_ptr<PhasedKernel> kernel_p, Pipeline &pipeline_p, const PhysicalOperator &op_p,
	                  bool first_p = true);

	void Schedule() override;
	void FinishEvent() override;

private:
	shared_ptr<PhasedKernel> kernel;
	const PhysicalOperator &op;
	bool first;
	//! Whether the kernel has a phase to run (false after Start() declined)
	bool running = true;
};

} // namespace duckdb
