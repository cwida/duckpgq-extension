#include "duckpgq/core/operator/phased_kernel_event.hpp"

#include "duckdb/parallel/executor_task.hpp"

namespace duckdb {

namespace {

class PhasedKernelTask : public ExecutorTask {
public:
	PhasedKernelTask(shared_ptr<Event> event_p, ClientContext &context, shared_ptr<PhasedKernel> kernel_p,
	                 idx_t worker_id_p, const PhysicalOperator &op_p)
	    : ExecutorTask(context, std::move(event_p), op_p), kernel(std::move(kernel_p)), worker_id(worker_id_p) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		if (kernel) {
			kernel->RunPhase(worker_id);
		}
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	//! Null for the placeholder task of a kernel with nothing to run
	shared_ptr<PhasedKernel> kernel;
	idx_t worker_id;
};

} // namespace

PhasedKernelEvent::PhasedKernelEvent(shared_ptr<PhasedKernel> kernel_p, Pipeline &pipeline_p,
                                     const PhysicalOperator &op_p, bool first_p)
    : BasePipelineEvent(pipeline_p), kernel(std::move(kernel_p)), op(op_p), first(first_p) {
}

void PhasedKernelEvent::Schedule() {
	auto &context = pipeline->GetClientContext();
	if (first) {
		running = kernel->Start();
	}
	vector<shared_ptr<Task>> tasks;
	if (!running) {
		// An event needs a task; this one only completes the event so that Finished() runs
		tasks.push_back(make_uniq<PhasedKernelTask>(shared_from_this(), context, nullptr, 0, op));
	} else {
		auto workers = std::max<idx_t>(1, kernel->WorkerCount());
		for (idx_t worker = 0; worker < workers; worker++) {
			tasks.push_back(make_uniq<PhasedKernelTask>(shared_from_this(), context, kernel, worker, op));
		}
	}
	SetTasks(std::move(tasks));
}

void PhasedKernelEvent::FinishEvent() {
	if (running && kernel->AdvancePhase()) {
		// The inserted event takes over this event's dependents and runs after it
		InsertEvent(make_shared_ptr<PhasedKernelEvent>(kernel, *pipeline, op, false));
		return;
	}
	kernel->Finished();
}

} // namespace duckdb
