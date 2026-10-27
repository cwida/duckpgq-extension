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
	bool continues = running && kernel->AdvancePhase();
	if (!continues) {
		kernel->Finished();
	} else {
		// The inserted event takes over this event's dependents (it moves the parents list) and runs after it
		auto next = make_shared_ptr<PhasedKernelEvent>(kernel, *pipeline, op, false);
		next->SetCompletion(completion);
		InsertEvent(std::move(next));
	}
	// Kernels spawned by this phase run alongside. An event added at runtime is only ever scheduled through a
	// dependency that completes, so each spawned event depends on this one (registered after InsertEvent moved the
	// chain's parents away, so this event itself completes it). The completion event waits for it too; this event
	// is not finished yet, so the completion event cannot be scheduled while its dependency count grows.
	vector<shared_ptr<PhasedKernel>> spawned;
	kernel->CollectSpawned(spawned);
	for (auto &spawned_kernel : spawned) {
		auto spawned_event = make_shared_ptr<PhasedKernelEvent>(std::move(spawned_kernel), *pipeline, op);
		spawned_event->SetCompletion(completion);
		spawned_event->AddDependency(*this);
		if (completion) {
			completion->AddDependency(*spawned_event);
		}
		executor.AddEvent(std::move(spawned_event));
	}
}

} // namespace duckdb
