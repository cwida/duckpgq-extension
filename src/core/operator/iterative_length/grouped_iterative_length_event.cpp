#include "duckpgq/core/operator/iterative_length/grouped_iterative_length_event.hpp"

#include "duckpgq/core/operator/iterative_length/iterative_length_kernel.hpp"
#include "duckpgq/core/operator/physical_path_finding_operator.hpp"

#include <duckpgq/core/option/duckpgq_option.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>

namespace duckdb {

namespace {

static mutex grouped_bfs_phase_timing_lock;

static size_t GetLocalCSREdgeCount(const std::vector<shared_ptr<LocalCSR>> &partition_csrs) {
	size_t edge_count = 0;
	for (const auto &local_csr : partition_csrs) {
		edge_count += local_csr->GetEdgeSize();
	}
	return edge_count;
}

static void AppendBFSPhaseTiming(const IterativeLengthState &state, double time_ms) {
	auto file_name = state.benchmark_output_prefix + "_phase_timing.csv";
	lock_guard<mutex> lock(grouped_bfs_phase_timing_lock);
	bool write_header = !std::filesystem::exists(file_name);
	std::ofstream outfile(file_name, std::ios::app);
	if (!outfile.is_open()) {
		throw IOException("Could not open path-finding phase benchmark file \"%s\"", file_name);
	}
	if (write_header) {
		outfile << "Phase,RunID,ThreadCount,PairCount,VertexCount,EdgeCount,PartitionCount,Time_ms,MemoryBytes\n";
	}
	outfile << "bfs_batch_grouped," << state.benchmark_run_id << "," << state.num_threads << "," << state.pairs->size()
	        << "," << state.v_size - 2 << "," << GetLocalCSREdgeCount(state.local_csrs) << ","
	        << state.local_csrs.size() << "," << time_ms << ",0\n";
}

} // namespace

GroupedIterativeLengthGroupState::GroupedIterativeLengthGroupState(idx_t worker_count)
    : barrier(make_uniq<Barrier>(worker_count)) {
}

GroupedIterativeLengthTask::GroupedIterativeLengthTask(shared_ptr<Event> event_p, ClientContext &context_p,
                                                       GroupedIterativeLengthEvent &grouped_event_p, idx_t group_id_p,
                                                       idx_t worker_id_p, const PhysicalOperator &op_p)
    : ExecutorTask(context_p, std::move(event_p), op_p), context(context_p), grouped_event(grouped_event_p),
      group_id(group_id_p), worker_id(worker_id_p) {
}

TaskExecutionResult GroupedIterativeLengthTask::ExecuteTask(TaskExecutionMode mode) {
	auto &group = grouped_event.Group(group_id);
	while (true) {
		if (worker_id == 0) {
			group.current_state = grouped_event.ClaimNextBatch();
		}
		group.barrier->Wait(worker_id);

		auto current_state = group.current_state;
		if (!current_state) {
			break;
		}

		ExecuteIterativeLengthBatch(*current_state, worker_id);
		if (worker_id == 0) {
			grouped_event.FinishBatch(*current_state);
		}
		group.barrier->Wait(worker_id);
	}

	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

GroupedIterativeLengthEvent::GroupedIterativeLengthEvent(vector<shared_ptr<IterativeLengthState>> states_p,
                                                         idx_t workers_per_group_p, idx_t group_count_p,
                                                         Pipeline &pipeline_p, const PhysicalPathFinding &op_p)
    : BasePipelineEvent(pipeline_p), states(std::move(states_p)), next_batch(0), workers_per_group(workers_per_group_p),
      group_count(group_count_p), op(op_p) {
	D_ASSERT(workers_per_group > 0);
	D_ASSERT(group_count > 0);
}

void GroupedIterativeLengthEvent::Schedule() {
	auto &context = pipeline->GetClientContext();
	vector<shared_ptr<Task>> bfs_tasks;
	groups.reserve(group_count);
	bfs_tasks.reserve(group_count * workers_per_group);

	for (idx_t group_id = 0; group_id < group_count; group_id++) {
		groups.push_back(make_uniq<GroupedIterativeLengthGroupState>(workers_per_group));
		for (idx_t worker_id = 0; worker_id < workers_per_group; worker_id++) {
			bfs_tasks.push_back(
			    make_uniq<GroupedIterativeLengthTask>(shared_from_this(), context, *this, group_id, worker_id, op));
		}
	}

	SetTasks(std::move(bfs_tasks));
}

void GroupedIterativeLengthEvent::FinishEvent() {
}

shared_ptr<IterativeLengthState> GroupedIterativeLengthEvent::ClaimNextBatch() {
	auto batch_idx = next_batch.fetch_add(1);
	if (batch_idx >= states.size()) {
		return nullptr;
	}

	auto state = states[batch_idx];
	state->phase_start_time = std::chrono::steady_clock::now();
	state->tasks_scheduled = workers_per_group;
	state->barrier = make_uniq<Barrier>(workers_per_group);
	return state;
}

void GroupedIterativeLengthEvent::FinishBatch(IterativeLengthState &state) {
	if (!state.benchmark_enabled) {
		return;
	}

	auto phase_end_time = std::chrono::steady_clock::now();
	auto time_ms = std::chrono::duration<double, std::milli>(phase_end_time - state.phase_start_time).count();
	AppendBFSPhaseTiming(state, time_ms);

	auto heavy_partition_fraction = std::to_string(GetHeavyPartitionFraction(state.context));
	auto light_partition_multiplier = std::to_string(GetLightPartitionMultiplier(state.context));
	auto file_name = state.benchmark_output_prefix + "_explore_timing_" + state.benchmark_run_id + "_threads_" +
	                 std::to_string(state.num_threads) + "_" + heavy_partition_fraction + "_" +
	                 light_partition_multiplier + ".csv";
	state.WriteTimingResults(file_name);
}

idx_t GroupedIterativeLengthEvent::WorkersPerGroup() const {
	return workers_per_group;
}

GroupedIterativeLengthGroupState &GroupedIterativeLengthEvent::Group(idx_t group_id) {
	D_ASSERT(group_id < groups.size());
	return *groups[group_id];
}

} // namespace duckdb
