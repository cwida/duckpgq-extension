#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/local_csr/local_csr_state.hpp"
#include "duckpgq/core/operator/local_csr/local_csr_task.hpp"
#include "duckpgq/core/operator/phased_kernel_event.hpp"

#include <chrono>
#include <functional>

namespace duckdb {

//! Builds the partitioned CSR (forward, reverse and pull variants) as a phased kernel: a list of steps, each with a
//! serial part before, a parallel part run by every worker, and a serial part after.
class LocalCSRBuildKernel : public PhasedKernel {
public:
	explicit LocalCSRBuildKernel(shared_ptr<LocalCSRState> state_p);

	bool Start() override;
	idx_t WorkerCount() const override;
	void RunPhase(idx_t worker_id) override;
	bool AdvancePhase() override;
	void Finished() override;

private:
	struct Step {
		std::function<void()> before;
		std::function<void(LocalCSRWorker &)> parallel;
		std::function<void()> after;
	};
	void AddStep(std::function<void()> before, std::function<void(LocalCSRWorker &)> parallel,
	             std::function<void()> after);
	void PlanDenseBuild(bool reverse);
	void PlanSparseForwardBuild();
	void PlanPullBuild();

	shared_ptr<LocalCSRState> state;
	vector<unique_ptr<LocalCSRWorker>> workers;
	vector<Step> steps;
	idx_t step = 0;
	std::chrono::steady_clock::time_point subphase_start;
};

} // namespace duckdb
