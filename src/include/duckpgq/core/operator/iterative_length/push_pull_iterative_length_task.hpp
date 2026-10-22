#pragma once

#include "duckpgq/common.hpp"
#include "duckpgq/core/operator/iterative_length/push_pull_iterative_length_state.hpp"

namespace duckdb {

//! The per-worker phases of the push/pull MS-BFS; the state (a PhasedKernel) sequences them
class PushPullIterativeLengthWorker {
public:
	PushPullIterativeLengthWorker(shared_ptr<PushPullIterativeLengthState> state_p, idx_t worker_id_p);

	//! Parallel phases
	void PreparePhase();
	void PushExplore(idx_t iteration);
	void PushCheck(idx_t iteration);
	void Pull(idx_t iteration);
	//! Serial steps, run through worker 0
	void DecideMode();
	void ReachDetect() const;
	void UnReachableSet() const;
	void RecordPhaseTiming(const char *phase, const char *mode, idx_t iteration, idx_t partition_count, idx_t vertices,
	                       idx_t edges, idx_t candidates, idx_t changed_vertices, double time_ms) const;

private:
	idx_t CheckChange(std::bitset<LANE_LIMIT> *seen, std::bitset<LANE_LIMIT> *next, shared_ptr<LocalCSR> &local_csr,
	                  idx_t &candidate_vertices) const;
	idx_t CountFrontierVertices(const std::bitset<LANE_LIMIT> *visit, idx_t start_vertex, idx_t end_vertex) const;
	idx_t Explore(const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next, const LocalCSR &local_csr);
	idx_t RunExplore(const std::bitset<LANE_LIMIT> *visit, std::bitset<LANE_LIMIT> *next, const LocalCSR &local_csr);

	shared_ptr<PushPullIterativeLengthState> state;
	idx_t worker_id;
};

} // namespace duckdb
