// #pragma once
//
//
// #include <duckdb/parallel/base_pipeline_event.hpp>
// #include <duckpgq/core/operator/physical_path_finding_operator.hpp>
// #include <duckpgq/core/operator/task/iterative_length_task.hpp>
//
// namespace duckpgq {
// namespace core {
//
// class ParallelIterativeEvent : public BasePipelineEvent {
// public:
//   ParallelIterativeEvent(GlobalBFSState &gstate_p, Pipeline &pipeline_p, const PhysicalPathFinding &op_p);
//
//   GlobalBFSState &gstate;
//   const PhysicalPathFinding &op;
//
//   void Schedule() override;
//
//   void FinishEvent() override;
// };
//
// } // namespace core
// } // namespace duckpgq
