#pragma once
#include <duckdb/planner/operator/logical_extension_operator.hpp>

namespace duckdb {

class LogicalPathFindingOperator : public LogicalExtensionOperator {
public:
  explicit LogicalPathFindingOperator(vector<unique_ptr<LogicalOperator>> &children_, vector<unique_ptr<Expression>> &expressions_, const string& mode_)
      : LogicalExtensionOperator(std::move(expressions_)) {
    children = std::move(children_);
    mode = mode_;
  }

  void Serialize(Serializer &serializer) const override {
    throw InternalException("Path Finding Operator should not be serialized");
  }

  unique_ptr<PhysicalOperator>
  CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override;

  vector<ColumnBinding> GetColumnBindings() override;

  std::string GetName() const override { return "PATH_FINDING"; }

  void ResolveTypes() override;

  string ParamsToString() const override;

public:
  string mode;
};

} // namespace duckdb
