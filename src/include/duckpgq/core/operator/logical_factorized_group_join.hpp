//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/operator/logical_factorized_group_join.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

//! Integer types whose values fit a BIGINT, usable as multiplicity weights and as directory keys
inline bool FactorizedKeyFitsInBigint(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return true;
	default:
		return false;
	}
}

//! Joins two inputs on equal keys without expanding the matches: both inputs are aggregated per key into one hash
//! table, and every key present on both sides is emitted once with the multiplicity of each side.
class LogicalFactorizedGroupJoin : public LogicalExtensionOperator {
public:
	static constexpr const char *IDENTIFIER = "duckpgq_factorized_group_join";
	static constexpr idx_t SIDES = 2;

	//! `expressions` holds the left keys, the right keys, and then the optional weight of each side
	LogicalFactorizedGroupJoin(TableIndex group_index, TableIndex aggregate_index,
	                           vector<unique_ptr<Expression>> left_keys, vector<unique_ptr<Expression>> right_keys,
	                           unique_ptr<Expression> left_weight, unique_ptr<Expression> right_weight);

	//! Table index of the key columns
	TableIndex group_index;
	//! Table index of the two multiplicity columns
	TableIndex aggregate_index;
	idx_t key_count;
	bool has_weight[SIDES];
	//! BIGINT for a plain count, HUGEINT when the rows of that side carry a weight
	LogicalType multiplicity_types[SIDES];
	//! When the single integer key has known bounds, counts live in a dense array instead of a hash table
	Value directory_min;
	idx_t directory_range = 0;

public:
	const Expression &Key(idx_t side, idx_t i) const {
		return *expressions[side * key_count + i];
	}
	optional_ptr<const Expression> Weight(idx_t side) const;
	bool UsesDirectory() const {
		return directory_range > 0;
	}
	//! Position of a side's weight in `expressions`
	idx_t WeightIndex(idx_t side) const;

	vector<ColumnBinding> GetColumnBindings() override;
	vector<TableIndex> GetTableIndex() const override;
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	string GetExtensionName() const override;
	optional_ptr<const string> GetTypeBindingVerificationIdentifier() const noexcept override;
	void ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) override;
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	bool SupportSerialization() const override {
		return false;
	}
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
