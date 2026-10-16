#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

enum class PGQLabelExpressionType : uint8_t { LABEL = 0, WILDCARD = 1, NEGATION = 2, CONJUNCTION = 3, DISJUNCTION = 4 };

//! A <label expression> (SQL/PGQ 10.8): label names combined with |, &, ! and %, kept as a tree so the binder can
//! evaluate it against the labels of each vertex or edge table.
class PGQLabelExpression {
public:
	explicit PGQLabelExpression(PGQLabelExpressionType type) : type(type) {
	}

	PGQLabelExpressionType type;
	//! The label name for LABEL nodes.
	string label;
	//! One child for NEGATION, two or more for CONJUNCTION and DISJUNCTION.
	vector<unique_ptr<PGQLabelExpression>> children;

public:
	static unique_ptr<PGQLabelExpression> Label(const string &label);

	string ToString() const;
	unique_ptr<PGQLabelExpression> Copy() const;
	bool Equals(const PGQLabelExpression &other) const;
	static bool Equals(const unique_ptr<PGQLabelExpression> &left, const unique_ptr<PGQLabelExpression> &right);

	//! Collect every label name mentioned in the expression.
	void CollectLabels(vector<string> &labels) const;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<PGQLabelExpression> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
