#pragma once

#include "duckdb/common/common.hpp"
#include "duckpgq/parser/label_expression.hpp"
#include "duckpgq/parser/path_reference.hpp"

namespace duckdb {

enum class PGQMatchType : uint8_t {
	MATCH_VERTEX = 0,
	MATCH_EDGE_ANY = 1,
	MATCH_EDGE_LEFT = 2,
	MATCH_EDGE_RIGHT = 3,
	MATCH_EDGE_LEFT_RIGHT = 4
};

class PathElement : public PathReference {
public:
	PGQMatchType match_type;

	//! The label the element binds to. For a compound label expression it is empty until the binder resolves it.
	std::string label;

	//! A compound <label expression> (10.8); null when the element has at most a single label name.
	unique_ptr<PGQLabelExpression> label_expression;

	std::string variable_binding;

	//! True for () and -[]->: the variable name is generated and the element is not exposed by COLUMNS.
	bool anonymous = false;

public:
	explicit PathElement(PGQPathReferenceType path_reference_type) : PathReference(path_reference_type) {
	}

	string ToString() const override;

	//! The <element pattern filler>: variable (unless anonymous) and label, without the brackets.
	string FillerToString() const;

	unique_ptr<PathReference> Copy() override;

	bool Equals(const PathReference *other_p) const override;

	void Serialize(Serializer &serializer) const override;

	static unique_ptr<PathReference> Deserialize(Deserializer &deserializer);
};
} // namespace duckdb
