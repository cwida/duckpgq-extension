#pragma once

#include "duckdb/common/vector.hpp"
#include "duckpgq/parser/path_pattern.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {

class MatchExpression : public ParsedExpression {
public:
	// An extension-only placeholder: the GRAPH_TABLE clause is carried in TableFunctionRef::bind_info and is never
	// bound as an expression, so it needs no real expression class.
	static constexpr const ExpressionClass TYPE = ExpressionClass::INVALID;

public:
	MatchExpression() : ParsedExpression(ExpressionType::FUNCTION_REF, TYPE) {
	}

	string pg_name;
	//! Catalog and schema the graph was referenced with; empty when unqualified.
	string pg_catalog;
	string pg_schema;
	string alias;
	vector<unique_ptr<PathPattern>> path_patterns;

	vector<unique_ptr<ParsedExpression>> column_list;

	unique_ptr<ParsedExpression> where_clause;

public:
	string ToString() const override;
	bool Equals(const BaseExpression &other_p) const override;

	unique_ptr<ParsedExpression> Copy() const override;

	//! Serializes a blob into a MatchRef
	void Serialize(Serializer &serializer) const override;
	//! Deserializes a blob back into a MatchRef
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
