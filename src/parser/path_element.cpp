#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckpgq/parser/path_element.hpp"
#include "duckpgq/parser/path_reference.hpp"

namespace duckdb {

bool PathElement::Equals(const PathReference *other_p) const {
	if (!PathReference::Equals(other_p)) {
		return false;
	}
	auto other = dynamic_cast<const PathElement *>(other_p);
	if (!other) {
		return false;
	}
	if (match_type != other->match_type) {
		return false;
	}
	if (label != other->label) {
		return false;
	}
	if (variable_binding != other->variable_binding) {
		return false;
	}
	if (anonymous != other->anonymous) {
		return false;
	}
	if (!PGQLabelExpression::Equals(label_expression, other->label_expression)) {
		return false;
	}

	return true;
}

void PathElement::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "match_type", uint8_t(match_type));
	serializer.WriteProperty(101, "label", label);
	serializer.WriteProperty(102, "variable_binding", variable_binding);
	serializer.WritePropertyWithDefault(103, "anonymous", anonymous, false);
	serializer.WritePropertyWithDefault(104, "label_expression", label_expression);
}

unique_ptr<PathReference> PathElement::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<PathElement>(PGQPathReferenceType::PATH_ELEMENT);
	uint8_t match_type = uint8_t(PGQMatchType::MATCH_VERTEX);
	deserializer.ReadProperty(100, "match_type", match_type);
	result->match_type = PGQMatchType(match_type);
	deserializer.ReadProperty(101, "label", result->label);
	deserializer.ReadProperty(102, "variable_binding", result->variable_binding);
	result->anonymous = deserializer.ReadPropertyWithExplicitDefault<bool>(103, "anonymous", false);
	result->label_expression = deserializer.ReadPropertyWithDefault<unique_ptr<PGQLabelExpression>>(104, "label_expression");
	return std::move(result);
}

unique_ptr<PathReference> PathElement::Copy() {
	auto result = make_uniq<PathElement>(PGQPathReferenceType::PATH_ELEMENT);
	result->path_reference_type = path_reference_type;
	result->match_type = match_type;
	result->label = label;
	result->variable_binding = variable_binding;
	result->anonymous = anonymous;
	if (label_expression) {
		result->label_expression = label_expression->Copy();
	}
	return std::move(result);
}

string PathElement::FillerToString() const {
	string result = anonymous ? "" : variable_binding;
	if (label_expression) {
		result += ":" + label_expression->ToString();
	} else if (!label.empty()) {
		result += ":" + label;
	}
	return result;
}

string PathElement::ToString() const {
	auto filler = FillerToString();
	switch (match_type) {
	case PGQMatchType::MATCH_VERTEX:
		return "(" + filler + ")";
	case PGQMatchType::MATCH_EDGE_ANY:
		return "-[" + filler + "]-";
	case PGQMatchType::MATCH_EDGE_LEFT:
		return "<-[" + filler + "]-";
	case PGQMatchType::MATCH_EDGE_RIGHT:
		return "-[" + filler + "]->";
	case PGQMatchType::MATCH_EDGE_LEFT_RIGHT:
		return "<-[" + filler + "]->";
	}
	return "";
}

} // namespace duckdb
