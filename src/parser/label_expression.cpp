#include "duckpgq/parser/label_expression.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<PGQLabelExpression> PGQLabelExpression::Label(const string &label) {
	auto result = make_uniq<PGQLabelExpression>(PGQLabelExpressionType::LABEL);
	result->label = label;
	return result;
}

static string ChildToString(const PGQLabelExpression &parent, const PGQLabelExpression &child) {
	bool compound =
	    child.type == PGQLabelExpressionType::CONJUNCTION || child.type == PGQLabelExpressionType::DISJUNCTION;
	if (compound && child.type != parent.type) {
		return "(" + child.ToString() + ")";
	}
	return child.ToString();
}

string PGQLabelExpression::ToString() const {
	switch (type) {
	case PGQLabelExpressionType::LABEL:
		return label;
	case PGQLabelExpressionType::WILDCARD:
		return "%";
	case PGQLabelExpressionType::NEGATION:
		return "!" + ChildToString(*this, *children[0]);
	case PGQLabelExpressionType::CONJUNCTION:
	case PGQLabelExpressionType::DISJUNCTION: {
		vector<string> parts;
		for (auto &child : children) {
			parts.push_back(ChildToString(*this, *child));
		}
		return StringUtil::Join(parts, type == PGQLabelExpressionType::CONJUNCTION ? "&" : "|");
	}
	}
	return "";
}

unique_ptr<PGQLabelExpression> PGQLabelExpression::Copy() const {
	auto result = make_uniq<PGQLabelExpression>(type);
	result->label = label;
	for (auto &child : children) {
		result->children.push_back(child->Copy());
	}
	return result;
}

bool PGQLabelExpression::Equals(const PGQLabelExpression &other) const {
	if (type != other.type || !StringUtil::CIEquals(label, other.label) || children.size() != other.children.size()) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!children[i]->Equals(*other.children[i])) {
			return false;
		}
	}
	return true;
}

bool PGQLabelExpression::Equals(const unique_ptr<PGQLabelExpression> &left,
                                const unique_ptr<PGQLabelExpression> &right) {
	if (!left || !right) {
		return !left && !right;
	}
	return left->Equals(*right);
}

void PGQLabelExpression::CollectLabels(vector<string> &labels) const {
	if (type == PGQLabelExpressionType::LABEL) {
		labels.push_back(label);
	}
	for (auto &child : children) {
		child->CollectLabels(labels);
	}
}

void PGQLabelExpression::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "type", uint8_t(type));
	serializer.WritePropertyWithDefault(101, "label", label, string());
	serializer.WritePropertyWithDefault(102, "children", children);
}

unique_ptr<PGQLabelExpression> PGQLabelExpression::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<uint8_t>(100, "type");
	auto result = make_uniq<PGQLabelExpression>(PGQLabelExpressionType(type));
	result->label = deserializer.ReadPropertyWithExplicitDefault<string>(101, "label", string());
	result->children = deserializer.ReadPropertyWithDefault<vector<unique_ptr<PGQLabelExpression>>>(102, "children");
	return result;
}

} // namespace duckdb
