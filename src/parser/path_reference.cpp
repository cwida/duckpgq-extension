
#include "duckpgq/parser/path_element.hpp"
#include "duckpgq/parser/path_reference.hpp"
#include "duckpgq/parser/subpath_element.hpp"

namespace duckdb {

bool PathReference::Equals(const PathReference *other_p) const {
	if (!other_p) {
		return false;
	}
	if (path_reference_type != other_p->path_reference_type) {
		return false;
	}
	return true;
}
void PathReference::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "path_reference_type", uint8_t(path_reference_type));
}

unique_ptr<PathReference> PathReference::Deserialize(Deserializer &deserializer) {
	unique_ptr<PathReference> result;
	uint8_t path_reference_type_value = uint8_t(PGQPathReferenceType::UNKNOWN);
	deserializer.ReadProperty(100, "path_reference_type", path_reference_type_value);
	auto path_reference_type = PGQPathReferenceType(path_reference_type_value);
	switch (path_reference_type) {
	case PGQPathReferenceType::PATH_ELEMENT:
		result = PathElement::Deserialize(deserializer);
		break;
	case PGQPathReferenceType::SUBPATH:
		result = SubPath::Deserialize(deserializer);
		break;
	default:
		throw InternalException("Unknown path reference type in deserializer.");
	}
	result->path_reference_type = path_reference_type;
	return result;
}

} // namespace duckdb
