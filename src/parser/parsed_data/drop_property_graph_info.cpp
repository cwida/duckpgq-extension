#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckpgq/parser/parsed_data/drop_property_graph_info.hpp"

namespace duckdb {

DropPropertyGraphInfo::DropPropertyGraphInfo() : DropInfo() {
	type = CatalogType::INVALID;
}

DropPropertyGraphInfo::DropPropertyGraphInfo(string property_graph_name, bool missing_ok)
    : DropInfo(), property_graph_name(std::move(property_graph_name)), missing_ok(missing_ok) {
	type = CatalogType::INVALID;
}

unique_ptr<DropInfo> DropPropertyGraphInfo::Copy() const {
	auto result = make_uniq<DropPropertyGraphInfo>(property_graph_name, missing_ok);
	return std::move(result);
}

void DropPropertyGraphInfo::Serialize(Serializer &serializer) const {
	serializer.WriteProperty<string>(100, "property_graph_name", property_graph_name);
	serializer.WriteProperty<bool>(101, "missing_ok", missing_ok);
}

unique_ptr<DropInfo> DropPropertyGraphInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<DropPropertyGraphInfo>();
	deserializer.ReadProperty<string>(100, "property_graph_name", result->property_graph_name);
	deserializer.ReadProperty<bool>(101, "missing_ok", result->missing_ok);
	return std::move(result);
}

} // namespace duckdb
