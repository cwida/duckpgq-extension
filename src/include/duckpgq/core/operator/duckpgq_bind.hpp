#pragma once

#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

BoundStatement duckpgq_bind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                            SQLStatement &statement);

struct DuckPGQOperatorExtension : public OperatorExtension {
	DuckPGQOperatorExtension() : OperatorExtension() {
		Bind = duckpgq_bind;
	}

	std::string GetName() override {
		return "duckpgq_bind";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override {
		throw InternalException("DuckPGQ operator should not be serialized");
	}
};

} // namespace core

} // namespace duckpgq
