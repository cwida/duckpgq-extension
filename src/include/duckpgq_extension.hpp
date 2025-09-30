#pragma once

#include "duckpgq/common.hpp"

namespace duckdb {

class DuckpgqExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
};

} // namespace duckdb
