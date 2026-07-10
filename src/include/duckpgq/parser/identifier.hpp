//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/parser/identifier.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#if __has_include("duckdb/common/identifier.hpp")
#include "duckdb/common/identifier.hpp"
#else

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/string.hpp"

#include <iosfwd>

namespace duckdb {

class Identifier {
public:
	Identifier() = default;
	Identifier(const char *str) : value(str) {
	}
	explicit Identifier(const string &str) : value(str) {
	}
	explicit Identifier(string &&str) : value(std::move(str)) {
	}

	static Identifier DefaultSchema() {
		return Identifier(DEFAULT_SCHEMA);
	}
	static Identifier InvalidSchema() {
		return Identifier(INVALID_SCHEMA);
	}
	static Identifier InvalidCatalog() {
		return Identifier(INVALID_CATALOG);
	}
	static Identifier SystemCatalog() {
		return Identifier(SYSTEM_CATALOG);
	}
	static Identifier TempCatalog() {
		return Identifier(TEMP_CATALOG);
	}

	explicit operator const string &() const {
		return value;
	}

	const string &GetIdentifierName() const {
		return value;
	}

	bool empty() const {
		return value.empty();
	}
	void clear() {
		value.clear();
	}
	idx_t size() const {
		return value.size();
	}
	const char *c_str() const {
		return value.c_str();
	}
	hash_t Hash() const {
		return StringUtil::CIHash(value);
	}

private:
	string value;
};

inline bool operator==(const Identifier &a, const Identifier &b) {
	return StringUtil::CIEquals(a.GetIdentifierName(), b.GetIdentifierName());
}
inline bool operator==(const Identifier &a, const string &b) {
	return StringUtil::CIEquals(a.GetIdentifierName(), b);
}
inline bool operator==(const string &a, const Identifier &b) {
	return StringUtil::CIEquals(a, b.GetIdentifierName());
}
inline bool operator==(const Identifier &a, const char *b) {
	return StringUtil::CIEquals(a.GetIdentifierName(), b);
}
inline bool operator==(const char *a, const Identifier &b) {
	return StringUtil::CIEquals(a, b.GetIdentifierName());
}
inline bool operator!=(const Identifier &a, const Identifier &b) {
	return !(a == b);
}
inline bool operator!=(const Identifier &a, const string &b) {
	return !(a == b);
}
inline bool operator!=(const string &a, const Identifier &b) {
	return !(a == b);
}
inline bool operator!=(const Identifier &a, const char *b) {
	return !(a == b);
}
inline bool operator!=(const char *a, const Identifier &b) {
	return !(a == b);
}
inline bool operator<(const Identifier &a, const Identifier &b) {
	return StringUtil::Lower(a.GetIdentifierName()) < StringUtil::Lower(b.GetIdentifierName());
}
inline string operator+(const Identifier &a, const string &b) {
	return a.GetIdentifierName() + b;
}
inline string operator+(const string &a, const Identifier &b) {
	return a + b.GetIdentifierName();
}
inline string operator+(const Identifier &a, const char *b) {
	return a.GetIdentifierName() + string(b);
}
inline string operator+(const char *a, const Identifier &b) {
	return string(a) + b.GetIdentifierName();
}

struct IdentifierHashFunction {
	uint64_t operator()(const Identifier &a) const {
		return a.Hash();
	}
};

struct IdentifierEquality {
	bool operator()(const Identifier &a, const Identifier &b) const {
		return a == b;
	}
};

struct IdentifierCompare {
	bool operator()(const Identifier &a, const Identifier &b) const {
		return a < b;
	}
};

} // namespace duckdb

#endif
