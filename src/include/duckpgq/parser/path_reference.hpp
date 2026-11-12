#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

enum class PGQPathReferenceType : uint8_t { PATH_ELEMENT = 0, SUBPATH = 1, UNKNOWN = 2 };

class PathReference {
public:
	PGQPathReferenceType path_reference_type;

public:
	explicit PathReference(PGQPathReferenceType path_reference_type) : path_reference_type(path_reference_type) {
	}

	virtual ~PathReference() {
	}

	virtual string ToString() const = 0;

	virtual unique_ptr<PathReference> Copy() = 0;

	virtual bool Equals(const PathReference *other_p) const;

	DUCKDB_API virtual void Serialize(Serializer &serializer) const;

	DUCKDB_API static unique_ptr<PathReference> Deserialize(Deserializer &deserializer);

public:
	template <class TARGET>
	TARGET &Cast() {
		if (path_reference_type != TARGET::TYPE) {
			throw InternalException("Failed to cast path reference to type - path reference type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (path_reference_type != TARGET::TYPE) {
			throw InternalException("Failed to cast path reference to type - path reference type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
