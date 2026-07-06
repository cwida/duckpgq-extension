#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
namespace duckpgq_peg {

struct TransformResultValue {
	virtual ~TransformResultValue() = default;
};

template <class T>
struct TypedTransformResult : public TransformResultValue {
	explicit TypedTransformResult(T value_p) : value(std::move(value_p)) {
	}
	T value;
};

} // namespace duckpgq_peg
} // namespace duckdb
