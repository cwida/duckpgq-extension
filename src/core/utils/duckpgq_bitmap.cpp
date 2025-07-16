#include "duckpgq/core/utils/duckpgq_bitmap.hpp"

namespace duckdb {

DuckPGQBitmap::DuckPGQBitmap(size_t size) {
	bitmap.resize((size + 63) / 64, 0);
}

void DuckPGQBitmap::set(size_t index) {
	bitmap[index / 64] |= (1ULL << (index % 64));
}

bool DuckPGQBitmap::test(size_t index) const {
	return (bitmap[index / 64] & (1ULL << (index % 64))) != 0;
}

void DuckPGQBitmap::reset() {
	fill(bitmap.begin(), bitmap.end(), 0);
}

} // namespace duckdb
