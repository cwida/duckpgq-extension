#include "duckpgq/core/utils/managed_array.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {

void MemoryReservation::Set(BufferManager &manager, idx_t bytes) {
	buffer_manager = &manager;
	if (bytes > reserved) {
		auto growth = bytes - reserved;
		try {
			// Evicts other blocks until the growth fits the limit
			manager.ReserveMemory(growth);
		} catch (const OutOfMemoryException &) {
			manager.GetBufferPool().UpdateUsedMemory(MemoryTag::EXTENSION, NumericCast<int64_t>(growth));
		}
	} else if (bytes < reserved) {
		manager.FreeReservedMemory(reserved - bytes);
	}
	reserved = bytes;
}

void MemoryReservation::Reset() {
	if (reserved > 0) {
		buffer_manager->FreeReservedMemory(reserved);
		reserved = 0;
	}
}

} // namespace duckdb
