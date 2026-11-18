//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/utils/managed_array.hpp
//
// A contiguous array whose memory can come from DuckDB's buffer manager, and
// the accounting of heap memory against the buffer pool.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckpgq/common.hpp"

#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <new>
#include <type_traits>

namespace duckdb {

//! A contiguous array of trivially copyable values with the part of std::vector's interface the CSR code uses.
//!
//! Without a buffer manager the memory comes from the heap, as with std::vector (the CSR built per query, the
//! overlay of a snapshot). With one (the arrays of a CSR index epoch) every allocation is a buffer from DuckDB's
//! buffer manager under MemoryTag::EXTENSION that stays pinned while the array holds it: the memory counts against
//! memory_limit and shows in duckdb_memory(), it is never evicted or spilled, and the data pointer is stable, so a
//! kernel reads it exactly as it reads a heap array. An allocation that does not fit the limit after other blocks
//! were evicted throws DuckDB's OutOfMemoryException and leaves the array unchanged (assign leaves it empty).
//!
//! The buffer manager must outlive the array: pass the database's (BufferManager::GetBufferManager(DatabaseInstance
//! &)), not the one of a ClientContext, which is a wrapper that dies with the connection.
template <class T>
class ManagedArray {
	static_assert(std::is_trivially_copyable<T>::value, "ManagedArray moves its elements with memcpy");
	static_assert(alignof(T) <= sizeof(idx_t), "the data of a managed buffer starts on an 8-byte boundary");

public:
	using value_type = T;
	using iterator = T *;
	using const_iterator = const T *;

	ManagedArray() = default;
	explicit ManagedArray(optional_ptr<BufferManager> buffer_manager_p) : buffer_manager(buffer_manager_p) {
	}
	~ManagedArray() {
		FreeStorage();
	}
	ManagedArray(const ManagedArray &) = delete;
	ManagedArray &operator=(const ManagedArray &) = delete;
	ManagedArray(ManagedArray &&other) noexcept {
		MoveFrom(other);
	}
	//! Takes the other array's memory and its buffer manager
	ManagedArray &operator=(ManagedArray &&other) noexcept {
		if (this != &other) {
			FreeStorage();
			MoveFrom(other);
		}
		return *this;
	}

public:
	T *data() {
		return data_ptr;
	}
	const T *data() const {
		return data_ptr;
	}
	idx_t size() const {
		return count;
	}
	idx_t capacity() const {
		return capacity_count;
	}
	bool empty() const {
		return count == 0;
	}
	T &operator[](idx_t index) {
		D_ASSERT(index < count);
		return data_ptr[index];
	}
	const T &operator[](idx_t index) const {
		D_ASSERT(index < count);
		return data_ptr[index];
	}
	T &back() {
		D_ASSERT(count > 0);
		return data_ptr[count - 1];
	}
	const T &back() const {
		D_ASSERT(count > 0);
		return data_ptr[count - 1];
	}
	iterator begin() {
		return data_ptr;
	}
	const_iterator begin() const {
		return data_ptr;
	}
	iterator end() {
		return data_ptr + count;
	}
	const_iterator end() const {
		return data_ptr + count;
	}

	//! Drops the elements and keeps the memory, like std::vector
	void clear() {
		count = 0;
	}
	void reserve(idx_t new_capacity) {
		if (new_capacity > capacity_count) {
			Reallocate(new_capacity);
		}
	}
	//! New elements are zero
	void resize(idx_t new_count) {
		if (new_count > capacity_count) {
			Reallocate(GrowthCapacity(new_count));
		}
		if (new_count > count) {
			memset(static_cast<void *>(data_ptr + count), 0, (new_count - count) * sizeof(T));
		}
		count = new_count;
	}
	void push_back(const T &value) {
		// Copied first: `value` may be an element of this array, which the reallocation moves
		T copy = value;
		if (count == capacity_count) {
			Reallocate(GrowthCapacity(count + 1));
		}
		data_ptr[count++] = copy;
	}
	//! Appends [first, last); `position` must be end(), the only insertion the CSR code makes
	template <class ITERATOR>
	void insert(const_iterator position, ITERATOR first, ITERATOR last) {
		D_ASSERT(position == end());
		(void)position;
		auto added = static_cast<idx_t>(std::distance(first, last));
		if (count + added > capacity_count) {
			Reallocate(GrowthCapacity(count + added));
		}
		std::copy(first, last, data_ptr + count);
		count += added;
	}
	//! Replaces the contents by [first, last), which must not be part of this array, in memory of exactly that size
	//! unless the array already holds more. The old memory is returned before the new is allocated, so an
	//! allocation that fails leaves the array empty.
	template <class ITERATOR>
	void assign(ITERATOR first, ITERATOR last) {
		auto new_count = static_cast<idx_t>(std::distance(first, last));
		count = 0;
		if (new_count > capacity_count) {
			FreeStorage();
			Reallocate(new_count);
		}
		std::copy(first, last, data_ptr);
		count = new_count;
	}
	void shrink_to_fit() {
		if (count < capacity_count) {
			Reallocate(count);
		}
	}

	//! Drops the elements and returns the memory
	void Release() {
		FreeStorage();
		count = 0;
	}
	//! Whether the memory comes from the buffer manager
	bool IsManaged() const {
		return buffer_manager;
	}
	//! Bytes of the allocation (a managed buffer is rounded up to 4 KiB by the buffer manager on top of this)
	idx_t AllocatedBytes() const {
		return capacity_count * sizeof(T);
	}

private:
	//! The capacity for an array that must hold `required` elements: exact for the first allocation (resize and
	//! assign on an empty array allocate what they need), doubling afterwards
	idx_t GrowthCapacity(idx_t required) const {
		if (capacity_count == 0) {
			return required;
		}
		return std::max<idx_t>(required, 2 * capacity_count);
	}

	//! Moves the elements into memory for `new_capacity` elements. The new memory is allocated before the old is
	//! returned, so a failed allocation leaves the array as it was.
	void Reallocate(idx_t new_capacity) {
		D_ASSERT(new_capacity >= count);
		if (new_capacity == capacity_count) {
			return;
		}
		if (new_capacity == 0) {
			FreeStorage();
			return;
		}
		auto bytes = new_capacity * sizeof(T);
		BufferHandle new_handle;
		T *new_data;
		if (buffer_manager) {
			// Destroyable: the buffer is unpinned only when the array lets go of it, and its contents are then
			// dropped instead of being written to the temporary directory
			new_handle = buffer_manager->Allocate(MemoryTag::EXTENSION, bytes, true);
			new_data = reinterpret_cast<T *>(new_handle.GetDataMutable());
		} else {
			new_data = static_cast<T *>(::operator new(bytes));
		}
		if (count > 0) {
			memcpy(static_cast<void *>(new_data), static_cast<const void *>(data_ptr), count * sizeof(T));
		}
		FreeStorage();
		handle = std::move(new_handle);
		data_ptr = new_data;
		capacity_count = new_capacity;
	}

	void FreeStorage() {
		if (!data_ptr) {
			return;
		}
		if (handle.IsValid()) {
			handle.Destroy();
		} else {
			::operator delete(data_ptr);
		}
		data_ptr = nullptr;
		capacity_count = 0;
	}

	void MoveFrom(ManagedArray &other) {
		buffer_manager = other.buffer_manager;
		handle = std::move(other.handle);
		data_ptr = other.data_ptr;
		count = other.count;
		capacity_count = other.capacity_count;
		other.data_ptr = nullptr;
		other.count = 0;
		other.capacity_count = 0;
	}

private:
	optional_ptr<BufferManager> buffer_manager;
	//! The pinned buffer of a managed array; invalid while the array is empty or lives on the heap
	BufferHandle handle;
	T *data_ptr = nullptr;
	idx_t count = 0;
	idx_t capacity_count = 0;
};

//! Heap memory that DuckDB's buffer pool accounts for under MemoryTag::EXTENSION: the parts of the CSR index that
//! cannot live in a buffer (hash maps, the change log that grows with every commit) or that a read query builds (the
//! overlay of a snapshot). The memory counts against memory_limit and shows in duckdb_memory(); it is released by
//! Reset or the destructor.
//!
//! Growth never fails: it first reserves the difference through the buffer manager, which evicts other blocks to make
//! room, and when that is not possible the difference is accounted unconditionally (the memory is already in use), so
//! the pool exceeds the limit by it. A commit or a read query is therefore not failed for this memory; what it tracks
//! is bounded by the fold ratio.
class MemoryReservation {
public:
	MemoryReservation() = default;
	~MemoryReservation() {
		Reset();
	}
	MemoryReservation(const MemoryReservation &) = delete;
	MemoryReservation &operator=(const MemoryReservation &) = delete;

	//! Sets the accounted size; `manager` must outlive the reservation (the database's buffer manager)
	void Set(BufferManager &manager, idx_t bytes);
	void Reset();
	idx_t Bytes() const {
		return reserved;
	}

private:
	optional_ptr<BufferManager> buffer_manager;
	idx_t reserved = 0;
};

} // namespace duckdb
