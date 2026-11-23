#include "duckpgq/core/operator/iterative_length/flat_csr.hpp"

#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <mutex>

namespace duckdb {

ExploreVariant ParseExploreVariant(const string &name) {
	auto lower = StringUtil::Lower(name);
	if (lower == "partitioned") {
		return ExploreVariant::PARTITIONED;
	}
	if (lower == "flat_serial") {
		return ExploreVariant::FLAT_SERIAL;
	}
	if (lower == "flat_ranges") {
		return ExploreVariant::FLAT_RANGES;
	}
	if (lower == "flat_atomic") {
		return ExploreVariant::FLAT_ATOMIC;
	}
	if (lower == "flat_locked") {
		return ExploreVariant::FLAT_LOCKED;
	}
	throw InvalidInputException("experimental_path_finding_operator_explore_variant must be one of partitioned, "
	                            "flat_serial, flat_ranges, flat_atomic, flat_locked (got \"%s\")",
	                            name);
}

namespace {

//! Calls on_row(source, first, last) for every row of a partition in any of its three forms (streaming segments,
//! sparse rows, dense offsets); the destinations are offsets within the partition's vertex range
template <class ON_ROW>
void ForEachRow(const LocalCSR &partition, ON_ROW &&on_row) {
	if (!partition.segments.empty()) {
		for (auto &segment : partition.segments) {
			for (idx_t row = 0; row < segment.source_vertices.size(); row++) {
				on_row(segment.source_vertices[row], segment.edges.data() + segment.row_offsets[row],
				       segment.edges.data() + segment.row_offsets[row + 1]);
			}
		}
		return;
	}
	if (partition.HasSparseRows()) {
		for (idx_t row = 0; row < partition.source_vertices.size(); row++) {
			on_row(partition.source_vertices[row], partition.e.data() + partition.row_offsets[row],
			       partition.e.data() + partition.row_offsets[row + 1]);
		}
		return;
	}
	for (idx_t source = 0; source < partition.GetVertexSize(); source++) {
		on_row(source, partition.e.data() + partition.v[source].load(std::memory_order_relaxed),
		       partition.e.data() + partition.v[source + 1].load(std::memory_order_relaxed));
	}
}

shared_ptr<FlatCSR> BuildFlatCSR(const std::vector<shared_ptr<LocalCSR>> &partitions, idx_t vertex_count) {
	auto flat = make_shared_ptr<FlatCSR>();
	flat->offsets.assign(vertex_count + 1, 0);
	// Partitions in destination order, so a source's destinations are appended ascending
	vector<const LocalCSR *> ordered;
	for (auto &partition : partitions) {
		if (partition) {
			ordered.push_back(partition.get());
		}
	}
	std::sort(ordered.begin(), ordered.end(),
	          [](const LocalCSR *left, const LocalCSR *right) { return left->start_vertex < right->start_vertex; });
	for (auto partition : ordered) {
		ForEachRow(*partition, [&](idx_t source, const uint16_t *first, const uint16_t *last) {
			if (source >= vertex_count) {
				throw InternalException("FlatCSR: source %llu outside the %llu vertices", source, vertex_count);
			}
			flat->offsets[source + 1] += NumericCast<uint64_t>(last - first);
		});
	}
	for (idx_t v = 0; v < vertex_count; v++) {
		flat->offsets[v + 1] += flat->offsets[v];
	}
	flat->destinations.resize(flat->offsets[vertex_count]);
	vector<uint64_t> cursor(flat->offsets.begin(), flat->offsets.end() - 1);
	for (auto partition : ordered) {
		auto base = NumericCast<uint32_t>(partition->start_vertex);
		ForEachRow(*partition, [&](idx_t source, const uint16_t *first, const uint16_t *last) {
			auto &position = cursor[source];
			for (auto edge = first; edge != last; edge++) {
				flat->destinations[position++] = base + *edge;
			}
		});
	}
	// FLAT_RANGES narrows a row to a destination range by binary search: rows a partition did not keep sorted
	// (streaming segments) are sorted here
	for (idx_t v = 0; v < vertex_count; v++) {
		auto first = flat->destinations.begin() + NumericCast<int64_t>(flat->offsets[v]);
		auto last = flat->destinations.begin() + NumericCast<int64_t>(flat->offsets[v + 1]);
		if (!std::is_sorted(first, last)) {
			std::sort(first, last);
		}
	}
	return flat;
}

struct FlatCSRCache {
	std::mutex lock;
	//! The partitions the cached CSR was built from, to recognise the same graph again. Weak references: the cache
	//! is a static object and outlives the database, and a partition of a CSR index holds memory of the database's
	//! buffer manager, so a partition this cache kept alive was released at process exit, after the buffer manager
	//! was gone (a crash at exit, after the query had answered). An expired reference never matches, so a new
	//! partition at a reused address is not taken for the old one
	std::vector<weak_ptr<LocalCSR>> partitions;
	idx_t vertex_count = 0;
	shared_ptr<FlatCSR> flat;
};

} // namespace

shared_ptr<FlatCSR> FlatCSR::Get(const std::vector<shared_ptr<LocalCSR>> &partitions, idx_t vertex_count) {
	static FlatCSRCache cache;
	std::lock_guard<std::mutex> guard(cache.lock);
	bool hit = cache.flat && cache.vertex_count == vertex_count && cache.partitions.size() == partitions.size();
	for (idx_t i = 0; hit && i < partitions.size(); i++) {
		hit = cache.partitions[i].lock().get() == partitions[i].get();
	}
	if (!hit) {
		cache.flat.reset();
		cache.partitions.assign(partitions.begin(), partitions.end());
		cache.vertex_count = vertex_count;
		cache.flat = BuildFlatCSR(partitions, vertex_count);
	}
	return cache.flat;
}

} // namespace duckdb
