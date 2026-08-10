//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/utils/compressed_sparse_row.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"

#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"

#include "duckpgq/parser/property_graph_table.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckpgq/common.hpp"

#include <algorithm>
#include <limits>

namespace duckdb {

class LocalCSR {
public:
	explicit LocalCSR(idx_t start_vertex_p, idx_t end_vertex_p, size_t number_of_vertices,
	                  bool initialize_vertex_array = true)
	    : start_vertex(start_vertex_p), end_vertex(end_vertex_p), v_array_size(number_of_vertices + 2) {
		if (initialize_vertex_array) {
			v = new std::atomic<uint32_t>[v_array_size]();
			initialized_v = true;
		}
	}

	~LocalCSR() {
		delete[] v;
	}

	std::atomic<uint32_t> *GetVertexArray() {
		return v;
	}
	std::vector<uint16_t> *GetEdgeVector() {
		return &e;
	}

	string ToString() const;

	size_t GetVertexSize() const {
		return sparse_rows_initialized ? source_vertices.size() : v_array_size - 2;
	}
	size_t GetEdgeSize() const {
		return e.size();
	}

	void FinalizeSparseRows() {
		if (sparse_rows_initialized) {
			return;
		}

		auto source_count = v_array_size - 2;
		source_vertices.clear();
		row_offsets.clear();
		source_vertices.reserve(std::min<size_t>(source_count, e.size()));
		row_offsets.reserve(source_vertices.capacity() + 1);
		row_offsets.push_back(0);

		for (idx_t source = 0; source < source_count; source++) {
			auto start_edges = v[source].load(std::memory_order_relaxed);
			auto end_edges = v[source + 1].load(std::memory_order_relaxed);
			if (start_edges == end_edges) {
				continue;
			}
			source_vertices.push_back(static_cast<uint32_t>(source));
			row_offsets.back() = start_edges;
			row_offsets.push_back(end_edges);
		}

		delete[] v;
		v = nullptr;
		v_array_size = 0;
		sparse_rows_initialized = true;
	}

	bool HasSparseRows() const {
		return sparse_rows_initialized;
	}

	idx_t FindSparseRow(idx_t source_vertex) const {
		auto entry = std::lower_bound(source_vertices.begin(), source_vertices.end(), static_cast<uint32_t>(source_vertex));
		if (entry == source_vertices.end() || *entry != source_vertex) {
			return std::numeric_limits<idx_t>::max();
		}
		return static_cast<idx_t>(entry - source_vertices.begin());
	}

	bool GetRowEdges(idx_t source_vertex, uint32_t &start_edges, uint32_t &end_edges) const {
		if (sparse_rows_initialized) {
			auto row_idx = FindSparseRow(source_vertex);
			if (row_idx == std::numeric_limits<idx_t>::max()) {
				return false;
			}
			start_edges = row_offsets[row_idx];
			end_edges = row_offsets[row_idx + 1];
			return start_edges != end_edges;
		}

		if (source_vertex + 1 >= v_array_size) {
			return false;
		}
		start_edges = v[source_vertex].load(std::memory_order_relaxed);
		end_edges = v[source_vertex + 1].load(std::memory_order_relaxed);
		return start_edges != end_edges;
	}

	bool PartitioningDone(size_t partition_size) const {
		return GetEdgeSize() <= partition_size;
	}

	std::atomic<uint32_t> *v {};
	size_t v_array_size;
	std::vector<uint16_t> e;
	std::vector<uint32_t> source_vertices;
	std::vector<uint32_t> row_offsets;

	idx_t start_vertex;
	idx_t end_vertex;
	bool initialized_v = false;
	bool initialized_e = false;
	bool sparse_rows_initialized = false;
};

class PullCSR {
public:
	explicit PullCSR(idx_t start_vertex_p, idx_t end_vertex_p)
	    : start_vertex(start_vertex_p), end_vertex(end_vertex_p), offsets_size(end_vertex_p - start_vertex_p + 1) {
		offsets = new std::atomic<uint32_t>[offsets_size]();
	}

	PullCSR(const PullCSR &) = delete;
	PullCSR &operator=(const PullCSR &) = delete;

	~PullCSR() {
		delete[] offsets;
	}

	size_t GetVertexSize() const {
		return offsets_size - 1;
	}
	size_t GetEdgeSize() const {
		return predecessors.size();
	}

	std::atomic<uint32_t> *offsets {};
	size_t offsets_size;
	std::vector<uint32_t> predecessors;

	idx_t start_vertex;
	idx_t end_vertex;
};

//! A completed partitioned CSR layout. Instances are immutable after publication
//! in the connection-local cache and can be shared by subsequent queries.
struct PartitionedCSRIndex {
	idx_t vertex_count = 0;
	idx_t edge_count = 0;
	std::vector<shared_ptr<LocalCSR>> forward_partitions;
	std::vector<shared_ptr<LocalCSR>> reverse_partitions;
	std::vector<shared_ptr<PullCSR>> pull_partitions;
};

class CSR {
public:
	CSR() = default;
	~CSR() {
		delete[] v;
	}

	atomic<int64_t> *v {};

	vector<int64_t> e;
	vector<int64_t> edge_ids;

	vector<int64_t> w;
	vector<double> w_double;

	bool initialized_v = false;
	bool initialized_e = false;
	bool initialized_w = false;

	size_t vsize {};

	string ToString() const;
};

struct CSRFunctionData : FunctionData {
	CSRFunctionData(ClientContext &context, int32_t id, const LogicalType &weight_type);
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
	static unique_ptr<FunctionData> CSRVertexBind(BindScalarFunctionInput &input);
	static unique_ptr<FunctionData> CSREdgeBind(BindScalarFunctionInput &input);
	static unique_ptr<FunctionData> CSRBind(BindScalarFunctionInput &input);

	ClientContext &context;
	const int32_t id;
	const LogicalType weight_type;
};

// CSR BindReplace functions
unique_ptr<CommonTableExpressionInfo> CreateUndirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                                                             const unique_ptr<SelectNode> &select_node);
unique_ptr<CommonTableExpressionInfo> CreateDirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                                                           const string &prev_binding, const string &edge_binding,
                                                           const string &next_binding);

// Helper functions
unique_ptr<CommonTableExpressionInfo> MakeEdgesCTE(const shared_ptr<PropertyGraphTable> &edge_table);
unique_ptr<SubqueryExpression> CreateDirectedCSRVertexSubquery(const shared_ptr<PropertyGraphTable> &edge_table,
                                                               const string &binding);
unique_ptr<SubqueryExpression> CreateUndirectedCSRVertexSubquery(const shared_ptr<PropertyGraphTable> &edge_table,
                                                                 const string &binding);
unique_ptr<SubqueryExpression> GetCountTable(const shared_ptr<PropertyGraphTable> &table, const string &table_alias,
                                             const Identifier &primary_key);
unique_ptr<SubqueryRef> CreateCountCTESubquery();
unique_ptr<SubqueryExpression> GetCountUndirectedEdgeTable();
unique_ptr<SubqueryExpression> GetCountEdgeTable(const shared_ptr<PropertyGraphTable> &edge_table);

} // namespace duckdb
