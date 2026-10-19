#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

#include "duckpgq/core/utils/compressed_sparse_row.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include <duckpgq/parser/parsed_data/create_property_graph_info.hpp>

namespace duckdb {

class DuckPGQState : public ClientContextState {
public:
	explicit DuckPGQState() {};

	static void InitializeInternalTable(ClientContext &context);
	void QueryEnd() override;
	CreatePropertyGraphInfo *GetPropertyGraph(const string &pg_name);
	//! Looks up a graph by name and, when the name is qualified, checks the catalog/schema it was created in.
	CreatePropertyGraphInfo *GetPropertyGraph(ClientContext &context, const QualifiedName &pg_name);
	optional_ptr<CreatePropertyGraphInfo> TryGetPropertyGraph(ClientContext &context, const QualifiedName &pg_name);
	//! Fills in the default catalog and schema, deciding whether a lone qualifier is a schema or a catalog.
	static QualifiedName ResolvePropertyGraphName(ClientContext &context, const QualifiedName &pg_name);
	CSR *GetCSR(int32_t id);

	void RetrievePropertyGraphs(const shared_ptr<Connection> &context);
	void ProcessPropertyGraphs(ClientContext &context, unique_ptr<QueryResult> &property_graphs, bool is_vertex);
	void PopulateEdgeSpecificFields(unique_ptr<DataChunk> &chunk, idx_t row_idx, PropertyGraphTable &table);
	static void ExtractListValues(const Value &list_value, vector<Identifier> &output);
	void RegisterPropertyGraph(const shared_ptr<PropertyGraphTable> &table, const QualifiedName &graph_name,
	                           bool is_vertex, bool verified);

public:
	unique_ptr<ParserExtensionParseData> parse_data;

	//! Property graphs that are registered
	case_insensitive_map_t<unique_ptr<CreateInfo>> registered_property_graphs;

	//! Used to build the CSR data structures required for path-finding queries
	std::unordered_map<int32_t, unique_ptr<CSR>> csr_list;
	std::mutex csr_lock;
	std::unordered_set<int32_t> csr_to_delete;
};

} // namespace duckdb
