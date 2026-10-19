//===----------------------------------------------------------------------===//
//                         DuckPGQ
//
// duckpgq/core/functions/table/match.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckpgq/common.hpp"
#include <duckdb/parser/parsed_data/create_pragma_function_info.hpp>

#include "duckdb/function/table_function.hpp"
#include <duckpgq/parser/parsed_data/create_property_graph_info.hpp>
#include "duckpgq/parser/path_element.hpp"
#include "duckpgq/parser/path_pattern.hpp"
#include "duckpgq/parser/subpath_element.hpp"
#include "duckpgq/parser/property_graph_table.hpp"

#include <duckpgq/parser/tableref/matchref.hpp>

#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

class ClientContext;
class TableCatalogEntry;

//! One join between a vertex variable and an edge variable:
//! vertex.vertex_keys[i] = edge_alias.edge_keys[i] for every i.
struct VertexEdgeKeyReference {
	//! Alias of the edge relation in the FROM clause: the edge table itself, or the UNION ALL subquery that an
	//! undirected edge is rewritten into.
	string edge_alias;
	shared_ptr<PropertyGraphTable> edge_table;
	//! The vertex table this end of the edge is declared to reference; the pattern's vertex must be that table
	shared_ptr<PropertyGraphTable> referenced_vertex_table;
	//! Columns of edge_alias that reference the vertex key
	vector<Identifier> edge_keys;
	//! The vertex key columns, aligned with edge_keys
	vector<Identifier> vertex_keys;
	//! The column tuples of edge_table that feed edge_keys. A directed edge has one (edge_keys itself); the UNION ALL
	//! of an undirected edge has two, the source and the destination key, so constraints must hold for both.
	vector<vector<Identifier>> backing_keys;
};

//! Everything the translation knows about the joins of one vertex variable
struct VertexJoinInfo {
	vector<VertexEdgeKeyReference> edge_references;
	//! The translation needs the vertex table for more than its key (path-finding endpoint); never eliminate it.
	bool required = false;
};

//! Vertex alias -> incident edge keys, filled while the path patterns are translated. The join conditions are
//! emitted from this map afterwards, so that vertices that are only joined for their key can be left out.
struct VertexJoinMap {
	case_insensitive_map_t<VertexJoinInfo> entries;
	//! Vertex aliases in the order they were first joined, for deterministic output
	vector<string> order;

	void AddReference(const string &vertex_alias, VertexEdgeKeyReference reference);
	void MarkRequired(const string &vertex_alias);

private:
	VertexJoinInfo &GetOrCreate(const string &vertex_alias);
};

//! Vertex-join elimination: a vertex table that is only joined to prove that the edge endpoints exist, and whose key
//! is unique, contributes nothing to the result. The edges incident to the vertex are joined to each other on their
//! key columns instead, references to the vertex key are redirected to one of the edge keys, and the vertex table is
//! left out of the FROM clause. CREATE PROPERTY GRAPH verifies that every edge key references a vertex row, which is
//! what makes the join redundant; an edge made incomplete by later modifications is no longer dropped. A vertex is
//! only eliminated when every reference to it is to its key, it is not a path-finding endpoint or label-inheritance
//! check, all incident edges use the same key columns, and the key has a PRIMARY KEY or UNIQUE constraint.
class VertexEliminationPlanner {
public:
	static constexpr const char *SETTING_NAME = "duckpgq_vertex_join_elimination";

	//! graph_verified: CREATE PROPERTY GRAPH verified that every edge key references a vertex row; without it the
	//! vertex joins are kept.
	VertexEliminationPlanner(ClientContext &context, VertexJoinMap &vertex_joins,
	                         const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
	                         bool graph_verified);

	//! Decide which vertex variables are eliminated, looking at every expression of the translated query.
	void Plan(const vector<unique_ptr<ParsedExpression>> &column_list,
	          const vector<unique_ptr<ParsedExpression>> &conditions);
	bool IsEliminated(const string &vertex_alias) const;
	//! Replace references to the key of an eliminated vertex by the representative edge key column. Unaliased
	//! references in the COLUMNS list keep their output name.
	void RewriteReferences(vector<unique_ptr<ParsedExpression>> &column_list,
	                       vector<unique_ptr<ParsedExpression>> &conditions);
	//! Append the vertex-edge join conditions. For an eliminated vertex the incident edges are joined to each other
	//! on their key columns, guarded by IS NOT NULL on the representative key when the column is nullable.
	void EmitJoinConditions(vector<unique_ptr<ParsedExpression>> &conditions);

private:
	struct EliminatedVertex {
		string representative_alias;
		//! vertex key column -> column of representative_alias that replaces it
		case_insensitive_map_t<Identifier> representative_column;
		//! vertex key column -> its name in the catalog, which is what the result column was called before
		case_insensitive_map_t<Identifier> output_name;
		//! vertex key column -> type to cast the replacement to, when the edge column type differs
		case_insensitive_map_t<LogicalType> cast_type;
		//! vertex key column -> whether the replacement column can be NULL
		case_insensitive_set_t nullable_keys;
	};

	void CollectReferences(const ParsedExpression &expression);
	void Disqualify(const string &vertex_alias);
	void DisqualifyAll();
	bool IsVertexKey(const VertexJoinInfo &info, const Identifier &column) const;
	bool TableHasColumn(const string &vertex_alias, const Identifier &column);
	optional_ptr<TableCatalogEntry> LookupTable(const PropertyGraphTable &table);
	bool TryPlanVertex(const string &vertex_alias, const VertexJoinInfo &info, EliminatedVertex &result);
	void RewriteExpression(unique_ptr<ParsedExpression> &expression, bool preserve_name);

	static bool SameKeySet(const vector<Identifier> &left, const vector<Identifier> &right);
	static idx_t KeyPosition(const vector<Identifier> &keys, const Identifier &key);
	static bool HasUniqueConstraint(const TableCatalogEntry &table, const vector<Identifier> &keys);
	static bool IsNotNull(const TableCatalogEntry &table, const Identifier &column);

	ClientContext &context;
	VertexJoinMap &vertex_joins;
	const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map;
	bool enabled = true;
	bool graph_verified = false;
	case_insensitive_set_t disqualified;
	case_insensitive_map_t<EliminatedVertex> eliminated;
	//! Catalog lookups, keyed by the qualified table name
	case_insensitive_map_t<optional_ptr<TableCatalogEntry>> table_cache;
};

struct PGQMatchFunction : public TableFunction {
public:
	PGQMatchFunction() : TableFunction("duckpgq_match", vector<LogicalType>(), nullptr, nullptr) {
		bind_replace = MatchBindReplace;
	}

	struct MatchBindData : public TableFunctionData {
		bool done = false;
	};

	static shared_ptr<PropertyGraphTable> FindGraphTable(const string &label, CreatePropertyGraphInfo &pg_table);

	static void CheckInheritance(const shared_ptr<PropertyGraphTable> &tableref, PathElement *element,
	                             vector<unique_ptr<ParsedExpression>> &conditions);

	static void CheckEdgeTableConstraints(const Identifier &src_reference, const Identifier &dst_reference,
	                                      const shared_ptr<PropertyGraphTable> &edge_table);

	static unique_ptr<ParsedExpression> CreateMatchJoinExpression(vector<Identifier> vertex_keys,
	                                                              vector<Identifier> edge_keys,
	                                                              const string &vertex_alias, const string &edge_alias);

	// Populate all vertex and edge tables and their alias into
	// [alias_to_vertex_and_edge_tables], from paths information from
	// [path_reference].
	static void
	PopulateGraphTableAliasMap(const CreatePropertyGraphInfo &pg_table, const unique_ptr<PathReference> &path_reference,
	                           case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_to_vertex_and_edge_tables);

	static case_insensitive_map_t<shared_ptr<PropertyGraphTable>>
	PopulateGraphTableAliasMap(const CreatePropertyGraphInfo &pg_table, const MatchExpression &match_expr);

	static PathElement *GetPathElement(const unique_ptr<PathReference> &path_reference);

	static SubPath *GetSubPath(const unique_ptr<PathReference> &path_reference);

	static unique_ptr<JoinRef> GetJoinRef(const shared_ptr<PropertyGraphTable> &edge_table, const string &edge_binding,
	                                      const string &prev_binding, const string &next_binding);

	static unique_ptr<SubqueryRef> CreateCountCTESubquery();

	static unique_ptr<ParsedExpression> CreateWhereClause(vector<unique_ptr<ParsedExpression>> &conditions);

	//! Record that vertex_alias.vertex_keys = edge_alias.edge_keys. The condition itself is emitted later by the
	//! VertexEliminationPlanner, which may replace it when the vertex is eliminated.
	static void RecordVertexJoin(VertexJoinMap &vertex_joins, const string &vertex_alias,
	                             const vector<Identifier> &vertex_keys, const string &edge_alias,
	                             const vector<Identifier> &edge_keys, const shared_ptr<PropertyGraphTable> &edge_table,
	                             const shared_ptr<PropertyGraphTable> &referenced_vertex_table,
	                             vector<vector<Identifier>> backing_keys = {});

	static void EdgeTypeAny(const shared_ptr<PropertyGraphTable> &edge_table, const string &edge_binding,
	                        const string &prev_binding, const string &next_binding, VertexJoinMap &vertex_joins,
	                        unique_ptr<TableRef> &from_clause);

	static void EdgeTypeLeft(const shared_ptr<PropertyGraphTable> &edge_table, const Identifier &next_table_name,
	                         const Identifier &prev_table_name, const string &edge_binding, const string &prev_binding,
	                         const string &next_binding, VertexJoinMap &vertex_joins);

	static void EdgeTypeRight(const shared_ptr<PropertyGraphTable> &edge_table, const Identifier &next_table_name,
	                          const Identifier &prev_table_name, const string &edge_binding, const string &prev_binding,
	                          const string &next_binding, VertexJoinMap &vertex_joins);

	static void EdgeTypeLeftRight(const shared_ptr<PropertyGraphTable> &edge_table, const string &edge_binding,
	                              const string &prev_binding, const string &next_binding, VertexJoinMap &vertex_joins,
	                              case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
	                              int32_t &extra_alias_counter);

	static PathElement *HandleNestedSubPath(unique_ptr<PathReference> &path_reference,
	                                        vector<unique_ptr<ParsedExpression>> &conditions, idx_t element_idx);

	static unique_ptr<ParsedExpression> AddPathQuantifierCondition(const string &prev_binding,
	                                                               const string &next_binding,
	                                                               const shared_ptr<PropertyGraphTable> &edge_table,
	                                                               const SubPath *subpath);

	static unique_ptr<TableRef> MatchBindReplace(ClientContext &context, TableFunctionBindInput &input);

	static unique_ptr<SubqueryRef> GenerateSubpathPatternSubquery(unique_ptr<PathPattern> &path_pattern,
	                                                              CreatePropertyGraphInfo *pg_table,
	                                                              vector<unique_ptr<ParsedExpression>> &column_list,
	                                                              unordered_set<string> &named_subpaths);

	static unique_ptr<CommonTableExpressionInfo>
	GenerateShortestPathCTE(CreatePropertyGraphInfo &pg_table, SubPath *edge_subpath, PathElement *path_element,
	                        PathElement *next_vertex_element,
	                        vector<unique_ptr<ParsedExpression>> &path_finding_conditions);

	static unique_ptr<ParsedExpression> CreatePathFindingFunction(vector<unique_ptr<PathReference>> &path_list,
	                                                              CreatePropertyGraphInfo &pg_table,
	                                                              const string &path_variable,
	                                                              unique_ptr<SelectNode> &final_select_node,
	                                                              vector<unique_ptr<ParsedExpression>> &conditions);

	static void AddPathFinding(unique_ptr<SelectNode> &select_node, vector<unique_ptr<ParsedExpression>> &conditions,
	                           const string &prev_binding, const string &edge_binding, const string &next_binding,
	                           const shared_ptr<PropertyGraphTable> &edge_table, CreatePropertyGraphInfo &pg_table,
	                           SubPath *subpath, PGQMatchType edge_type, VertexJoinMap &vertex_joins);

	static void AddEdgeJoins(const shared_ptr<PropertyGraphTable> &edge_table,
	                         const shared_ptr<PropertyGraphTable> &previous_vertex_table,
	                         const shared_ptr<PropertyGraphTable> &next_vertex_table, PGQMatchType edge_type,
	                         const string &edge_binding, const string &prev_binding, const string &next_binding,
	                         VertexJoinMap &vertex_joins,
	                         case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
	                         int32_t &extra_alias_counter, unique_ptr<TableRef> &from_clause);

	static void ProcessPathList(vector<unique_ptr<PathReference>> &path_pattern,
	                            vector<unique_ptr<ParsedExpression>> &conditions, unique_ptr<SelectNode> &select_node,
	                            case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_map,
	                            CreatePropertyGraphInfo &pg_table, int32_t &extra_alias_counter,
	                            MatchExpression &original_ref, VertexJoinMap &vertex_joins);

	static void CheckNamedSubpath(SubPath &subpath, MatchExpression &original_ref, CreatePropertyGraphInfo &pg_table,
	                              unique_ptr<SelectNode> &final_select_node,
	                              vector<unique_ptr<ParsedExpression>> &conditions);

	// Check whether columns to query are valid against the property graph, throws
	// BinderException if error.
	static void
	CheckColumnBinding(const CreatePropertyGraphInfo &pg_table, const MatchExpression &ref,
	                   const case_insensitive_map_t<shared_ptr<PropertyGraphTable>> &alias_to_vertex_and_edge_tables);
};

} // namespace duckdb
