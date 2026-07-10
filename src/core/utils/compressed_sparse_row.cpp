#include "duckpgq/core/utils/compressed_sparse_row.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

static string CSRIdentifier(const string &identifier) {
	return SQLIdentifier::ToString(identifier);
}

static string CSRQualifiedTableName(const PropertyGraphTable &table) {
	string result;
	if (!table.catalog_name.empty()) {
		result += CSRIdentifier(table.catalog_name) + ".";
	}
	if (!table.schema_name.empty()) {
		result += CSRIdentifier(table.schema_name) + ".";
	}
	result += CSRIdentifier(table.table_name);
	return result;
}

static string CSRTableRef(const PropertyGraphTable &table, const string &alias = "") {
	auto result = CSRQualifiedTableName(table);
	if (!alias.empty()) {
		result += " AS " + CSRIdentifier(alias);
	}
	return result;
}

static string CSRColumn(const string &column_name, const string &table_name = "") {
	if (table_name.empty()) {
		return CSRIdentifier(column_name);
	}
	return CSRIdentifier(table_name) + "." + CSRIdentifier(column_name);
}

static unique_ptr<SelectStatement> CSRParseSelect(const string &query) {
	Parser parser;
	parser.ParseQuery(query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement while building DuckPGQ CSR query");
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
}

static unique_ptr<CommonTableExpressionInfo> CSRParseCTE(const string &query) {
	auto statement = CSRParseSelect(query);
	auto result = make_uniq<CommonTableExpressionInfo>();
	result->query_node = std::move(statement->node);
	return result;
}

static unique_ptr<SubqueryExpression> CSRParseScalarSubquery(const string &query) {
	auto result = make_uniq<SubqueryExpression>();
	result->SubqueryMutable() = CSRParseSelect(query);
	result->GetSubqueryTypeMutable() = SubqueryType::SCALAR;
	return result;
}

static unique_ptr<SubqueryRef> CSRParseSubqueryRef(const string &query, const string &alias = "") {
	return make_uniq<SubqueryRef>(CSRParseSelect(query), Identifier(alias));
}

string CSR::ToString() const {
	std::ostringstream result;

	if (initialized_v) {
		result << "v (Node Offsets):\n";
		for (size_t i = 0; i < vsize; i++) {
			result << "  Node " << i << ": Offset " << v[i].load() << "\n";
		}
	} else {
		result << "v: V has not been initialized\n";
	}

	result << "\n";

	if (initialized_e) {
		result << "e (Edges):\n";
		for (size_t i = 0; i < vsize - 2; i++) {
			result << "  Node " << i << " connects to: ";
			for (size_t j = v[i].load(); j < v[i + 1].load(); j++) {
				result << e[j] << " ";
			}
			result << "\n";
		}
	} else {
		result << "e: E has not been initialized\n";
	}

	result << "\n";

	if (initialized_w) {
		result << "w (Weights):\n";
		for (size_t i = 0; i < vsize - 1; i++) {
			result << "  Node " << i << " weights: ";
			for (size_t j = v[i].load(); j < v[i + 1].load(); j++) {
				result << w[j] << " ";
			}
			result << "\n";
		}
	} else {
		result << "w: W has not been initialized\n";
	}

	return result.str();
}

CSRFunctionData::CSRFunctionData(ClientContext &context, int32_t id, const LogicalType &weight_type)
    : context(context), id(id), weight_type(weight_type) {
}

unique_ptr<FunctionData> CSRFunctionData::Copy() const {
	return make_uniq<CSRFunctionData>(context, id, weight_type);
}

bool CSRFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = dynamic_cast<const CSRFunctionData &>(other_p);
	return id == other.id && weight_type == other.weight_type;
}

unique_ptr<FunctionData> CSRFunctionData::CSRVertexBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("Id must be constant.");
	}

	Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (arguments.size() == 4) {
		auto logical_type = LogicalType::SQLNULL;
		return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(), logical_type);
	}
	return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(), arguments[3]->GetReturnType());
}

unique_ptr<FunctionData> CSRFunctionData::CSREdgeBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("Id must be constant.");
	}
	Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (arguments.size() == 8) {
		return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(), arguments[7]->GetReturnType());
	}
	auto logical_type = LogicalType::SQLNULL;
	return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(), logical_type);
}

unique_ptr<FunctionData> CSRFunctionData::CSRBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (!arguments[0]->IsFoldable()) {
		throw InvalidInputException("Id must be constant.");
	}
	Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	return make_uniq<CSRFunctionData>(context, id.GetValue<int32_t>(), LogicalType::BOOLEAN);
}

static string CSRCountTableSQL(const PropertyGraphTable &table, const string &table_alias, const string &primary_key) {
	std::ostringstream query;
	query << "SELECT count(" << CSRColumn(primary_key, table_alias) << ") FROM " << CSRTableRef(table, table_alias);
	return query.str();
}

static string CSRCountEdgeTableSQL(const PropertyGraphTable &edge_table) {
	std::ostringstream query;
	query << "SELECT count() FROM " << CSRTableRef(edge_table) << " INNER JOIN "
	      << CSRTableRef(*edge_table.source_pg_table, "src") << " ON "
	      << CSRColumn(edge_table.source_fk[0], edge_table.table_name) << " = "
	      << CSRColumn(edge_table.source_pk[0], "src") << " INNER JOIN "
	      << CSRTableRef(*edge_table.destination_pg_table, "dst") << " ON "
	      << CSRColumn(edge_table.destination_fk[0], edge_table.table_name) << " = "
	      << CSRColumn(edge_table.destination_pk[0], "dst");
	return query.str();
}

static string CSRCountUndirectedEdgeTableSQL() {
	std::ostringstream query;
	query << "SELECT multiply(2, count()) FROM ("
	      << "SELECT src, dst FROM edges_cte UNION BY NAME SELECT dst AS src, src AS dst FROM edges_cte)";
	return query.str();
}

static string CSRDirectedCSRVertexSQL(const PropertyGraphTable &edge_table, const string &prev_binding) {
	std::ostringstream query;
	query << "SELECT sum(create_csr_vertex(0, ("
	      << CSRCountTableSQL(*edge_table.source_pg_table, prev_binding, edge_table.source_pk[0])
	      << "), sub.dense_id, sub.cnt)) FROM (SELECT " << CSRColumn("rowid", prev_binding) << " AS dense_id, count("
	      << CSRColumn(edge_table.source_fk[0], edge_table.table_name) << ") AS cnt FROM "
	      << CSRTableRef(*edge_table.source_pg_table, prev_binding) << " LEFT JOIN " << CSRTableRef(edge_table)
	      << " ON " << CSRColumn(edge_table.source_fk[0], edge_table.table_name) << " = "
	      << CSRColumn(edge_table.source_pk[0], prev_binding) << " GROUP BY dense_id) sub";
	return query.str();
}

static string CSRUniqueEdgesSQL(const PropertyGraphTable &edge_table, bool reverse) {
	std::ostringstream query;
	query << "SELECT " << CSRColumn("rowid", edge_table.source_reference) << " AS dense_id, ";
	if (!reverse) {
		query << CSRColumn(edge_table.source_fk[0], edge_table.table_name) << " AS outgoing_edges, "
		      << CSRColumn(edge_table.destination_fk[0], edge_table.table_name) << " AS incoming_edges FROM "
		      << CSRTableRef(edge_table) << " INNER JOIN " << CSRTableRef(*edge_table.source_pg_table) << " ON "
		      << CSRColumn(edge_table.source_fk[0], edge_table.table_name) << " = "
		      << CSRColumn(edge_table.source_pk[0], edge_table.source_reference);
	} else {
		query << CSRColumn(edge_table.destination_fk[0], edge_table.table_name) << " AS outgoing_edges, "
		      << CSRColumn(edge_table.source_fk[0], edge_table.table_name) << " AS incoming_edges FROM "
		      << CSRTableRef(edge_table) << " INNER JOIN " << CSRTableRef(*edge_table.source_pg_table) << " ON "
		      << CSRColumn(edge_table.destination_fk[0], edge_table.table_name) << " = "
		      << CSRColumn(edge_table.source_pk[0], edge_table.source_reference);
	}
	return query.str();
}

static string CSRUndirectedCSRVertexSQL(const PropertyGraphTable &edge_table, const string &binding) {
	std::ostringstream query;
	query << "SELECT multiply(2, sum(create_csr_vertex(0, ("
	      << CSRCountTableSQL(*edge_table.source_pg_table, binding, edge_table.source_pk[0])
	      << "), sub.dense_id, sub.cnt))) FROM (SELECT dense_id, count(outgoing_edges) AS cnt FROM ("
	      << CSRUniqueEdgesSQL(edge_table, false) << " UNION BY NAME " << CSRUniqueEdgesSQL(edge_table, true)
	      << ") unique_edges GROUP BY dense_id) sub";
	return query.str();
}

// Function to create a subquery expression for counting table entries
unique_ptr<SubqueryExpression> GetCountTable(const shared_ptr<PropertyGraphTable> &table, const string &table_alias,
                                             const string &primary_key) {
	return CSRParseScalarSubquery(CSRCountTableSQL(*table, table_alias, primary_key));
}

unique_ptr<SubqueryExpression> CreateDirectedCSRVertexSubquery(const shared_ptr<PropertyGraphTable> &edge_table,
                                                               const string &prev_binding) {
	return CSRParseScalarSubquery(CSRDirectedCSRVertexSQL(*edge_table, prev_binding));
}

// Helper function to create CSR Vertex Subquery
unique_ptr<SubqueryExpression> CreateUndirectedCSRVertexSubquery(const shared_ptr<PropertyGraphTable> &edge_table,
                                                                 const string &binding) {
	return CSRParseScalarSubquery(CSRUndirectedCSRVertexSQL(*edge_table, binding));
}

// Function to create the CTE for the edges
unique_ptr<CommonTableExpressionInfo> MakeEdgesCTE(const shared_ptr<PropertyGraphTable> &edge_table) {
	std::ostringstream query;
	query << "SELECT " << CSRColumn("rowid", "src_table") << " AS src, " << CSRColumn("rowid", "dst_table")
	      << " AS dst, " << CSRColumn("rowid", edge_table->table_name) << " AS edges FROM " << CSRTableRef(*edge_table)
	      << " INNER JOIN " << CSRTableRef(*edge_table->source_pg_table, "src_table") << " ON "
	      << CSRColumn(edge_table->source_fk[0], edge_table->table_name) << " = "
	      << CSRColumn(edge_table->source_pk[0], "src_table") << " INNER JOIN "
	      << CSRTableRef(*edge_table->destination_pg_table, "dst_table") << " ON "
	      << CSRColumn(edge_table->destination_fk[0], edge_table->table_name) << " = "
	      << CSRColumn(edge_table->destination_pk[0], "dst_table");
	return CSRParseCTE(query.str());
}

// Function to create the CTE for the Undirected CSR
unique_ptr<CommonTableExpressionInfo> CreateUndirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                                                             const unique_ptr<SelectNode> &select_node) {
	if (select_node->cte_map.map.find(Identifier("edges_cte")) == select_node->cte_map.map.end()) {
		select_node->cte_map.map[Identifier("edges_cte")] = MakeEdgesCTE(edge_table);
	}

	std::ostringstream query;
	query << "SELECT create_csr_edge(0, ("
	      << CSRCountTableSQL(*edge_table->source_pg_table, edge_table->source_reference, edge_table->source_pk[0])
	      << "), CAST((" << CSRUndirectedCSRVertexSQL(*edge_table, edge_table->source_reference) << ") AS BIGINT), ("
	      << CSRCountUndirectedEdgeTableSQL() << "), src, dst, edge) AS temp FROM ("
	      << "SELECT src, dst, any_value(edges) AS edge FROM ("
	      << "SELECT src, dst, edges FROM edges_cte UNION ALL SELECT dst, src, edges FROM edges_cte"
	      << ") GROUP BY src, dst)";
	return CSRParseCTE(query.str());
}

unique_ptr<SubqueryExpression> GetCountUndirectedEdgeTable() {
	return CSRParseScalarSubquery(CSRCountUndirectedEdgeTableSQL());
}

unique_ptr<SubqueryExpression> GetCountEdgeTable(const shared_ptr<PropertyGraphTable> &edge_table) {
	return CSRParseScalarSubquery(CSRCountEdgeTableSQL(*edge_table));
}

// Function to create the CTE for the Directed CSR
unique_ptr<CommonTableExpressionInfo> CreateDirectedCSRCTE(const shared_ptr<PropertyGraphTable> &edge_table,
                                                           const string &prev_binding, const string &edge_binding,
                                                           const string &next_binding) {
	std::ostringstream query;
	query << "SELECT create_csr_edge(0, ("
	      << CSRCountTableSQL(*edge_table->source_pg_table, prev_binding, edge_table->source_pk[0]) << "), CAST(("
	      << CSRDirectedCSRVertexSQL(*edge_table, prev_binding) << ") AS BIGINT), ("
	      << CSRCountEdgeTableSQL(*edge_table) << "), " << CSRColumn("rowid", prev_binding) << ", "
	      << CSRColumn("rowid", next_binding) << ", " << CSRColumn("rowid", edge_binding) << ") AS temp FROM "
	      << CSRTableRef(*edge_table, edge_binding) << " INNER JOIN "
	      << CSRTableRef(*edge_table->source_pg_table, prev_binding) << " ON "
	      << CSRColumn(edge_table->source_fk[0], edge_binding) << " = "
	      << CSRColumn(edge_table->source_pk[0], prev_binding) << " INNER JOIN "
	      << CSRTableRef(*edge_table->destination_pg_table, next_binding) << " ON "
	      << CSRColumn(edge_table->destination_fk[0], edge_binding) << " = "
	      << CSRColumn(edge_table->destination_pk[0], next_binding);
	return CSRParseCTE(query.str());
}

// Function to create a subquery for counting with CTE
unique_ptr<SubqueryRef> CreateCountCTESubquery() {
	return CSRParseSubqueryRef("SELECT multiply(0, count(csr_cte.temp)) AS temp FROM csr_cte", "__x");
}

} // namespace duckdb
