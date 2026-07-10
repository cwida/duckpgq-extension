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

namespace duckdb {

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
                                             const string &primary_key);
unique_ptr<SubqueryRef> CreateCountCTESubquery();
unique_ptr<SubqueryExpression> GetCountUndirectedEdgeTable();
unique_ptr<SubqueryExpression> GetCountEdgeTable(const shared_ptr<PropertyGraphTable> &edge_table);

} // namespace duckdb
