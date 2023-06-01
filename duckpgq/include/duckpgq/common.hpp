//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlpgq_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/helper.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/compressed_sparse_row.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"

namespace duckdb {

    class DuckPGQContext : public ClientContextState {
    public:
        explicit DuckPGQContext() {
        }

        CreatePropertyGraphInfo *GetPropertyGraph(const string &pg_name) {
            auto pg_table_entry = registered_property_graphs.find(pg_name);
            if (pg_table_entry == registered_property_graphs.end()) {
                throw BinderException("Property graph %s does not exist", pg_name);
            }
            return reinterpret_cast<CreatePropertyGraphInfo *>(pg_table_entry->second.get());
        }

        CSR *GetCSR(int32_t id) {
            auto csr_entry = csr_list.find(id);
            if (csr_entry == csr_list.end()) {
                throw ConstraintException("CSR not found with ID %d", id);
            }
            return csr_entry->second.get();
        }

        void QueryEnd() override {
            for (const auto &csr_id : csr_to_delete) {
                csr_list.erase(csr_id);
            }
        }

    public:
        //! Property graphs that are registered
        std::unordered_map<string, unique_ptr<CreateInfo>> registered_property_graphs;

        //! Used to build the CSR data structures required for path-finding queries
        std::unordered_map<int32_t, unique_ptr<CSR>> csr_list;
        std::mutex csr_lock;
        std::unordered_set<int32_t> csr_to_delete;
    };

    struct CSRFunctionData : public FunctionData {
    public:
        CSRFunctionData(ClientContext &context, int32_t id, LogicalType weight_type);
        unique_ptr<FunctionData> Copy() const override;
        bool Equals(const FunctionData &other_p) const override;
        static unique_ptr<FunctionData> CSRVertexBind(ClientContext &context, ScalarFunction &bound_function,
                                                      vector<unique_ptr<Expression>> &arguments);
        static unique_ptr<FunctionData> CSREdgeBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments);
        static unique_ptr<FunctionData> CSRBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments);

    public:
        ClientContext &context;
        const int32_t id;
        const LogicalType weight_type; // TODO Make sure type is LogicalType::SQLNULL when no type is provided
    };

    struct IterativeLengthFunctionData : public FunctionData {
    public:
        ClientContext &context;
        int32_t csr_id;

        IterativeLengthFunctionData(ClientContext &context, int32_t csr_id) : context(context), csr_id(csr_id) {
        }
        static unique_ptr<FunctionData> IterativeLengthBind(ClientContext &context, ScalarFunction &bound_function,
                                                            vector<unique_ptr<Expression>> &arguments);

        unique_ptr<FunctionData> Copy() const override;
        bool Equals(const FunctionData &other_p) const override;
    };

    struct CheapestPathLengthFunctionData : public FunctionData {
        ClientContext &context;
        int32_t csr_id;

        CheapestPathLengthFunctionData(ClientContext &context, int32_t csr_id) : context(context), csr_id(csr_id) {
        }
        static unique_ptr<FunctionData> CheapestPathLengthBind(ClientContext &context, ScalarFunction &bound_function,
                                                               vector<unique_ptr<Expression>> &arguments);

        unique_ptr<FunctionData> Copy() const override;
        bool Equals(const FunctionData &other_p) const override;
    };

} // namespace duckdb
