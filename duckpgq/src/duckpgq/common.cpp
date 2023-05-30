#include "duckpgq/common.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckpgq/compressed_sparse_row.hpp"

#include <utility>

namespace duckdb {

    CSRFunctionData::CSRFunctionData(ClientContext &context, int32_t id, LogicalType weight_type)
            : context(context), id(id), weight_type(std::move(weight_type)) {
    }

    unique_ptr<FunctionData> CSRFunctionData::Copy() const {
        return make_unique<CSRFunctionData>(context, id, weight_type);
    }

    bool CSRFunctionData::Equals(const FunctionData &other_p) const {
        auto &other = (const CSRFunctionData &)other_p;
        return id == other.id && weight_type == other.weight_type;
    }

    unique_ptr<FunctionData> CSRFunctionData::CSRVertexBind(ClientContext &context, ScalarFunction &bound_function,
                                                            vector<unique_ptr<Expression>> &arguments) {
        if (!arguments[0]->IsFoldable()) {
            throw InvalidInputException("Id must be constant.");
        }

        Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
        if (arguments.size() == 4) {
            auto logical_type = LogicalType::SQLNULL;
            return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), logical_type);
        } else {
            return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), arguments[3]->return_type);
        }
    }

    unique_ptr<FunctionData> CSRFunctionData::CSREdgeBind(ClientContext &context, ScalarFunction &bound_function,
                                                          vector<unique_ptr<Expression>> &arguments) {
        if (!arguments[0]->IsFoldable()) {
            throw InvalidInputException("Id must be constant.");
        }
        Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
        if (arguments.size() == 7) {
            return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), arguments[6]->return_type);
        } else {
            auto logical_type = LogicalType::SQLNULL;
            return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), logical_type);
        }
    }

    unique_ptr<FunctionData> CSRFunctionData::CSRBind(ClientContext &context, ScalarFunction &bound_function,
                                                      vector<unique_ptr<Expression>> &arguments) {
        if (!arguments[0]->IsFoldable()) {
            throw InvalidInputException("Id must be constant.");
        }
        Value id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
        return make_unique<CSRFunctionData>(context, id.GetValue<int32_t>(), LogicalType::BOOLEAN);
    }

    unique_ptr<FunctionData> IterativeLengthFunctionData::Copy() const {
        return make_unique<IterativeLengthFunctionData>(context, csr_id);
    }

    bool IterativeLengthFunctionData::Equals(const FunctionData &other_p) const {
        auto &other = (const IterativeLengthFunctionData &)other_p;
        return other.csr_id == csr_id;
    }

    unique_ptr<FunctionData> IterativeLengthFunctionData::IterativeLengthBind(ClientContext &context,
                                                                              ScalarFunction &bound_function,
                                                                              vector<unique_ptr<Expression>> &arguments) {
        if (!arguments[0]->IsFoldable()) {
            throw InvalidInputException("Id must be constant.");
        }

        int32_t csr_id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]).GetValue<int32_t>();

        return make_unique<IterativeLengthFunctionData>(context, csr_id);
    }

    unique_ptr<FunctionData>
    CheapestPathLengthFunctionData::CheapestPathLengthBind(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments) {

        if (!arguments[0]->IsFoldable()) {
            throw InvalidInputException("Id must be constant.");
        }

        auto sqlpgq_state_entry = context.registered_state.find("sqlpgq");
        if (sqlpgq_state_entry == context.registered_state.end()) {
            //! Wondering how you can get here if the extension wasn't loaded, but leaving this check in anyways
            throw MissingExtensionException("The SQL/PGQ extension has not been loaded");
        }
        auto sqlpgq_state = reinterpret_cast<SQLPGQContext *>(sqlpgq_state_entry->second.get());

        int32_t csr_id = ExpressionExecutor::EvaluateScalar(context, *arguments[0]).GetValue<int32_t>();
        CSR *csr = sqlpgq_state->GetCSR(csr_id);

        if (!(csr->initialized_v && csr->initialized_e && csr->initialized_w)) {
            throw ConstraintException("Need to initialize CSR before doing cheapest path");
        }

        if (csr->w.empty()) {
            bound_function.return_type = LogicalType::DOUBLE;
        } else {
            bound_function.return_type = LogicalType::BIGINT;
        }

        return make_unique<CheapestPathLengthFunctionData>(context, csr_id);
    }

    unique_ptr<FunctionData> CheapestPathLengthFunctionData::Copy() const {
        return make_unique<CheapestPathLengthFunctionData>(context, csr_id);
    }

    bool CheapestPathLengthFunctionData::Equals(const FunctionData &other_p) const {
        auto &other = (const CheapestPathLengthFunctionData &)other_p;
        return other.csr_id == csr_id;
    }

} // namespace duckdb
