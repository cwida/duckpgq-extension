#include "duckpgq/core/functions/function_data/csr_operator_function_data.hpp"

namespace duckpgq {

namespace core {

CSROperatorFunctionData::CSROperatorFunctionData(ClientContext &context)
    : context(context) {}

unique_ptr<FunctionData> CSROperatorFunctionData::Copy() const {
    return make_uniq<CSROperatorFunctionData>(context);
}

bool CSROperatorFunctionData::Equals(const FunctionData &other_p) const {
    // TODO implement me
    // auto &other = (const CSROperatorFunctionData &)other_p;
    return true;
}
} // namespace core

} // namespace duckpgq