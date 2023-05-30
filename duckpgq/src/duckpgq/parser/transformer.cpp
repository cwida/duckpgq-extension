#include <nodes/pg_list.hpp>
#include "duckpgq/parser/transformer.hpp"
#include "duckdb/parser/sql_statement.hpp"

using namespace duckdb;

namespace duckpgq {
PGQTransformer::PGQTransformer() = default;

PGQTransformer::~PGQTransformer() = default;

bool PGQTransformer::TransformParseTree(duckdb_libpgquery::PGList *tree, vector<unique_ptr<SQLStatement>> &statements) {
    InitializeStackCheck();
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        Clear();
//        auto n = (duckpgq_libpgquery::PGNode *)entry->data.ptr_value;
//        auto stmt = TransformStatement(n);
//        D_ASSERT(stmt);
//        if (HasPivotEntries()) {
//            stmt = CreatePivotStatement(std::move(stmt));
//        }
//        stmt->n_param = ParamCount();
//        statements.push_back(std::move(stmt));
    }
    return true;
}

PGQTransformer &PGQTransformer::RootTransformer() {
//    reference<PGQTransformer> node = *this;
//    while (node.get().parent) {
//        node = *node.get().parent;
//    }
//    return node.get();
}

void PGQTransformer::SetParamCount(idx_t new_count) {
//    auto &root = RootTransformer();
//    root.prepared_statement_parameter_index = new_count;
}

void PGQTransformer::Clear() {
//    SetParamCount(0);
//    pivot_entries.clear();
}

void PGQTransformer::InitializeStackCheck() {
//    stack_depth = 0;
}

}