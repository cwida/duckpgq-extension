#include <nodes/pg_list.hpp>
#include "duckpgq/parser/transformer.hpp"
#include "duckdb/parser/sql_statement.hpp"

using namespace duckdb;

namespace duckpgq {
    PGQTransformer::PGQTransformer() {

    }

    PGQTransformer::~PGQTransformer() {
    }

    bool PGQTransformer::TransformParseTree(duckpgq_libpgquery::PGList *tree, vector<unique_ptr<duckdb::SQLStatement>> &statements) {
        return false;
    }

}