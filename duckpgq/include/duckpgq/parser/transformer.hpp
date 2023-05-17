#pragma once

#include "duckpgq/common.hpp"
#include "duckdb/parser/parser_options.hpp"
#include "duckdb/parser/sql_statement.hpp"

namespace duckpgq {
    class PGQTransformer {
    public:
        explicit PGQTransformer();
//        explicit PGQTransformer(ParserOptions &options);
//        explicit PGQTransformer(PGQTransformer &parent);
        ~PGQTransformer();

    bool TransformParseTree(duckpgq_libpgquery::PGList *tree, vector<unique_ptr<SQLStatement>> &statements);
    };
}