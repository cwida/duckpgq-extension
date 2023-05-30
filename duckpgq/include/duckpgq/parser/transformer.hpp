#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/tokens.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckpgq/common.hpp"

#include "pg_definitions.hpp"
#include "nodes/parsenodes.hpp"
#include "nodes/primnodes.hpp"


namespace duckdb {
class PGQTransformer {

    struct CreatePivotEntry {
        string enum_name;
        unique_ptr<SelectNode> base;
        unique_ptr<ParsedExpression> column;
        unique_ptr<QueryNode> subquery;
    };
public:
        explicit PGQTransformer();
//        explicit PGQTransformer(ParserOptions &options);
//        explicit PGQTransformer(PGQTransformer &parent);
        ~PGQTransformer();

    bool TransformParseTree(duckdb_libpgquery::PGList *tree, vector<unique_ptr<SQLStatement>> &statements);

    void Clear();

    PGQTransformer &RootTransformer();
    const PGQTransformer &RootTransformer() const;
    void SetParamCount(idx_t new_count);
//    optional_ptr<PGQTransformer> parent;
    vector<unique_ptr<CreatePivotEntry>> pivot_entries;
    idx_t prepared_statement_parameter_index = 0;

private:
    //! Current stack depth
    idx_t stack_depth;

    void InitializeStackCheck();
    };
}