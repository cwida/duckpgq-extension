#!/usr/bin/env python3

import argparse
from pathlib import Path


SOURCE_SUFFIXES = {".cpp", ".hpp", ".h"}
VENDORED_INCLUDE_PREFIX = "duckpgq/third_party/duckdb_peg_parser/peg/"


def wrap_file(path: Path) -> bool:
    text = path.read_text()
    text = text.replace('"duckdb/parser/peg/', f'"{VENDORED_INCLUDE_PREFIX}')
    text = text.replace("<duckdb/parser/peg/", f"<{VENDORED_INCLUDE_PREFIX}")
    text = text.replace(
        "EnumUtil::ToString(frame.state)",
        '(frame.state == TransformFrameState::INITIALIZE ? "INITIALIZE" : "WAITING")',
    )
    text = text.replace(
        "\tstatic vector<GenericCopyOption> TransformAttachOptions(PEGTransformer &transformer, const vector<GenericCopyOption> &generic_copy_option_list);\n"
        "\tstatic unique_ptr<TransformResultValue> TransformCallStatementInternal",
        "\tstatic vector<GenericCopyOption> TransformAttachOptions(PEGTransformer &transformer, const vector<GenericCopyOption> &generic_copy_option_list);\n"
        "\tstatic void SplitGenericOptions(const vector<GenericCopyOption> &options_in,\n"
        "\t                                case_insensitive_map_t<unique_ptr<ParsedExpression>> &parsed_options,\n"
        "\t                                unordered_map<string, Value> &options, const char *statement_name);\n"
        "\tstatic unique_ptr<TransformResultValue> TransformCallStatementInternal",
    )
    text = text.replace(
        "shared_ptr<PEGMatcher> PEGMatcher::Get(DatabaseInstance &db) {\n"
        "\tauto &parser_cache = db.GetParserCache();\n"
        "\treturn parser_cache.GetMatcher();\n"
        "}",
        "shared_ptr<PEGMatcher> PEGMatcher::Get(DatabaseInstance &db) {\n"
        "\t(void)db;\n"
        "\treturn make_shared_ptr<PEGMatcher>();\n"
        "}",
    )

    if "namespace duckpgq_peg" in text:
        path.write_text(text)
        return False
    if "namespace duckdb {" not in text:
        path.write_text(text)
        return False
    if "} // namespace duckdb" not in text:
        raise RuntimeError(f"Cannot safely wrap namespace in {path}: missing namespace close comment")

    text = text.replace("struct QualifiedName;", "using duckdb::QualifiedName;")

    updated = text.replace(
        "namespace duckdb {\n",
        "namespace duckdb {\nnamespace duckpgq_peg {\n",
    )
    updated = updated.replace(
        "} // namespace duckdb",
        "} // namespace duckpgq_peg\n} // namespace duckdb",
    )

    if updated == text:
        return False

    path.write_text(updated)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wrap vendored DuckDB PEG parser C++ files in duckdb::duckpgq_peg."
    )
    parser.add_argument("root", type=Path, help="Vendored DuckDB PEG parser root")
    args = parser.parse_args()

    root = args.root.resolve()
    if not root.exists():
        raise SystemExit(f"Root does not exist: {root}")

    wrapped = 0
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in SOURCE_SUFFIXES:
            continue
        if wrap_file(path):
            wrapped += 1

    print(f"Wrapped {wrapped} DuckDB PEG parser files in duckdb::duckpgq_peg")


if __name__ == "__main__":
    main()
