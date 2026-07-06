#!/usr/bin/env python3

import argparse
from pathlib import Path


SOURCE_SUFFIXES = {".cpp", ".hpp", ".h"}


def wrap_file(path: Path) -> bool:
    text = path.read_text()
    if "namespace duckpgq_peg" in text:
        return False
    if "namespace duckdb {" not in text:
        return False
    if "} // namespace duckdb" not in text:
        raise RuntimeError(f"Cannot safely wrap namespace in {path}: missing namespace close comment")

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
