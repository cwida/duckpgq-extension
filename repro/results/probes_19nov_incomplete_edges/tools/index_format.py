#!/usr/bin/env python3
"""Which stored CSR indexes of a database can a build of the extension load?

For every CSR index of the edge table, in a fresh process with the database attached read-only:
duckpgq_csr_index_stats loads the stored state of the index but does not build an epoch from the table, so
`epochs=1` means the stored state was loaded and `epochs=0` means the build cannot use it (another storage format, or
stored without the reverse side the definition now asks for) and the first query would rebuild the index from the
table, in every process while the database is attached read-only.

    index_format.py --cli <duckdb> --extension <duckpgq.duckdb_extension> --label new <database> [<database> ...]
"""
import argparse
import subprocess


def run(cli, script):
    result = subprocess.run([cli, "-no-agent", "-unsigned", "-list", "-noheader"], input=script, text=True,
                            capture_output=True)
    return result.stdout.strip().splitlines(), result.stderr.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    parser.add_argument("--extension", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--table", default="person_knows_person")
    parser.add_argument("databases", nargs="+")
    args = parser.parse_args()
    for database in args.databases:
        attach = f"LOAD '{args.extension}';\nATTACH '{database}' AS ldbc (READ_ONLY);\n"
        names, err = run(args.cli, attach + "SELECT index_name || '|' || sql FROM duckdb_indexes() WHERE "
                         f"database_name = 'ldbc' AND table_name = '{args.table}' AND sql ILIKE '%USING CSR%' ORDER BY 1;")
        if err:
            print(f"{database}: {err}")
            continue
        usable = 0
        for line in names:
            name, definition = line.split("|", 1)
            out, err = run(args.cli, attach + f"SELECT duckpgq_csr_index_stats('ldbc.main.{args.table}', '{name}');")
            stats = out[-1] if out else "ERROR " + " ".join(err.split())[:200]
            loaded = stats.startswith("epochs=") and not stats.startswith("epochs=0")
            usable += loaded
            options = definition.split(")", 1)[1].strip(" ;") if ")" in definition else ""
            print(f"{database.rsplit('/', 1)[-1]:24s} {args.label:5s} {name:42s} {'loaded ' if loaded else 'UNUSABLE'} "
                  f"{stats}  {options}")
        print(f"{database.rsplit('/', 1)[-1]}: {args.label}: {usable} of {len(names)} stored indexes load")


if __name__ == "__main__":
    main()
