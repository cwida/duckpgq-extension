from os import listdir, mkdir
from os.path import isfile, join, exists

from pathlib import Path
import shutil
from textwrap import dedent

def main():
    test_path_duckpgq = Path("../test/sql")
    test_path_duckdb = Path("../duckdb/test/extension/duckpgq")

    onlyfiles = [str(f) for f in listdir(test_path_duckpgq) if isfile(join(test_path_duckpgq, f))]

    if not exists(test_path_duckdb):
        mkdir(test_path_duckdb)
    else:
        shutil.rmtree(test_path_duckdb)
        mkdir(test_path_duckdb)

    for file in onlyfiles:
        f = open(test_path_duckpgq / file, "r")
        content = f.read()
        content = content.replace("require duckpgq\n",
                                  dedent("""\
                                  statement ok
                                  install '__BUILD_DIR__/../../../build/release/extension/duckpgq/duckpgq.duckdb_extension';
                                  
                                  statement ok
                                  load 'duckpgq';
                                  """))

        new_file = open(test_path_duckdb/file, "w")
        new_file.write(content)
        new_file.close()


if __name__ == "__main__":
    main()

