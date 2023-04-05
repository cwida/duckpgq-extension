#!/usr/bin/python3

import sys, os
from pathlib import Path

if (len(sys.argv) != 2):
    raise Exception('usage: python3 set_extension_name.py <name_for_extension>')

string_to_find = "quack"
string_to_replace = sys.argv[1]

def replace(file_name, to_find, to_replace):
    with open(file_name, 'r', encoding="utf8") as file :
        filedata = file.read()
    filedata = filedata.replace(to_find, to_replace)
    with open(file_name, 'w', encoding="utf8") as file:
        file.write(filedata)

files_to_search = []
files_to_search.extend(Path('./.github').rglob('./**/*.yml'))
files_to_search.extend(Path('./test').rglob('./**/*.py'))
files_to_search.extend(Path('./test').rglob('./**/*.test'))
files_to_search.extend(Path('./test').rglob('./**/*.js'))
files_to_search.extend(Path('./src').rglob('./**/*.hpp'))
files_to_search.extend(Path('./src').rglob('./**/*.cpp'))
files_to_search.extend(Path('./src').rglob('./**/*.txt'))
files_to_search.extend(Path('./src').rglob('./**/*.md'))
for path in files_to_search:
    replace(path, string_to_find, string_to_replace)
    replace(path, string_to_find.capitalize(), string_to_replace.capitalize())

replace("./CMakeLists.txt", string_to_find, string_to_replace)
replace("./Makefile", string_to_find, string_to_replace)
replace("./Makefile", string_to_find.capitalize(), string_to_replace.capitalize())
replace("./Makefile", string_to_find.upper(), string_to_replace.upper())
replace("./README.md", string_to_find, string_to_replace)

# rename files
os.rename(f'test/python/{string_to_find}_test.py', f'test/python/{string_to_replace}_test.py')
os.rename(f'test/sql/{string_to_find}.test', f'test/sql/{string_to_replace}.test')
os.rename(f'src/{string_to_find}_extension.cpp', f'src/{string_to_replace}_extension.cpp')
os.rename(f'src/include/{string_to_find}_extension.hpp', f'src/include/{string_to_replace}_extension.hpp')
os.rename(f'test/nodejs/{string_to_find}_test.js', f'test/nodejs/{string_to_replace}_test.js')
