import os

# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['src/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in
                ['src/sqlpgq_extension.cpp',
                 'src/sqlpgq_common.cpp',
                 'src/sqlpgq_parser.cpp',
                 'src/sqlpgq_functions/sqlpgq_cheapest_path_length.cpp',
                 'src/sqlpgq_functions/sqlpgq_csr_creation.cpp',
                 'src/sqlpgq_functions/sqlpgq_reachability.cpp',
                 'src/sqlpgq_functions/sqlpgq_shortest_path.cpp',
                 'src/sqlpgq_functions/sqlpgq_iterativelength.cpp',
                 'src/sqlpgq_functions/sqlpgq_iterativelength2.cpp',
                 'src/sqlpgq_functions'
                 'src/sqlpgq_functions/sqlpgq_iterativelength_bidirectional.cpp']]
