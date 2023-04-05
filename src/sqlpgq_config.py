import os

# list all include directories
include_directories = [os.path.sep.join(x.split('/')) for x in ['extension/sqlpgq/include']]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in
                ['extension/sqlpgq/sqlpgq-extension.cpp', 'extension/sqlpgq/sqlpgq_common.cpp',
                 'extension/sqlpgq/sqlpgq_functions/sqlpgq_cheapest_path_length.cpp',
                 'extension/sqlpgq/sqlpgq_functions/sqlpgq_csr_creation.cpp',
                 'extension/sqlpgq/sqlpgq_functions/sqlpgq_reachability.cpp',
                 'extension/sqlpgq/sqlpgq_functions/sqlpgq_shortest_path.cpp',
                 'extension/sqlpgq/sqlpgq_functions/sqlpgq_iterativelength.cpp',
                 'extension/sqlpgq/sqlpgq_functions/sqlpgq_iterativelength2.cpp',
                 'extension/sqlpgq/sqlpgq_functions/sqlpgq_iterativelength_bidirectional.cpp']]
