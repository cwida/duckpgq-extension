cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME sqlpgq)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
        src/sqlpgq_extension.cpp
        src/sqlpgq_common.cpp
        src/sqlpgq_functions/sqlpgq_csr_creation.cpp
        src/sqlpgq_functions/sqlpgq_cheapest_path_length.cpp
        src/sqlpgq_functions/sqlpgq_shortest_path.cpp
        src/sqlpgq_functions/sqlpgq_reachability.cpp
        src/sqlpgq_functions/sqlpgq_iterativelength.cpp
        src/sqlpgq_functions/sqlpgq_iterativelength2.cpp
        src/sqlpgq_functions/sqlpgq_bidirectional_iterativelength.cpp
        src/sqlpgq_functions/sqlpgq_iterativelength_bidirectional.cpp
        src/sqlpgq_functions/sqlpgq_csr_deletion.cpp
        )
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
