cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME sqlpgq)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)
add_subdirectory(third_party)

#set(DUCKPGQ_LINK_LIBS
#    ${DUCKPGQ_SYSTEM_LIBS}
#    duckpgq_pg_query)

include_directories(third_party/libpg_query/include)
include_directories(third_party/libpg_query)



set(EXTENSION_SOURCES
    src/sqlpgq_extension.cpp
    src/sqlpgq_common.cpp
    src/sqlpgq_parser.cpp
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

#add_library(duckpgq SHARED ${ALL_OBJECT_FILES})
#target_link_libraries(duckpgq ${DUCKPGQ_LINK_LIBS})
#link_threads(duckpgq)
#
#add_library(duckpgq_static STATIC ${ALL_OBJECT_FILES})
#target_link_libraries(duckpgq_static ${DUCKPGQ_LINK_LIBS})
#link_threads(duckpgq_static)


set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
