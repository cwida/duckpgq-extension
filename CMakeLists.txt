cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME duckpgq)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
set(CMAKE_CXX_STANDARD 11)

include_directories(duckpgq/include)
add_subdirectory(duckpgq/src)

include_directories(../duckdb-pgq/third_party/libpg_query/include)
#add_subdirectory(../duckdb-pgq/third_party/libpg_query)

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

