cmake_minimum_required(VERSION 2.8.12)

# Set extension name here <<<<<<< HEAD set(TARGET_NAME duckpgq)
#
# set(EXTENSION_NAME ${TARGET_NAME}_extension) project(${TARGET_NAME})
# set(CMAKE_CXX_STANDARD 11)
#
# include_directories(duckpgq/include) add_subdirectory(duckpgq/src)
#
# include_directories(../duckdb-pgq/third_party/libpg_query/include)
#
# add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})
#
# set(PARAMETERS "-warnings") build_loadable_extension(${TARGET_NAME}
# ${PARAMETERS} ${EXTENSION_SOURCES})
# =======
set(TARGET_NAME duckpgq)
set(CMAKE_CXX_STANDARD 11)
# DuckDB's extension distribution supports vcpkg. As such, dependencies can be
# added in ./vcpkg.json and then used in cmake with find_package. Feel free to
# remove or replace with other dependencies. Note that it should also be removed
# from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(duckpgq/include)
add_subdirectory(duckpgq/src)
include_directories(../duckdb-pgq/third_party/libpg_query/include)

# set(EXTENSION_SOURCES duckpgq/src/duckpgq_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
# >>>>>>> template/main

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
