add_subdirectory(duckpgq)

set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES}
    ${CMAKE_CURRENT_SOURCE_DIR}/duckpgq_extension.cpp
    PARENT_SCOPE)