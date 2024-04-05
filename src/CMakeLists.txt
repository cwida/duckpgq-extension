add_subdirectory(functions)
add_subdirectory(operators)

set(EXTENSION_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/duckpgq_extension.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${EXTENSION_SOURCES}
    PARENT_SCOPE
    )
