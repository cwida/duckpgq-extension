add_subdirectory(functions)
add_subdirectory(operators)
add_subdirectory(utils)

set(EXTENSION_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/duckpgq_extension.cpp
    ${EXTENSION_SOURCES}
    PARENT_SCOPE
    )
