add_subdirectory(parser)
add_subdirectory(functions)

set(EXTENSION_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/common.cpp
    ${EXTENSION_SOURCES}
    PARENT_SCOPE
    )