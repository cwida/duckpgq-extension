add_subdirectory(functions)
add_subdirectory(operator)
add_subdirectory(option)
add_subdirectory(parser)
add_subdirectory(utils)


set(EXTENSION_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/module.cpp
        ${EXTENSION_SOURCES}
        PARENT_SCOPE
    )