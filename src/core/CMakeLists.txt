add_subdirectory(function_data)
add_subdirectory(scalar)
add_subdirectory(tablefunctions)

set(EXTENSION_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/module.cpp
        ${EXTENSION_SOURCES}
        PARENT_SCOPE
    )