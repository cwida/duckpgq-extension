add_subdirectory(function_data)
add_subdirectory(scalar)
add_subdirectory(tablefunctions)

set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES}
    PARENT_SCOPE
    )