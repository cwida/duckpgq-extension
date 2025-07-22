add_subdirectory(core)

set(EXTENSION_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/duckpgq_extension.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckpgq_state.cpp ${EXTENSION_SOURCES}
    PARENT_SCOPE)
