add_subdirectory(iterative_length)
add_subdirectory(shortest_path)
add_subdirectory(local_csr)

set(EXTENSION_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/bfs_state.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/duckpgq_bind.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/logical_path_finding_operator.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/physical_path_finding_operator.cpp
        ${EXTENSION_SOURCES}
        PARENT_SCOPE
)