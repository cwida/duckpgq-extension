add_subdirectory(parsed_data)

set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES}
#    ${CMAKE_CURRENT_SOURCE_DIR}/transformer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/property_graph_table.cpp

        PARENT_SCOPE
)