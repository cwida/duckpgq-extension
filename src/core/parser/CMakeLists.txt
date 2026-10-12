set(DUCKPGQ_CREATE_GRAPH_GRAMMAR
    ${CMAKE_CURRENT_SOURCE_DIR}/grammar/create_property_graph.gram)
set_property(
  DIRECTORY
  APPEND
  PROPERTY CMAKE_CONFIGURE_DEPENDS ${DUCKPGQ_CREATE_GRAPH_GRAMMAR})

# Keep one rule per non-comment line; only embed the text, without interpreting
# PEG.
file(STRINGS ${DUCKPGQ_CREATE_GRAPH_GRAMMAR} DUCKPGQ_GRAMMAR_LINES)
set(DUCKPGQ_GRAMMAR_RULES "")
foreach(DUCKPGQ_GRAMMAR_LINE IN LISTS DUCKPGQ_GRAMMAR_LINES)
  string(STRIP "${DUCKPGQ_GRAMMAR_LINE}" DUCKPGQ_GRAMMAR_LINE)
  if(DUCKPGQ_GRAMMAR_LINE MATCHES "^#|^$")
    continue()
  endif()
  string(APPEND DUCKPGQ_GRAMMAR_RULES
         "\tR\"duckpgq_rule(${DUCKPGQ_GRAMMAR_LINE})duckpgq_rule\",\n")
endforeach()
configure_file(
  grammar/create_property_graph.hpp.in
  ${DUCKPGQ_GENERATED_INCLUDE_DIR}/duckpgq_create_property_graph_grammar.hpp
  @ONLY)

set(EXTENSION_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/create_property_graph_grammar.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckpgq_parser.cpp ${EXTENSION_SOURCES}
    PARENT_SCOPE)
