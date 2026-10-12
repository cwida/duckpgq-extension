set(DUCKPGQ_GRAMMAR_TEMPLATE
    ${CMAKE_CURRENT_SOURCE_DIR}/grammar/grammar_rules.hpp.in)

# Embed a one-rule-per-non-comment-line grammar file as a string array in a
# generated header. Only the text is embedded, without interpreting PEG.
function(duckpgq_embed_grammar GRAM_FILE HEADER_NAME ARRAY_NAME)
  set(GRAM_PATH ${CMAKE_CURRENT_SOURCE_DIR}/grammar/${GRAM_FILE})
  set_property(
    DIRECTORY
    APPEND
    PROPERTY CMAKE_CONFIGURE_DEPENDS ${GRAM_PATH})
  file(STRINGS ${GRAM_PATH} GRAMMAR_LINES)
  set(DUCKPGQ_GRAMMAR_RULES "")
  foreach(GRAMMAR_LINE IN LISTS GRAMMAR_LINES)
    string(STRIP "${GRAMMAR_LINE}" GRAMMAR_LINE)
    if(GRAMMAR_LINE MATCHES "^#|^$")
      continue()
    endif()
    string(APPEND DUCKPGQ_GRAMMAR_RULES
           "\tR\"duckpgq_rule(${GRAMMAR_LINE})duckpgq_rule\",\n")
  endforeach()
  set(DUCKPGQ_GRAMMAR_SOURCE ${GRAM_FILE})
  set(DUCKPGQ_GRAMMAR_ARRAY ${ARRAY_NAME})
  configure_file(${DUCKPGQ_GRAMMAR_TEMPLATE}
                 ${DUCKPGQ_GENERATED_INCLUDE_DIR}/${HEADER_NAME} @ONLY)
endfunction()

duckpgq_embed_grammar(
  create_property_graph.gram duckpgq_create_property_graph_grammar.hpp
  DUCKPGQ_CREATE_PROPERTY_GRAPH_RULES)
duckpgq_embed_grammar(graph_table.gram duckpgq_graph_table_grammar.hpp
                      DUCKPGQ_GRAPH_TABLE_RULES)

set(EXTENSION_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/create_property_graph_grammar.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckpgq_grammar_extension.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckpgq_parser.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/graph_table_grammar.cpp
    ${EXTENSION_SOURCES}
    PARENT_SCOPE)
