#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckpgq/core/pragma/duckpgq_pragma.hpp>

namespace duckpgq {

    namespace core {

        static string PragmaCreateVertexTable(ClientContext &context,
                                               const FunctionParameters &parameters) {
            if (parameters.values.size() != 5) {
                throw InvalidInputException("PRAGMA create_vertex_table requires exactly four parameters: edge_table, source_column, destination_column, id_column_name");
            }

            string edge_table = parameters.values[0].GetValue<string>();
            string source_column = parameters.values[1].GetValue<string>();
            string destination_column = parameters.values[2].GetValue<string>();
            string vertex_table_name = parameters.values[3].GetValue<string>();
            string id_column_name = parameters.values[4].GetValue<string>();

            auto result_query = "CREATE TABLE " + vertex_table_name + " AS " +
                   "SELECT DISTINCT " + id_column_name + " FROM " +
                   "(SELECT " + source_column + " AS " + id_column_name + " FROM " + edge_table +
                   " UNION ALL " +
                   "SELECT " + destination_column + " AS " + id_column_name + " FROM " + edge_table + ")";
            return result_query;
        }

        void CorePGQPragma::RegisterCreateVertexTable(duckdb::DatabaseInstance &instance) {
            // Define the pragma function
            auto pragma_func = PragmaFunction::PragmaCall(
                    "create_vertex_table",   // Name of the pragma
                    PragmaCreateVertexTable, // Query substitution function
                    {
                        LogicalType::VARCHAR, // Edge table
                        LogicalType::VARCHAR, // Source column
                        LogicalType::VARCHAR, // Destination column
                        LogicalType::VARCHAR, // Vertex table name
                        LogicalType::VARCHAR  // ID column name
                    }
            );

            // Register the pragma function
            ExtensionUtil::RegisterFunction(instance, pragma_func);
        }

    } // namespace core

} // namespace duckpgq
