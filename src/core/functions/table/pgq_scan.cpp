#include "duckpgq/core/functions/table/pgq_scan.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/property_graph_table.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"
#include "duckpgq/core/utils/duckpgq_utils.hpp"
#include <cstdint>
#include <duckpgq/core/functions/table.hpp>
#include <duckpgq_extension.hpp>

namespace duckpgq {

namespace core {

static void ScanCSREFunction(ClientContext &context, TableFunctionInput &data_p,
                             DataChunk &output) {
  auto state = &data_p.global_state->Cast<CSRScanState>();

  if (state->finished) {
    output.SetCardinality(0);
    return;
  }

  auto duckpgq_state = GetDuckPGQState(context);
  auto csr_id = data_p.bind_data->Cast<CSRScanEData>().csr_id;
  CSR *csr = duckpgq_state->GetCSR(csr_id);

  idx_t vector_size = state->csr_e_offset + DEFAULT_STANDARD_VECTOR_SIZE <= csr->e.size() ?
    DEFAULT_STANDARD_VECTOR_SIZE : csr->e.size() - state->csr_e_offset;

  output.SetCardinality(vector_size);
  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  for (idx_t idx_i = 0; idx_i < vector_size; idx_i++) {
    output.data[0].SetValue(idx_i, Value(csr->e[state->csr_e_offset + idx_i]));
  }

  if (state->csr_e_offset + vector_size >= csr->e.size()) {
    state->finished = true;
  } else {
    state->csr_e_offset += vector_size;
  }
}

static void ScanCSRPtrFunction(ClientContext &context,
                               TableFunctionInput &data_p, DataChunk &output) {
  bool &gstate = ((CSRScanState &)*data_p.global_state).finished;

  if (gstate) {
    output.SetCardinality(0);
    return;
  }

  gstate = true;

  auto duckpgq_state = GetDuckPGQState(context);
  auto csr_id = data_p.bind_data->Cast<CSRScanPtrData>().csr_id;
  CSR *csr = duckpgq_state->GetCSR(csr_id);
  output.SetCardinality(5);
  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<uint64_t>(output.data[0]);
  // now set the result vector
  // the first element is the address of the vertex array
  result_data[0] = (uint64_t)(csr->v);
  // the second element is the address of the edge array
  result_data[1] = (uint64_t)(&(csr->e));
  // here we check the type of the weight array
  // and set the third and fifth element
  // the third element is the address of the weight array
  // the fifth element is the type of the weight array
  // 0 if the weights are integres, 1 if they are doubles, and 2 for unweighted
  if (csr->w.size()) {
    result_data[2] = (uint64_t)(&(csr->w));
    result_data[4] = (uint64_t)(0);
  } else if (csr->w_double.size()) {
    result_data[2] = (uint64_t)(&(csr->w_double));
    result_data[4] = (uint64_t)(1);
  } else {
    result_data[2] = (uint64_t)(0);
    result_data[4] = (uint64_t)(2);
  }
  // we also need the number of elements in the vertex array, since its C-array
  // not a vector.
  result_data[3] = (uint64_t)(csr->vsize);
}

static void ScanCSRVFunction(ClientContext &context, TableFunctionInput &data_p,
                             DataChunk &output) {
  auto state = &data_p.global_state->Cast<CSRScanState>();

  if (state->finished) {
    output.SetCardinality(0);
    return;
  }

  auto duckpgq_state = GetDuckPGQState(context);
  auto csr_id = data_p.bind_data->Cast<CSRScanVData>().csr_id;
  CSR *csr = duckpgq_state->GetCSR(csr_id);

  idx_t vector_size = state->csr_v_offset + DEFAULT_STANDARD_VECTOR_SIZE <= csr->vsize ?
    DEFAULT_STANDARD_VECTOR_SIZE : csr->vsize - state->csr_v_offset;

  output.SetCardinality(vector_size);
  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  for (idx_t idx_i = 0; idx_i < vector_size; idx_i++) {
    output.data[0].SetValue(idx_i, Value(csr->v[state->csr_v_offset + idx_i]));
  }

  if (state->csr_v_offset + vector_size >= csr->vsize) {
    state->finished = true;
  } else {
    state->csr_v_offset += vector_size;
  }
}

static void ScanCSRWFunction(ClientContext &context, TableFunctionInput &data_p,
                             DataChunk &output) {
  auto state = &data_p.global_state->Cast<CSRScanState>();

  if (state->finished) {
    output.SetCardinality(0);
    return;
  }

  auto duckpgq_state = GetDuckPGQState(context);
  auto csr_scanw_data = data_p.bind_data->Cast<CSRScanWData>();
  auto csr_id = csr_scanw_data.csr_id;
  CSR *csr = duckpgq_state->GetCSR(csr_id);

  size_t w_size = 0;
  if (csr_scanw_data.is_double) {
    w_size = csr->w_double.size();
  } else {
    w_size = csr->w.size();
  }

  idx_t vector_size = state->csr_w_offset + DEFAULT_STANDARD_VECTOR_SIZE <= w_size ?
    DEFAULT_STANDARD_VECTOR_SIZE : w_size - state->csr_w_offset;

  output.SetCardinality(vector_size);
  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  if (csr_scanw_data.is_double) {
    for (idx_t idx_i = 0; idx_i < vector_size; idx_i++) {
      output.data[0].SetValue(idx_i, Value(csr->w_double[state->csr_w_offset + idx_i]));
    }
  } else {
    for (idx_t idx_i = 0; idx_i < vector_size; idx_i++) {
      output.data[0].SetValue(idx_i, Value(csr->w[state->csr_w_offset + idx_i]));
    }
  }

  if (state->csr_w_offset + vector_size >= w_size) {
    state->finished = true;
  } else {
    state->csr_w_offset += vector_size;
  }

}

static void ScanPGVTableFunction(ClientContext &context,
                                 TableFunctionInput &data_p,
                                 DataChunk &output) {
  bool &gstate = ((CSRScanState &)*data_p.global_state).finished;

  if (gstate) {
    output.SetCardinality(0);
    return;
  }

  gstate = true;

  auto duckpgq_state = GetDuckPGQState(context);
  auto pg_name = data_p.bind_data->Cast<PGScanVTableData>().pg_name;
  auto pg = duckpgq_state->GetPropertyGraph(pg_name);

  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  auto vtables = FlatVector::GetData<string_t>(output.data[0]);
  idx_t size = 0;
  for (auto &ele : pg->vertex_tables) {
    vtables[size] = string_t(ele->table_name.c_str(), ele->table_name.size());
    size++;
  }
  output.SetCardinality(size);
}

static void ScanPGETableFunction(ClientContext &context,
                                 TableFunctionInput &data_p,
                                 DataChunk &output) {
  bool &gstate = ((CSRScanState &)*data_p.global_state).finished;

  if (gstate) {
    output.SetCardinality(0);
    return;
  }

  gstate = true;

  auto duckpgq_state = GetDuckPGQState(context);
  auto pg_name = data_p.bind_data->Cast<PGScanETableData>().pg_name;
  auto pg = duckpgq_state->GetPropertyGraph(pg_name);

  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  auto etables = FlatVector::GetData<string_t>(output.data[0]);
  idx_t size = 0;
  for (auto &ele : pg->edge_tables) {
    etables[size] = string_t(ele->table_name.c_str(), ele->table_name.size());
    size++;
  }
  output.SetCardinality(size);
}

shared_ptr<PropertyGraphTable>
find_table_entry(const vector<shared_ptr<PropertyGraphTable>> &vec,
                 string &table_name) {
  for (auto &&entry : vec) {
    if (entry->table_name == table_name) {
      return entry;
    }
  }
  throw BinderException("Table name %s does not exist", table_name);
}

static void ScanPGVColFunction(ClientContext &context,
                               TableFunctionInput &data_p, DataChunk &output) {
  bool &gstate = ((CSRScanState &)*data_p.global_state).finished;

  if (gstate) {
    output.SetCardinality(0);
    return;
  }

  gstate = true;

  auto duckpgq_state = GetDuckPGQState(context);
  auto scan_v_col_data = data_p.bind_data->Cast<PGScanVColData>();
  auto pg_name = scan_v_col_data.pg_name;
  auto table_name = scan_v_col_data.table_name;
  auto pg = duckpgq_state->GetPropertyGraph(pg_name);

  auto table_entry = find_table_entry(pg->vertex_tables, table_name);

  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  auto colsdata = FlatVector::GetData<string_t>(output.data[0]);
  idx_t size = 0;
  for (auto &ele : table_entry->column_names) {
    colsdata[size] = string_t(ele.c_str(), ele.size());
    size++;
  }
  output.SetCardinality(size);
}

static void ScanPGEColFunction(ClientContext &context,
                               TableFunctionInput &data_p, DataChunk &output) {
  bool &gstate = ((CSRScanState &)*data_p.global_state).finished;

  if (gstate) {
    output.SetCardinality(0);
    return;
  }

  gstate = true;

  auto duckpgq_state = GetDuckPGQState(context);
  auto pg_scan_e_col_data = data_p.bind_data->Cast<PGScanEColData>();
  auto pg_name = pg_scan_e_col_data.pg_name;
  auto table_name = pg_scan_e_col_data.table_name;
  auto pg = duckpgq_state->GetPropertyGraph(pg_name);

  auto table_entry = find_table_entry(pg->edge_tables, table_name);

  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  auto colsdata = FlatVector::GetData<string_t>(output.data[0]);
  idx_t size = 0;
  for (auto &ele : table_entry->column_names) {
    colsdata[size] = string_t(ele.c_str(), ele.size());
    size++;
  }
  output.SetCardinality(size);
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------
void CoreTableFunctions::RegisterScanTableFunctions(DatabaseInstance &db) {
  ExtensionUtil::RegisterFunction(
      db, TableFunction("get_csr_e", {LogicalType::INTEGER}, ScanCSREFunction,
                        CSRScanEData::ScanCSREBind, CSRScanState::Init));

  ExtensionUtil::RegisterFunction(
      db, TableFunction("get_csr_v", {LogicalType::INTEGER}, ScanCSRVFunction,
                        CSRScanVData::ScanCSRVBind, CSRScanState::Init));

  ExtensionUtil::RegisterFunction(
      db, TableFunction("get_csr_w", {LogicalType::INTEGER}, ScanCSRWFunction,
                        CSRScanWData::ScanCSRWBind, CSRScanState::Init));

  ExtensionUtil::RegisterFunction(
      db,
      TableFunction("get_pg_vtablenames", {LogicalType::VARCHAR},
                    ScanPGVTableFunction, PGScanVTableData::ScanPGVTableBind,
                    CSRScanState::Init));

  ExtensionUtil::RegisterFunction(
      db, TableFunction("get_pg_vcolnames",
                        {LogicalType::VARCHAR, LogicalType::VARCHAR},
                        ScanPGVColFunction, PGScanVColData::ScanPGVColBind,
                        CSRScanState::Init));

  ExtensionUtil::RegisterFunction(
      db,
      TableFunction("get_csr_ptr", {LogicalType::INTEGER}, ScanCSRPtrFunction,
                    CSRScanPtrData::ScanCSRPtrBind, CSRScanState::Init));

  ExtensionUtil::RegisterFunction(
      db,
      TableFunction("get_pg_etablenames", {LogicalType::VARCHAR},
                    ScanPGETableFunction, PGScanETableData::ScanPGETableBind,
                    CSRScanState::Init));

  ExtensionUtil::RegisterFunction(
      db, TableFunction("get_pg_ecolnames",
                        {LogicalType::VARCHAR, LogicalType::VARCHAR},
                        ScanPGEColFunction, PGScanEColData::ScanPGEColBind,
                        CSRScanState::Init));
}
} // namespace core

} // namespace duckpgq
