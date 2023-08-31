#include <duckpgq_extension.hpp>
#include "duckpgq/functions/tablefunctions/pgq_scan.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/duckpgq_functions.hpp"
#include "duckdb/common/compressed_sparse_row.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_property_graph_info.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/property_graph_table.hpp"

namespace duckdb {

static void ScanCSREFunction(ClientContext &context, TableFunctionInput &data_p,
                             DataChunk &output) {
  bool &gstate = ((CSRScanState &)*data_p.global_state).finished;

  if (gstate) {
    output.SetCardinality(0);
    return;
  }

  gstate = true;

  auto duckpgq_state_entry = context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());
  auto csr_id = data_p.bind_data->Cast<CSRScanEData>().csr_id;
  CSR *csr = duckpgq_state->GetCSR(csr_id);
  output.SetCardinality(csr->e.size());
  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  FlatVector::SetData(output.data[0], (data_ptr_t)csr->e.data());
}

static void ScanCSRVFunction(ClientContext &context, TableFunctionInput &data_p,
                             DataChunk &output) {
  bool &gstate = ((CSRScanState &)*data_p.global_state).finished;

  if (gstate) {
    output.SetCardinality(0);
    return;
  }

  gstate = true;

  auto duckpgq_state_entry = context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());
  auto csr_id = data_p.bind_data->Cast<CSRScanVData>().csr_id;
  CSR *csr = duckpgq_state->GetCSR(csr_id);
  output.SetCardinality(csr->vsize);
  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  FlatVector::SetData(output.data[0],
                      (data_ptr_t)(reinterpret_cast<int64_t *>(csr->v)));
}

static void ScanCSRWFunction(ClientContext &context, TableFunctionInput &data_p,
                             DataChunk &output) {
  bool &gstate = ((CSRScanState &)*data_p.global_state).finished;

  if (gstate) {
    output.SetCardinality(0);
    return;
  }

  gstate = true;

  auto duckpgq_state_entry = context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());
  auto csr_scanw_data = data_p.bind_data->Cast<CSRScanWData>();
  auto csr_id = csr_scanw_data.csr_id;
  CSR *csr = duckpgq_state->GetCSR(csr_id);
  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  if (csr_scanw_data.is_double) {
    output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
    FlatVector::SetData(output.data[0], (data_ptr_t)csr->w_double.data());
  } else {
    output.SetCardinality(csr->w.size());
    FlatVector::SetData(output.data[0], (data_ptr_t)csr->w.data());
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

  auto duckpgq_state_entry = context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());
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

  auto duckpgq_state_entry = context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());
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

  auto duckpgq_state_entry = context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());
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

  auto duckpgq_state_entry = context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());
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

static void ScanCSRWDoubleFunction(ClientContext &context,
                                   TableFunctionInput &data_p,
                                   DataChunk &output) {
  bool &gstate = ((CSRScanState &)*data_p.global_state).finished;

  if (gstate) {
    output.SetCardinality(0);
    return;
  }

  gstate = true;

  auto duckpgq_state_entry = context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());
  auto csr_id = data_p.bind_data->Cast<CSRScanEData>().csr_id;
  CSR *csr = duckpgq_state->GetCSR(csr_id);
  output.SetCardinality(csr->w_double.size());
  output.data[0].SetVectorType(VectorType::FLAT_VECTOR);
  FlatVector::SetData(output.data[0], (data_ptr_t)csr->w_double.data());
}

CreateTableFunctionInfo DuckPGQFunctions::GetScanCSREFunction() {
  TableFunctionSet function_set("get_csr_e");

  function_set.AddFunction(
      TableFunction({LogicalType::INTEGER}, ScanCSREFunction,
                    CSRScanEData::ScanCSREBind, CSRScanState::Init));
  return CreateTableFunctionInfo(function_set);
}

CreateTableFunctionInfo DuckPGQFunctions::GetScanCSRVFunction() {
  TableFunctionSet function_set("get_csr_v");

  function_set.AddFunction(
      TableFunction({LogicalType::INTEGER}, ScanCSRVFunction,
                    CSRScanVData::ScanCSRVBind, CSRScanState::Init));
  return CreateTableFunctionInfo(function_set);
}

CreateTableFunctionInfo DuckPGQFunctions::GetScanCSRWFunction() {
  TableFunctionSet function_set("get_csr_w");

  function_set.AddFunction(
      TableFunction({LogicalType::INTEGER}, ScanCSRWFunction,
                    CSRScanWData::ScanCSRWBind, CSRScanState::Init));
  return CreateTableFunctionInfo(function_set);
}

CreateTableFunctionInfo DuckPGQFunctions::GetScanPGVTableFunction() {
  TableFunctionSet function_set("get_pg_vtablenames");

  function_set.AddFunction(
      TableFunction({LogicalType::VARCHAR}, ScanPGVTableFunction,
                    PGScanVTableData::ScanPGVTableBind, CSRScanState::Init));
  return CreateTableFunctionInfo(function_set);
}

CreateTableFunctionInfo DuckPGQFunctions::GetScanPGVColFunction() {
  TableFunctionSet function_set("get_pg_vcolnames");

  function_set.AddFunction(TableFunction(
      {LogicalType::VARCHAR, LogicalType::VARCHAR}, ScanPGVColFunction,
      PGScanVColData::ScanPGVColBind, CSRScanState::Init));
  return CreateTableFunctionInfo(function_set);
}

CreateTableFunctionInfo DuckPGQFunctions::GetScanPGETableFunction() {
  TableFunctionSet function_set("get_pg_etablenames");

  function_set.AddFunction(
      TableFunction({LogicalType::VARCHAR}, ScanPGETableFunction,
                    PGScanETableData::ScanPGETableBind, CSRScanState::Init));
  return CreateTableFunctionInfo(function_set);
}

CreateTableFunctionInfo DuckPGQFunctions::GetScanPGEColFunction() {
  TableFunctionSet function_set("get_pg_ecolnames");

  function_set.AddFunction(TableFunction(
      {LogicalType::VARCHAR, LogicalType::VARCHAR}, ScanPGEColFunction,
      PGScanEColData::ScanPGEColBind, CSRScanState::Init));
  return CreateTableFunctionInfo(function_set);
}

} // namespace duckdb
