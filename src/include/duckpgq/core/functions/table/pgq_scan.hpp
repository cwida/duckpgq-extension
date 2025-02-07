/* we treat csr -> v, csr -> e and csr -> w as tables.
 * The col names and tables names are also treated as tables.
 * And later, we define some table function to get graph-related data.
 *
 * This header defines all the structs and classes used later.
 */

#pragma once
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckpgq/core/utils/compressed_sparse_row.hpp"

#include <duckpgq_state.hpp>

namespace duckpgq {

namespace core {

struct CSRScanVData : TableFunctionData {
public:
  static unique_ptr<FunctionData>
  ScanCSRVBind(ClientContext &context, TableFunctionBindInput &input,
               vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<CSRScanVData>();
    result->csr_id = input.inputs[0].GetValue<int32_t>();
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("csrv");
    return std::move(result);
  }

public:
  int32_t csr_id;
};

struct CSRScanPtrData : public TableFunctionData {
public:
  static unique_ptr<FunctionData>
  ScanCSRPtrBind(ClientContext &context, TableFunctionBindInput &input,
                 vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<CSRScanPtrData>();
    result->csr_id = input.inputs[0].GetValue<int32_t>();
    return_types.emplace_back(LogicalType::UBIGINT);
    names.emplace_back("ptr");
    return std::move(result);
  }

public:
  int32_t csr_id;
};

struct CSRScanEData : public TableFunctionData {
public:
  static unique_ptr<FunctionData>
  ScanCSREBind(ClientContext &context, TableFunctionBindInput &input,
               vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<CSRScanEData>();
    result->csr_id = input.inputs[0].GetValue<int32_t>();
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("csre");
    return std::move(result);
  }

public:
  int32_t csr_id;
};

struct CSRScanWData : public TableFunctionData {
public:
  static unique_ptr<FunctionData>
  ScanCSRWBind(ClientContext &context, TableFunctionBindInput &input,
               vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<CSRScanWData>();
    result->csr_id = input.inputs[0].GetValue<int32_t>();

    auto duckpgq_state = context.registered_state->Get<DuckPGQState>("duckpgq");
    if (!duckpgq_state) {
      //! Wondering how you can get here if the extension wasn't loaded, but
      //! leaving this check in anyways
      throw InternalException("The DuckPGQ extension has not been loaded");
    }

    CSR *csr = duckpgq_state->GetCSR(result->csr_id);

    if (!csr->w.empty()) {
      result->is_double = false;
      return_types.emplace_back(LogicalType::BIGINT);
    } else {
      result->is_double = true;
      return_types.emplace_back(LogicalType::DOUBLE);
    }
    names.emplace_back("csrw");
    return std::move(result);
  }

public:
  int32_t csr_id;
  bool is_double;
};

struct CSRScanWDoubleData : public TableFunctionData {
public:
  static unique_ptr<FunctionData>
  ScanCSRWDoubleBind(ClientContext &context, TableFunctionBindInput &input,
                     vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<CSRScanWDoubleData>();
    result->csr_id = input.inputs[0].GetValue<int32_t>();
    return_types.emplace_back(LogicalType::DOUBLE);
    names.emplace_back("csrw");
    return std::move(result);
  }

public:
  int32_t csr_id;
};

struct PGScanVTableData : public TableFunctionData {
public:
  static unique_ptr<FunctionData>
  ScanPGVTableBind(ClientContext &context, TableFunctionBindInput &input,
                   vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<PGScanVTableData>();
    result->pg_name = StringValue::Get(input.inputs[0]);
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("vtables");
    return std::move(result);
  }

public:
  string pg_name;
};

struct PGScanVColData : public TableFunctionData {
public:
  static unique_ptr<FunctionData>
  ScanPGVColBind(ClientContext &context, TableFunctionBindInput &input,
                 vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<PGScanVColData>();
    result->pg_name = StringValue::Get(input.inputs[0]);
    result->table_name = StringValue::Get(input.inputs[1]);
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("colnames");
    return std::move(result);
  }

public:
  string pg_name;
  string table_name;
};

struct PGScanETableData : public TableFunctionData {
public:
  static unique_ptr<FunctionData>
  ScanPGETableBind(ClientContext &context, TableFunctionBindInput &input,
                   vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<PGScanETableData>();
    result->pg_name = StringValue::Get(input.inputs[0]);
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("etables");
    return std::move(result);
  }

public:
  string pg_name;
};

struct PGScanEColData : public TableFunctionData {
public:
  static unique_ptr<FunctionData>
  ScanPGEColBind(ClientContext &context, TableFunctionBindInput &input,
                 vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<PGScanEColData>();
    result->pg_name = StringValue::Get(input.inputs[0]);
    result->table_name = StringValue::Get(input.inputs[1]);
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("colnames");
    return std::move(result);
  }

public:
  string pg_name;
  string table_name;
};

struct CSRScanState : public GlobalTableFunctionState {
public:
  static unique_ptr<GlobalTableFunctionState>
  Init(ClientContext &context, TableFunctionInitInput &input) {
    auto result = make_uniq<CSRScanState>();
    result->csr_v_offset = 0;
    result->csr_e_offset = 0;
    result->csr_w_offset = 0;
    return std::move(result);
  }

public:
  bool finished = false;
  idx_t csr_v_offset;
  idx_t csr_e_offset;
  idx_t csr_w_offset;
};

} // namespace core

} // namespace duckpgq
