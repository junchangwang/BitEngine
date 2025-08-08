#pragma once

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/typedefs.hpp"
#include "bitmaps/rabit/segBtv.h"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include <cstdint>
#include <immintrin.h>

namespace duckdb {

class BMTableScan {
public:
    BMTableScan();

    ~BMTableScan();

    vector<row_t> *row_ids;

    idx_t *cursor;

public:
static uint32_t reverseBits(uint32_t x);

static void util_btv_to_id_list(int64_t *base_ptr, uint32_t &base,
									uint64_t idx, uint32_t bits);

static void btv_logic_or(ibis::bitvector *res, ibis::bitvector *rhs);                                    

static void GetRowidsSeg(SegBtv &btv_res, vector<row_t> *row_ids);

void BMTPCH_Q1(ExecutionContext &context, const PhysicalTableScan &op);

void TPCH_Q1_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids);

SourceResultType BMTPCH_Q1(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data);

void TPCH_Q5_Orders_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids);

SourceResultType BMTPCH_Q5(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data);

void TPCH_Q6_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids);

SourceResultType BMTPCH_Q6(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data);

void TPCH_Q14_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids);

SourceResultType BMTPCH_Q14(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data);

};

}