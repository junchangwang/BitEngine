#pragma once

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/typedefs.hpp"
#include "bitmaps/rabit/segBtv.h"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include <cstdint>
#include <immintrin.h>
#include <netinet/in.h>

namespace duckdb {

extern std::vector<uint32_t> g_idlist;
extern std::vector<uint32_t> sizes;
extern int64_t sum_q6;
extern unsigned char reverse_table[256];

class BMTableScan {
public:
    BMTableScan();

    ~BMTableScan();

    vector<row_t> *row_ids;

    idx_t *cursor;

    idx_t num_idlist;

public:
static uint32_t reverseBits(uint32_t x);

static void util_btv_to_id_list(int64_t *base_ptr, uint32_t &base,
									uint64_t idx, uint32_t bits);

static void btv_logic_or(ibis::bitvector *res, ibis::bitvector *rhs);

static void GetRowids(ibis::bitvector &btv_res, vector<row_t> *row_ids);

static void GetRowidsSeg(SegBtv &btv_res, vector<row_t> *row_ids);

void bm_gather_i64_from_i32_idx(const int64_t* A, const uint32_t* B, int n, int64_t* out);

void bm_exe_aggregation(int64_t *price_ptr, int64_t *discount_ptr, uint16_t base, int64_t &sum);

vector<uint32_t>* reduce_leadingbits(ibis::bitvector &ttt_res);
vector<uint32_t>* reduce_leadingbits_seg(SegBtv &ttt_res);



void BMTPCH_Q1(ExecutionContext &context, const PhysicalTableScan &op);

void BMTPCH_Q3(ExecutionContext &context, const PhysicalTableScan &op);

void BMTPCH_Q4(ExecutionContext &context, const PhysicalTableScan &op);

void BMTPCH_Q5(ExecutionContext &context, const PhysicalTableScan &op);

void TPCH_Q6_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids);

SourceResultType BMTPCH_Q6(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data);

void TPCH_Q8_Orders_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids);

SourceResultType BMTPCH_Q8(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data);

void BMTPCH_Q10(ExecutionContext &context, const PhysicalTableScan &op);

void BMTPCH_Q12(ExecutionContext &context, const PhysicalTableScan &op);

void TPCH_Q14_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids);

SourceResultType BMTPCH_Q14(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data);

void TPCH_Q15_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids);

SourceResultType BMTPCH_Q15(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data);

void BMTPCH_Q17(ExecutionContext &context, const PhysicalTableScan &op);

void TPCH_Q19_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids);

SourceResultType BMTPCH_Q19(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data);



void CheckTime(ExecutionContext &context, vector<row_t> *row_ids);

void BMGather(ExecutionContext &context, const PhysicalTableScan &op, std::vector<uint32_t>* sizes, std::vector<uint32_t>* g_idlist);

void Debit_SIMD(ExecutionContext &context, const PhysicalTableScan &op);

void Groupby_Test(ExecutionContext &context, const PhysicalTableScan &op);

void DuckDB_SIMD(ExecutionContext &context, const PhysicalTableScan &op, std::vector<uint32_t>* btv_res);

void Selective_Gather(ExecutionContext &context, const PhysicalTableScan &op, std::vector<uint32_t>* idlist, std::vector<uint32_t>* tuple_counts);

void HashJoin(ExecutionContext &context, const PhysicalTableScan &op);

};

}