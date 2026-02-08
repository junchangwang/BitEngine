#pragma once

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/typedefs.hpp"
#include "bitmaps/rabit/segBtv.h"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include <cstdint>
#include <immintrin.h>
#include <netinet/in.h>

namespace duckdb {

class BMHashJoin {
public:
    BMHashJoin();

    ~BMHashJoin();

    vector<row_t> *row_ids;

    idx_t *cursor;

    idx_t num_idlist;

public:
    static uint32_t reverseBits(uint32_t x);

    static void util_btv_to_id_list(int64_t *base_ptr, uint32_t &base,
									uint64_t idx, uint32_t bits);

    static void GetRowids(ibis::bitvector &btv_res, std::vector<row_t> *row_ids);

    static string getJoinName(const duckdb::PhysicalHashJoin* comp, string mode);


    
    OperatorResultType probeBitmap(ExecutionContext &context, JoinHashTable &build_ht, const PhysicalHashJoin &op, const string &probe_col_name);


};


}