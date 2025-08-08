#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution/tpch/bitmap_table_scan.hpp"
#include "bitmaps/rabit/table.h"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include <iostream>
#include <chrono>

namespace duckdb{

void BMTableScan::TPCH_Q5_Orders_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids)
{
    auto rabit_orderdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderdate);
    
    int c_year = 1994;
    int start_year = 1992;
    //'1994-01-01'
    // int lower_days = 8766;
    //'1995-01-01' - 1
    // int upper_days = 9130;
    
    auto s0 = std::chrono::high_resolution_clock::now();

    SegBtv* btv_internal = rabit_orderdate->Btvs_GE[c_year - start_year]->seg_btv;
    SegBtv* btv_orderdate = new SegBtv(*btv_internal);
    btv_orderdate->deepCopy(*btv_internal);
    btv_orderdate->decompress();

    // SegBtv *btv_orderdate = rabit_orderdate->range_res(0, lower_days, upper_days - lower_days);

    auto s1 = std::chrono::high_resolution_clock::now();

    auto &btv_res = *btv_orderdate;

    GetRowidsSeg(btv_res, row_ids);

    auto s2 = std::chrono::high_resolution_clock::now();
    if(btv_orderdate){
        delete btv_orderdate;
        btv_orderdate = nullptr;
    }
    
    std::cout << "orderdate's bitmaps time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;
    std::cout << "get row id : " << std::chrono::duration_cast<std::chrono::milliseconds>(s2 - s1).count() << "ms" << std::endl;
}

SourceResultType BMTableScan::BMTPCH_Q5(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data)
{
    if(*cursor == 0) {
        TPCH_Q5_Orders_GetRowIds(context, row_ids);
    }
    if(*cursor < row_ids->size()) {
        vector<StorageIndex> storage_column_ids;

        storage_column_ids.push_back(StorageIndex(1));	// For o_custkey
        storage_column_ids.push_back(StorageIndex(0));	// For o_orderkey

        TableScanState local_storage_state;
        local_storage_state.Initialize(storage_column_ids);
        ColumnFetchState column_fetch_state;

        auto &table_bind_data = bind_data;
        auto &transaction = DuckTransaction::Get(context.client, table_bind_data.table.catalog);

        data_ptr_t row_ids_data = nullptr;
        row_ids_data = (data_ptr_t)&((*row_ids)[*cursor]);
        Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
        idx_t fetch_count = 2048;
        if(*cursor + fetch_count > row_ids->size()) {
            fetch_count = row_ids->size() - *cursor;
        }

        table_bind_data.table.GetStorage().Fetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
                                                column_fetch_state);
        *cursor += fetch_count;
        
        return SourceResultType::HAVE_MORE_OUTPUT;
    }
    else {
        row_ids->clear();
        *cursor = 0;
        context.client.query_source = "tpch";

        return SourceResultType::FINISHED;
    }
}

}