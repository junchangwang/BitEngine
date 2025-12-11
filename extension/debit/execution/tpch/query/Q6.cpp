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

namespace duckdb {

void BMTableScan::TPCH_Q6_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids)
{
	auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);
	auto rabit_discount = dynamic_cast<rabit::Rabit *>(context.client.bitmap_discount);
	auto rabit_quantity = dynamic_cast<rabit::Rabit *>(context.client.bitmap_quantity);

	int c_year = 1994;
	int start_year = 1970;

	int lower_discount = 5;
	int upper_discount = 7;

	int upper_quantity = 23;

	auto s0 = std::chrono::high_resolution_clock::now();

	SegBtv* btv_internal = rabit_shipdate->Btvs_GE[c_year - start_year]->seg_btv;
	SegBtv* btv_shipdate = new SegBtv(*btv_internal);
	btv_shipdate->deepCopy(*btv_internal);
	btv_shipdate->decompress();

	auto s1 = std::chrono::high_resolution_clock::now();

	SegBtv *btv_discount = rabit_discount->range_res(0, lower_discount, upper_discount - lower_discount + 1);

	auto s2 = std::chrono::high_resolution_clock::now();

	SegBtv *btv_quantity = rabit_quantity->range_res(0, 0, upper_quantity + 1);

	auto s3 = std::chrono::high_resolution_clock::now();
    
	auto &btv_res = *btv_shipdate;
	btv_res._and_in_thread(btv_discount, 0, btv_res.seg_table.size());
	btv_res._and_in_thread(btv_quantity, 0, btv_res.seg_table.size());

	auto s4 = std::chrono::high_resolution_clock::now();
	
	GetRowidsSeg(btv_res, row_ids);

	auto s5 = std::chrono::high_resolution_clock::now();

    if(btv_shipdate){
        delete btv_shipdate;
        btv_shipdate = nullptr;
    }
    if(btv_discount){
        delete btv_discount;
        btv_discount = nullptr;
    }
    if(btv_quantity){
        delete btv_quantity;
        btv_quantity = nullptr;
    }

	std::cout << "shipdate's bitmap time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;
	std::cout << "discount's bitmap time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s2 - s1).count() << "ms" << std::endl;
	std::cout << "quantity's bitmap time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s3 - s2).count() << "ms" << std::endl;
	std::cout << "bitmaps time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s4 - s0).count() << "ms" << std::endl;
	std::cout << "get row id : " << std::chrono::duration_cast<std::chrono::milliseconds>(s5 - s4).count() << "ms" << std::endl;

}

SourceResultType BMTableScan::BMTPCH_Q6(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data)
{
    if(*cursor == 0) {
			TPCH_Q6_Lineitem_GetRowIds(context, row_ids);
			num_idlist = row_ids->size();
		}
		
		if(*cursor < row_ids->size()) {

			vector<StorageIndex> storage_column_ids;

			storage_column_ids.push_back(StorageIndex(6));	// For l_discount
			storage_column_ids.push_back(StorageIndex(5));	// For l_extendedprice

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

			table_bind_data.table.GetStorage().BMFetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
                                                column_fetch_state, num_idlist);
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