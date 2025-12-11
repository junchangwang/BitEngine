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

void BMTableScan::TPCH_Q19_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids)
{
	auto rabit_shipmode = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipmode);
	auto rabit_shipinstruct = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipinstruct);

	std::unordered_map<std::string, int> modeti;
	modeti["REG AIR"] = 0;
	modeti["AIR"] = 1;
	modeti["RAIL"] = 2;
	modeti["SHIP"] = 3;
	modeti["TRUCK"] = 4;
	modeti["MAIL"] = 5;
	modeti["FOB"] = 6;

	std::unordered_map<std::string, int> instructti;
	instructti["DELIVER IN PERSON"] = 0;
	instructti["COLLECT COD"] = 1;
	instructti["NONE"] = 2;
	instructti["TAKE BACK RETURN"] = 3;

    auto s0 = std::chrono::high_resolution_clock::now();

	ibis::bitvector btv_res;
	btv_res.copy(*rabit_shipmode->Btvs[modeti["AIR"]]->btv);
	btv_res.decompress();

	btv_res &= *rabit_shipinstruct->Btvs[instructti["DELIVER IN PERSON"]]->btv;

    auto s1 = std::chrono::high_resolution_clock::now();

	GetRowids(btv_res, row_ids);

    auto s2 = std::chrono::high_resolution_clock::now();

	std::cout << "bitmaps time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;
	std::cout << "get row id : " << std::chrono::duration_cast<std::chrono::milliseconds>(s2 - s1).count() << "ms" << std::endl;

}

SourceResultType BMTableScan::BMTPCH_Q19(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data)
{
    if(*cursor == 0) {
			TPCH_Q19_Lineitem_GetRowIds(context, row_ids);
		}
		if(*cursor < row_ids->size()) {
			vector<StorageIndex> storage_column_ids;

			storage_column_ids.push_back(StorageIndex(1));	// For l_partkey
			storage_column_ids.push_back(StorageIndex(4));	// For l_quantity
			storage_column_ids.push_back(StorageIndex(14));	// For l_shipmode
			storage_column_ids.push_back(StorageIndex(5));	// For l_extendedprice
			storage_column_ids.push_back(StorageIndex(6));	// For l_discount

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