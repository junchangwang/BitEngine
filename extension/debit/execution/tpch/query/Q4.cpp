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
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <iostream>
#include <iomanip>
#include <chrono>
#include <netinet/in.h>

namespace duckdb {

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op) {
		if (op.dynamic_filters && op.dynamic_filters->HasFilters()) {
			table_filters = op.dynamic_filters->GetFinalTableFilters(op, op.table_filters.get());
		}

		if (op.function.init_global) {
			auto filters = table_filters ? *table_filters : GetTableFilters(op);
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, filters,
			                             op.extra_info.sample_options);

			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
		if (op.function.in_out_function) {
			// this is an in-out function, we need to setup the input chunk
			vector<LogicalType> input_types;
			for (auto &param : op.parameters) {
				input_types.push_back(param.type());
			}
			input_chunk.Initialize(context, input_types);
			for (idx_t c = 0; c < op.parameters.size(); c++) {
				input_chunk.data[c].Reference(op.parameters[c]);
			}
			input_chunk.SetCardinality(1);
		}
	}

	idx_t max_threads = 0;
	unique_ptr<GlobalTableFunctionState> global_state;
	bool in_out_final = false;
	DataChunk input_chunk;
	//! Combined table filters, if we have dynamic filters
	unique_ptr<TableFilterSet> table_filters;

	optional_ptr<TableFilterSet> GetTableFilters(const PhysicalTableScan &op) const {
		return table_filters ? table_filters.get() : op.table_filters.get();
	}
	idx_t MaxThreads() override {
		return max_threads;
	}
};

void BMTableScan::BMTPCH_Q4(ExecutionContext &context, const PhysicalTableScan &op)
{
    auto s0 = std::chrono::high_resolution_clock::now();

	auto &orders_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "orders");
	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");

	int32_t left_days = 8582;
	int32_t right_days = 8673;

	std::unordered_map<int32_t, std::string> priority_map;
	std::unordered_map<std::string, int32_t> sti_map;
	priority_map[5] = "5-LOW";
	priority_map[1] = "1-URGENT";
	priority_map[4] = "4-NOT SPECIFIED";
	priority_map[2] = "2-HIGH";
	priority_map[3] = "3-MEDIUM";
	sti_map["5-LOW"] = 5;
	sti_map["1-URGENT"] = 1;
	sti_map["4-NOT SPECIFIED"] = 4;
	sti_map["2-HIGH"] = 2;
	sti_map["3-MEDIUM"] = 3;


	std::unordered_map<int64_t, int32_t> orderkey_map;
	orderkey_map.reserve(580000);
	{
		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
		TableScanState orders_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(4));
		storage_column_ids.push_back(StorageIndex(5));
		orders_table.GetStorage().InitializeScan(context.client, orders_transaction, orders_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[4]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[5]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			orders_table.GetStorage().Scan(orders_transaction, result, orders_scan_state);
			if(result.size() == 0)
				break;

			auto order_key_data = FlatVector::GetData<int64_t>(result.data[0]);
			auto date_data = FlatVector::GetData<int32_t>(result.data[1]);

			if(result.data[2].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &sel_vec = DictionaryVector::SelVector(result.data[2]);
				auto &child_vec = DictionaryVector::Child(result.data[2]);
				for(int i = 0; i < result.size(); i++) {
					if(date_data[i] >= left_days && date_data[i] <= right_days)
						orderkey_map[order_key_data[i]] = sti_map[reinterpret_cast<string_t *>(child_vec.GetData())[sel_vec.get_index(i)].GetString()];
				}
			}
			else {
				for(int i = 0; i < result.size(); i++) {
					if(date_data[i] >= left_days && date_data[i] <= right_days)
						orderkey_map[order_key_data[i]] = sti_map[reinterpret_cast<string_t *>(result.data[2].GetData())[i].GetString()];
				}
			}
		}
	}

	auto s1 = std::chrono::high_resolution_clock::now();

	long long time_ids = 0;
	long long time_fetch = 0;
	long long time_bitmap = 0;
	std::unordered_set<int64_t> orderkey_set;
	orderkey_set.reserve(570000);
	{
		auto rabit_orderkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderkey);
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(11));
		storage_column_ids.push_back(StorageIndex(12));
		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[11]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[12]);

		auto s_bitmap = std::chrono::high_resolution_clock::now();
		ibis::bitvector btv_res;
		auto it1 = orderkey_map.begin();
		btv_res.copy(*rabit_orderkey->Btvs[it1->first]->btv);
		btv_res.decompress();
		it1++;
		while(it1 != orderkey_map.end()) {
			btv_res |= *rabit_orderkey->Btvs[it1->first]->btv;
			it1++;
		}

		auto s_ids = std::chrono::high_resolution_clock::now();

		time_bitmap = std::chrono::duration_cast<std::chrono::milliseconds>(s_ids - s_bitmap).count();

		vector<row_t> *row_ids = new vector<row_t>;
		size_t cursor = 0;

		row_ids->resize(btv_res.count() + 64);

		auto element_ptr = &(*row_ids)[0];

		uint32_t ids_count = 0;
		uint64_t ids_idx = 0;
		// traverse m_vec
		auto it = btv_res.m_vec.begin();
		while(it != btv_res.m_vec.end()) {
			util_btv_to_id_list(element_ptr, ids_count, ids_idx, reverseBits(*it));
			ids_idx += 31;
			it++;
		}

		// active word
		util_btv_to_id_list(element_ptr, ids_count, ids_idx, \
								reverseBits(btv_res.active.val << (31 - btv_res.active.nbits)));

		row_ids->resize(btv_res.count());

		auto e_ids = std::chrono::high_resolution_clock::now();
		time_ids = std::chrono::duration_cast<std::chrono::milliseconds>(e_ids - s_ids).count();
        
        num_idlist = row_ids->size();
		while(true) {
			auto s_fetch = std::chrono::high_resolution_clock::now();
			DataChunk result;
			result.Initialize(context.client, types);

			if(cursor < row_ids->size()) {
				ColumnFetchState column_fetch_state;
				data_ptr_t row_ids_data = nullptr;
				row_ids_data = (data_ptr_t)&((*row_ids)[cursor]);
				Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
				idx_t fetch_count = 2048;
				if(cursor + fetch_count > row_ids->size()) {
					fetch_count = row_ids->size() - cursor;
				}
				lineitem_table.GetStorage().BMFetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
														column_fetch_state, num_idlist);

				cursor += fetch_count;
			}
			else {
				delete row_ids;
				break;
			}
			auto e_fetch = std::chrono::high_resolution_clock::now();
			time_fetch += std::chrono::duration_cast<std::chrono::nanoseconds>(e_fetch - s_fetch).count();

			auto order_key_data = FlatVector::GetData<int64_t>(result.data[0]);
			auto commitdate_data = FlatVector::GetData<int32_t>(result.data[1]);
			auto receiptdate_data = FlatVector::GetData<int32_t>(result.data[2]);

			auto s_c = std::chrono::high_resolution_clock::now();

			for(int i = 0; i < result.size(); i++) {
				if(commitdate_data[i] < receiptdate_data[i]) {
					orderkey_set.insert(order_key_data[i]);
				}
			}
		}
	}
	
	auto s2 = std::chrono::high_resolution_clock::now();

	std::map<int32_t, int32_t> q4_ans;
	{
		for(auto orderkey : orderkey_set) {
			q4_ans[orderkey_map[orderkey]]++;
		}
	}
	
	auto s3 = std::chrono::high_resolution_clock::now();

	for(auto &it : q4_ans) {
		std::cout << priority_map[it.first] << " : " << it.second << std::endl;
	} 

	std::cout << "fetch time : " << time_fetch / 1000000 << "ms" << std::endl;
	std::cout << "ids time : " << time_ids / 1000000 << "ms" << std::endl;
	std::cout << "bitmap time : " << time_bitmap << "ms" << std::endl;
	std::cout << "q4 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s3 - s0).count() << "ms" << std::endl;
}

}