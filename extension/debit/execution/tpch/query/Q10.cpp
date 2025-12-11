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


namespace duckdb
{

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

struct pair_hash {
	template <class T1, class T2>
	std::size_t operator() (const std::pair<T1, T2>& p) const {
		auto h1 = std::hash<T1>{}(p.first);
		auto h2 = std::hash<T2>{}(p.second);
		return h1 ^ (h2 << 1);
	}
};

struct q10_topk {
	bool operator ()(std::pair<int64_t, int64_t> &a, std::pair<int64_t, int64_t> &b) {
		return a.second > b.second;
	}
};

void BMTableScan::BMTPCH_Q10(ExecutionContext &context, const PhysicalTableScan &op)
{

	auto s0 = std::chrono::high_resolution_clock::now();

	auto &nation_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "nation");
	auto &customer_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "customer");
	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	auto &orders_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "orders");

	const char returnflag = 'R';

	std::unordered_map<int32_t, std::string> nation_map;
	{
		auto &nation_transaction = DuckTransaction::Get(context.client, nation_table.catalog);
		TableScanState nation_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(1));
		nation_table.GetStorage().InitializeScan(context.client, nation_transaction, nation_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(nation_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(nation_table.GetColumns().GetColumnTypes()[1]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			nation_table.GetStorage().Scan(nation_transaction, result, nation_scan_state);
			if(result.size() == 0)
				break;

			auto &nation_key = result.data[0];
			auto nation_key_data = FlatVector::GetData<int32_t>(nation_key);

			for(int i = 0; i < result.size(); i++) {
				nation_map[nation_key_data[i]] = "tmp";
			}
		}
	}

	std::unordered_map<int64_t, int32_t> customer_nation_map;
	// customer_nation_map.reserve(1500000);
	{
		auto &customer_transaction = DuckTransaction::Get(context.client, customer_table.catalog);
		TableScanState customer_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(3));
		// storage_column_ids.push_back(StorageIndex(1));
		// storage_column_ids.push_back(StorageIndex(2));
		// storage_column_ids.push_back(StorageIndex(4));
		// storage_column_ids.push_back(StorageIndex(5));
		// storage_column_ids.push_back(StorageIndex(7));
		customer_table.GetStorage().InitializeScan(context.client, customer_transaction, customer_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(customer_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(customer_table.GetColumns().GetColumnTypes()[3]);
		// types.push_back(customer_table.GetColumns().GetColumnTypes()[1]);
		// types.push_back(customer_table.GetColumns().GetColumnTypes()[2]);
		// types.push_back(customer_table.GetColumns().GetColumnTypes()[4]);
		// types.push_back(customer_table.GetColumns().GetColumnTypes()[5]);
		// types.push_back(customer_table.GetColumns().GetColumnTypes()[7]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			customer_table.GetStorage().Scan(customer_transaction, result, customer_scan_state);
			if(result.size() == 0)
				break;

			auto &customer_key = result.data[0];
			auto &nation_key = result.data[1];
			auto customer_key_data = FlatVector::GetData<int64_t>(customer_key);
			auto nation_key_data = FlatVector::GetData<int32_t>(nation_key);

			for(int i = 0; i < result.size(); i++) {
				if(nation_map.count(nation_key_data[i]))
					customer_nation_map[customer_key_data[i]] = nation_key_data[i];
			}
		}
	}

	auto rabit_orderkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderkey);
	ibis::bitvector btv_res;
	btv_res.adjustSize(0, rabit_orderkey->config->n_rows);
	btv_res.decompress();
	std::unordered_map<int64_t, std::pair<int64_t, int32_t>> order_map;
	{
		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
		TableScanState orders_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(1));
		storage_column_ids.push_back(StorageIndex(4));
		orders_table.GetStorage().InitializeScan(context.client, orders_transaction, orders_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[1]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[4]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			orders_table.GetStorage().Scan(orders_transaction, result, orders_scan_state);
			if(result.size() == 0)
				break;

			auto &orders_key = result.data[0];
			auto &customer_key = result.data[1];
			auto &days = result.data[2];
			auto orders_key_data = FlatVector::GetData<int64_t>(orders_key);
			auto customer_key_data = FlatVector::GetData<int64_t>(customer_key);
			auto days_data = FlatVector::GetData<int32_t>(days);

			for(int i = 0; i < result.size(); i++) {
				if(days_data[i] < 8766 && days_data[i] >= 8674) {
					if(customer_nation_map.count(customer_key_data[i])) {
						btv_res |= *rabit_orderkey->Btvs[orders_key_data[i]]->btv;
						order_map[orders_key_data[i]] = {customer_key_data[i], customer_nation_map[customer_key_data[i]]};
					}
				}
			}
		}
	}

	std::unordered_map<std::pair<int64_t, int32_t>, int64_t, pair_hash> group_map;
	{
		long long time1 = 0;
		auto rabit_returnflag = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(5));
		storage_column_ids.push_back(StorageIndex(6));
		// storage_column_ids.push_back(uint64_t(8));
		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
		// types.push_back(lineitem_table.GetColumns().GetColumnTypes()[8]);

		// 1 is 'R'
		btv_res &= *rabit_returnflag->Btvs[1]->btv;


		vector<row_t> *row_ids = new vector<row_t>;
		size_t cursor = 0;

		row_ids->resize(btv_res.count() + 64);
		// ids[btv_res.count()] = 999999999;
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

		while(true) {
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
				lineitem_table.GetStorage().Fetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
														column_fetch_state);

				cursor += fetch_count;
			}
			else {
				delete row_ids;
				break;
			}

			auto &order_key = result.data[0];
			auto &extenedprice = result.data[1];
			auto &discount = result.data[2];
			auto order_key_data = FlatVector::GetData<int64_t>(order_key);
			auto extenedprice_data = FlatVector::GetData<int64_t>(extenedprice);
			auto discount_data = FlatVector::GetData<int64_t>(discount);

			for(int i = 0; i < result.size(); i++) {
					group_map[order_map[order_key_data[i]]] += extenedprice_data[i] * (100 - discount_data[i]);
			}
		}

	}

	std::priority_queue<std::pair<int64_t, int64_t>,vector<std::pair<int64_t, int64_t>>,q10_topk> minHeap;
	int heapc = 20;

	for(auto &it : group_map) {
		if(minHeap.size() < heapc)
			minHeap.push({it.first.first, it.second});
		else {
			if(it.second <= minHeap.top().second)
				continue;
			minHeap.pop();
			minHeap.push({it.first.first, it.second});
		}
	}

    std::vector<std::pair<int64_t, int64_t>> results;
	while (!minHeap.empty()) {
		results.push_back(minHeap.top());
		minHeap.pop();
	}

    std::cout<<"c_custkey    revenue"<<std::endl;
	for (auto it = results.rbegin(); it != results.rend(); ++it) {
		std::cout << it->first 
                  <<"  :  " << std::fixed << std::setprecision(4)<< ((double)it->second) / 10000 << std::endl;
	}

	auto e0 = std::chrono::high_resolution_clock::now();

	std::cout << "q10 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(e0 - s0).count() << "ms" << std::endl;

}

}