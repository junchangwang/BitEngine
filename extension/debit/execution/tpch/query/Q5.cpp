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


// void BMTableScan::BMTPCH_Q5(ExecutionContext &context, const PhysicalTableScan &op)
// {
// 	auto &nation_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "nation");
// 	auto &customer_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "customer");
// 	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
// 	auto &supplier_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "supplier");
// 	auto &region_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "region");
// 	auto &orders_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "orders");

// 	double half_time = 0;
// 	double whole_time = 0;

// 	auto s1 = std::chrono::high_resolution_clock::now();
	
// 	std::unordered_set<int32_t> r_regionkey_set;
// 	{
// 		auto &region_transaction = DuckTransaction::Get(context.client, region_table.catalog);
// 		TableScanState region_scan_state;
// 		TableScanGlobalSourceState gs(context.client, op);
// 		vector<StorageIndex> storage_column_ids;
// 		storage_column_ids.push_back(StorageIndex(0)); // r_regionkey
// 		storage_column_ids.push_back(StorageIndex(1)); // r_name
//         region_table.GetStorage().InitializeScan(context.client, region_transaction, region_scan_state, storage_column_ids);
// 		vector<LogicalType> types;
// 		types.push_back(LogicalType(LogicalTypeId::INTEGER));
// 		types.push_back(LogicalType(LogicalTypeId::VARCHAR));
// 		while(true) {
// 			// TODO: read name actually
// 			DataChunk result;
// 			result.Initialize(context.client, types);
// 			region_table.GetStorage().Scan(region_transaction, result, region_scan_state);
// 			if(result.size() == 0)
// 				break;
// 			r_regionkey_set.insert(2);
// 		}
// 	}
	
// 	std::unordered_set<int32_t> n_nationkey_set;
// 	{
// 		auto &nation_transaction = DuckTransaction::Get(context.client, nation_table.catalog);
// 		TableScanState nation_scan_state;
// 		TableScanGlobalSourceState gs(context.client, op);
// 		vector<StorageIndex> storage_column_ids;
// 		storage_column_ids.push_back(StorageIndex(0)); // n_nationkey
// 		storage_column_ids.push_back(StorageIndex(2)); // n_regionkey
// 		nation_table.GetStorage().InitializeScan(context.client, nation_transaction, nation_scan_state, storage_column_ids);
// 		vector<LogicalType> types;
// 		types.push_back(LogicalType(LogicalTypeId::INTEGER));
// 		types.push_back(LogicalType(LogicalTypeId::INTEGER));
// 		while(true) {
// 			DataChunk result;
// 			result.Initialize(context.client, types);
// 			nation_table.GetStorage().Scan(nation_transaction, result, nation_scan_state);
// 			if(result.size() == 0)
// 				break;

// 			auto &n_nationkey = result.data[0];
// 			auto &n_regionkey = result.data[1];
// 			auto n_nationkey_data = FlatVector::GetData<int32_t>(n_nationkey);
// 			auto n_regionkey_data = FlatVector::GetData<int32_t>(n_regionkey);

// 			for(int i = 0; i < result.size(); i++) {
// 				if(r_regionkey_set.count(n_regionkey_data[i]))
// 					n_nationkey_set.insert(n_nationkey_data[i]);
// 			}
// 		}
// 	}

// 	std::unordered_map<int64_t, int32_t> customer_nation_map;
// 	{
// 		auto &customer_transaction = DuckTransaction::Get(context.client, customer_table.catalog);
// 		TableScanState customer_scan_state;
// 		TableScanGlobalSourceState gs(context.client, op);
// 		vector<StorageIndex> storage_column_ids;
// 		storage_column_ids.push_back(StorageIndex(0)); // c_custkey
// 		storage_column_ids.push_back(StorageIndex(3)); // c_nationkey
// 		customer_table.GetStorage().InitializeScan(context.client, customer_transaction, customer_scan_state, storage_column_ids);
// 		vector<LogicalType> types;
// 		types.push_back(customer_table.GetColumns().GetColumnTypes()[0]);
// 		types.push_back(customer_table.GetColumns().GetColumnTypes()[3]);
// 		while(true) {
// 			DataChunk result;
// 			result.Initialize(context.client, types);
// 			customer_table.GetStorage().Scan(customer_transaction, result, customer_scan_state);
// 			if(result.size() == 0)
// 				break;

// 			auto &c_custkey = result.data[0];
// 			auto &c_nationkey = result.data[1];
// 			auto c_custkey_data = FlatVector::GetData<int64_t>(c_custkey);
// 			auto c_nationkey_data = FlatVector::GetData<int32_t>(c_nationkey);

// 			for(int i = 0; i < result.size(); i++) {
// 				if(n_nationkey_set.count(c_nationkey_data[i]))
// 					customer_nation_map[c_custkey_data[i]] = c_nationkey_data[i];
// 			}
// 		}
// 	}

// 	std::unordered_map<int64_t, int32_t> order_nation_map;
// 	{
// 		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
// 		TableScanState orders_scan_state;
// 		TableScanGlobalSourceState gs(context.client, op);
// 		vector<StorageIndex> storage_column_ids;
// 		storage_column_ids.push_back(StorageIndex(0));
// 		storage_column_ids.push_back(StorageIndex(1));
// 		storage_column_ids.push_back(StorageIndex(4));
// 		orders_table.GetStorage().InitializeScan(context.client, orders_transaction, orders_scan_state, storage_column_ids);
// 		vector<LogicalType> types;
// 		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);
// 		types.push_back(orders_table.GetColumns().GetColumnTypes()[1]);
// 		types.push_back(orders_table.GetColumns().GetColumnTypes()[4]);
// 		while(true) {
// 			DataChunk result;
// 			result.Initialize(context.client, types);
// 			orders_table.GetStorage().Scan(orders_transaction, result, orders_scan_state);
// 			if(result.size() == 0)
// 				break;

// 			auto &o_orderkey = result.data[0];
// 			auto &o_custkey = result.data[1];
// 			auto &o_orderdate = result.data[2];
// 			auto o_orderkey_data = FlatVector::GetData<int64_t>(o_orderkey);
// 			auto o_custkey_data = FlatVector::GetData<int64_t>(o_custkey);
// 			auto o_orderdate_data = FlatVector::GetData<int32_t>(o_orderdate);

// 			for(int i = 0; i < result.size(); i++) {
// 				if(o_orderdate_data[i] < 8766 || o_orderdate_data[i] >= 9131)
// 					continue;
// 				if(customer_nation_map.count(o_custkey_data[i]))
// 					order_nation_map[o_orderkey_data[i]] = customer_nation_map[o_custkey_data[i]];
// 			}
// 		}
// 	}

// 	std::unordered_map<int64_t, int32_t> supp_nation_map;
// 	{
// 		auto &supplier_transaction = DuckTransaction::Get(context.client, supplier_table.catalog);
// 		TableScanState supplier_scan_state;
// 		TableScanGlobalSourceState gs(context.client, op);
// 		vector<StorageIndex> storage_column_ids;
// 		storage_column_ids.push_back(StorageIndex(0));
// 		storage_column_ids.push_back(StorageIndex(3));
// 		supplier_table.GetStorage().InitializeScan(context.client, supplier_transaction, supplier_scan_state, storage_column_ids);
// 		vector<LogicalType> types;
// 		types.push_back(supplier_table.GetColumns().GetColumnTypes()[0]);
// 		types.push_back(supplier_table.GetColumns().GetColumnTypes()[3]);
// 		while(true) {
// 			DataChunk result;
// 			result.Initialize(context.client, types);
// 			supplier_table.GetStorage().Scan(supplier_transaction, result, supplier_scan_state);
// 			if(result.size() == 0)
// 				break;

// 			auto &supp_key = result.data[0];
// 			auto &nation_key = result.data[1];
// 			auto supp_key_data = FlatVector::GetData<int64_t>(supp_key);
// 			auto nation_key_data = FlatVector::GetData<int32_t>(nation_key);

// 			for(int i = 0; i < result.size(); i++) {
// 				if(n_nationkey_set.count(nation_key_data[i])) {
// 					supp_nation_map[supp_key_data[i]] = nation_key_data[i];
// 				}
// 			}
// 		}
// 	}

// 	auto s2 = std::chrono::high_resolution_clock::now();
	
// 	std::unordered_map<int32_t, int64_t> q5_ans;
	
// 	{	
// 		long long time_bitmap_or = 0;
// 		long long time_compute = 0;
// 		long long time_fetch = 0;
// 		long long time_ids = 0;
// 		auto rabit_orderkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderkey);
// 		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
// 		TableScanState lineitem_scan_state;
// 		TableScanGlobalSourceState gs(context.client, op);
// 		vector<StorageIndex> storage_column_ids;
// 		storage_column_ids.push_back(StorageIndex(0));
// 		storage_column_ids.push_back(StorageIndex(2));
// 		storage_column_ids.push_back(StorageIndex(5));
// 		storage_column_ids.push_back(StorageIndex(6));
// 		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
// 		vector<LogicalType> types;
// 		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
// 		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[2]);
// 		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
// 		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);

// 		vector<row_t> *ids = new vector<row_t>;
// 		size_t cursor = 0;

// 		auto st_or = std::chrono::high_resolution_clock::now();

// 		ibis::bitvector btv_res;

// 		auto it = order_nation_map.begin();

// 		btv_res.copy(*rabit_orderkey->Btvs[it->first]->btv);
// 		btv_res.decompress();
// 		it++;

// 		while(it != order_nation_map.end()) {
// 			btv_res |= *rabit_orderkey->Btvs[it->first]->btv;
// 			it++;
// 		}

// 		auto et_or = std::chrono::high_resolution_clock::now();
// 		time_bitmap_or += std::chrono::duration_cast<std::chrono::nanoseconds>(et_or - st_or).count();

// 		auto st_ids = std::chrono::high_resolution_clock::now();

// 		GetRowids(btv_res, ids);

// 		auto et_ids = std::chrono::high_resolution_clock::now();
// 		time_ids += std::chrono::duration_cast<std::chrono::nanoseconds>(et_ids - st_ids).count();

// 		num_idlist = ids->size();
// 		while(true) {
// 			auto st_fetch = std::chrono::high_resolution_clock::now();
			
// 			DataChunk result;
// 			result.Initialize(context.client, types);

// 			if(cursor < ids->size()) {
// 				ColumnFetchState column_fetch_state;
// 				data_ptr_t row_ids_data = nullptr;
// 				row_ids_data = (data_ptr_t)&((*ids)[cursor]);
// 				Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
// 				idx_t fetch_count = 2048;
// 				if(cursor + fetch_count > ids->size()) {
// 					fetch_count = ids->size() - cursor;
// 				}
// 				lineitem_table.GetStorage().BMFetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
// 														column_fetch_state, num_idlist);

// 				cursor += fetch_count;
// 			}
// 			else {
// 				delete ids;
// 				break;
// 			}
// 			auto et_fetch = std::chrono::high_resolution_clock::now();
// 			time_fetch += std::chrono::duration_cast<std::chrono::nanoseconds>(et_fetch - st_fetch).count();

// 			auto &order_key = result.data[0];
// 			auto &supp_key = result.data[1];
// 			auto &extenedprice = result.data[2];
// 			auto &discount = result.data[3];
// 			auto order_key_data = FlatVector::GetData<int64_t>(order_key);
// 			auto supp_key_data = FlatVector::GetData<int64_t>(supp_key);
// 			auto extenedprice_data = FlatVector::GetData<int64_t>(extenedprice);
// 			auto discount_data = FlatVector::GetData<int64_t>(discount);

// 			auto st_compute = std::chrono::high_resolution_clock::now();
// 			for(int i = 0; i < result.size(); i++) {
// 				if(order_nation_map[order_key_data[i]] == supp_nation_map[supp_key_data[i]])
// 					q5_ans[supp_nation_map[supp_key_data[i]]] += extenedprice_data[i] * (100 - discount_data[i]);
// 			}
// 			auto et_compute = std::chrono::high_resolution_clock::now();
// 			time_compute += std::chrono::duration_cast<std::chrono::nanoseconds>(et_compute - st_compute).count();
// 		}

// 		std::cout << "bitmap or time : "<< time_bitmap_or/1000000 << "ms" << std::endl;
// 		std::cout << "ids time : "<< time_ids/1000000 << "ms" << std::endl;
// 		std::cout << "fetch time : "<< time_fetch/1000000 << "ms" << std::endl;
// 		std::cout << "compute time : "<< time_compute/1000000 << "ms" << std::endl;
// 	}

// 	auto s3 = std::chrono::high_resolution_clock::now();
// 	half_time = std::chrono::duration_cast<std::chrono::milliseconds>(s2 - s1).count();
// 	whole_time = half_time + std::chrono::duration_cast<std::chrono::milliseconds>(s3 - s2).count();
// 	std::cout << "half time : "<< half_time << "ms" << std::endl;
// 	std::cout << "q5 time : "<< whole_time << "ms" << std::endl;

// 	std::vector<std::pair<uint64_t, uint32_t> > output;
// 	for(auto &it : q5_ans) {
// 		output.push_back({it.second, it.first});
// 	}
// 	std::sort(output.begin(), output.end(), [](std::pair<uint64_t, uint32_t> &a, std::pair<uint64_t, uint32_t> &b) {return a.first > b.first;});
// 	for(auto &p : output) {
// 		std::cout << p.second << " : " << std::fixed << std::setprecision(4) << ((double)p.first) / 10000 << std::endl;
// 	}

// 	return;
// }

void BMTableScan::BMTPCH_Q5(ExecutionContext &context, const PhysicalTableScan &op)
{
	auto &nation_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "nation");
	auto &customer_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "customer");
	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	auto &supplier_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "supplier");
	auto &region_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "region");
	auto &orders_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "orders");

	double half_time = 0;
	double whole_time = 0;

	auto s1 = std::chrono::high_resolution_clock::now();
	
	std::unordered_set<int32_t> r_regionkey_set; // r_regionkey = 2 (r_name = 'ASIA')
	{
		auto &region_transaction = DuckTransaction::Get(context.client, region_table.catalog);
		TableScanState region_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0)); // r_regionkey
		storage_column_ids.push_back(StorageIndex(1)); // r_name
        region_table.GetStorage().InitializeScan(context.client, region_transaction, region_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(LogicalType(LogicalTypeId::INTEGER));
		types.push_back(LogicalType(LogicalTypeId::VARCHAR));
		while(true) {
			// TODO: read name actually
			DataChunk result;
			result.Initialize(context.client, types);
			region_table.GetStorage().Scan(region_transaction, result, region_scan_state);
			if(result.size() == 0)
				break;
			r_regionkey_set.insert(2);
		}
	}
	
	std::unordered_set<int32_t> n_nationkey_set;
	{
		auto &nation_transaction = DuckTransaction::Get(context.client, nation_table.catalog);
		TableScanState nation_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0)); // n_nationkey
		storage_column_ids.push_back(StorageIndex(2)); // n_regionkey
		nation_table.GetStorage().InitializeScan(context.client, nation_transaction, nation_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(LogicalType(LogicalTypeId::INTEGER));
		types.push_back(LogicalType(LogicalTypeId::INTEGER));
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			nation_table.GetStorage().Scan(nation_transaction, result, nation_scan_state);
			if(result.size() == 0)
				break;

			auto &n_nationkey = result.data[0];
			auto &n_regionkey = result.data[1];
			auto n_nationkey_data = FlatVector::GetData<int32_t>(n_nationkey);
			auto n_regionkey_data = FlatVector::GetData<int32_t>(n_regionkey);

			for(int i = 0; i < result.size(); i++) {
				if(r_regionkey_set.count(n_regionkey_data[i]))
					n_nationkey_set.insert(n_nationkey_data[i]);
			}
		}
	}

	std::unordered_map<int64_t, int32_t> customer_nation_map;
	{
		auto &customer_transaction = DuckTransaction::Get(context.client, customer_table.catalog);
		TableScanState customer_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0)); // c_custkey
		storage_column_ids.push_back(StorageIndex(3)); // c_nationkey
		customer_table.GetStorage().InitializeScan(context.client, customer_transaction, customer_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(customer_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(customer_table.GetColumns().GetColumnTypes()[3]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			customer_table.GetStorage().Scan(customer_transaction, result, customer_scan_state);
			if(result.size() == 0)
				break;

			auto &c_custkey = result.data[0];
			auto &c_nationkey = result.data[1];
			auto c_custkey_data = FlatVector::GetData<int64_t>(c_custkey);
			auto c_nationkey_data = FlatVector::GetData<int32_t>(c_nationkey);

			for(int i = 0; i < result.size(); i++) {
				if(n_nationkey_set.count(c_nationkey_data[i]))
					customer_nation_map[c_custkey_data[i]] = c_nationkey_data[i];
			}
		}
	}

	std::unordered_map<int64_t, int32_t> order_nation_map;
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

			auto &o_orderkey = result.data[0];
			auto &o_custkey = result.data[1];
			auto &o_orderdate = result.data[2];
			auto o_orderkey_data = FlatVector::GetData<int64_t>(o_orderkey);
			auto o_custkey_data = FlatVector::GetData<int64_t>(o_custkey);
			auto o_orderdate_data = FlatVector::GetData<int32_t>(o_orderdate);

			for(int i = 0; i < result.size(); i++) {
				if(o_orderdate_data[i] < 8766 || o_orderdate_data[i] >= 9131)
					continue;
				if(customer_nation_map.count(o_custkey_data[i]))
					order_nation_map[o_orderkey_data[i]] = customer_nation_map[o_custkey_data[i]];
			}
		}
	}

	std::unordered_map<int64_t, int32_t> supp_nation_map;
	{	
		auto &supplier_transaction = DuckTransaction::Get(context.client, supplier_table.catalog);
		TableScanState supplier_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(3));
		supplier_table.GetStorage().InitializeScan(context.client, supplier_transaction, supplier_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(supplier_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(supplier_table.GetColumns().GetColumnTypes()[3]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			supplier_table.GetStorage().Scan(supplier_transaction, result, supplier_scan_state);
			if(result.size() == 0)
				break;

			auto &supp_key = result.data[0];
			auto &nation_key = result.data[1];
			auto supp_key_data = FlatVector::GetData<int64_t>(supp_key);
			auto nation_key_data = FlatVector::GetData<int32_t>(nation_key);

			for(int i = 0; i < result.size(); i++) {
				if(n_nationkey_set.count(nation_key_data[i])) {
					supp_nation_map[supp_key_data[i]] = nation_key_data[i];
				}
			}
		}
	}

	auto s2 = std::chrono::high_resolution_clock::now();
	
	std::unordered_map<int32_t, int64_t> q5_ans;
	
	{	
		long long time_bitmap_or = 0;
		long long time_compute = 0;
		long long time_fetch = 0;
		long long time_ids = 0;
		auto rabit_orderkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderkey);
		auto rabit_suppkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_suppkey);
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(2));
		storage_column_ids.push_back(StorageIndex(5));
		storage_column_ids.push_back(StorageIndex(6));
		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[2]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);

		vector<row_t> *ids = new vector<row_t>;
		size_t cursor = 0;

		auto st_or = std::chrono::high_resolution_clock::now();

		ibis::bitvector btv_res,btv_suppkey;

		auto it = order_nation_map.begin();

		btv_res.copy(*rabit_orderkey->Btvs[it->first]->btv);
		btv_res.decompress();
		it++;

		while(it != order_nation_map.end()) {
			btv_res |= *rabit_orderkey->Btvs[it->first]->btv;
			it++;
		}

		it = supp_nation_map.begin();

		btv_suppkey.copy(*rabit_suppkey->Btvs[it->first]->btv);
		btv_suppkey.decompress();
		it++;

		while(it != supp_nation_map.end()) {
			btv_suppkey |= *rabit_suppkey->Btvs[it->first]->btv;
			it++;
		}

		btv_res &= btv_suppkey;

		auto et_or = std::chrono::high_resolution_clock::now();
		time_bitmap_or += std::chrono::duration_cast<std::chrono::nanoseconds>(et_or - st_or).count();

		auto st_ids = std::chrono::high_resolution_clock::now();

		GetRowids(btv_res, ids);

		auto et_ids = std::chrono::high_resolution_clock::now();
		time_ids += std::chrono::duration_cast<std::chrono::nanoseconds>(et_ids - st_ids).count();

		num_idlist = ids->size();
		while(true) {
			auto st_fetch = std::chrono::high_resolution_clock::now();
			
			DataChunk result;
			result.Initialize(context.client, types);

			if(cursor < ids->size()) {
				ColumnFetchState column_fetch_state;
				data_ptr_t row_ids_data = nullptr;
				row_ids_data = (data_ptr_t)&((*ids)[cursor]);
				Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
				idx_t fetch_count = 2048;
				if(cursor + fetch_count > ids->size()) {
					fetch_count = ids->size() - cursor;
				}
				lineitem_table.GetStorage().BMFetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
														column_fetch_state, num_idlist);

				cursor += fetch_count;
			}
			else {
				delete ids;
				break;
			}
			auto et_fetch = std::chrono::high_resolution_clock::now();
			time_fetch += std::chrono::duration_cast<std::chrono::nanoseconds>(et_fetch - st_fetch).count();

			auto &order_key = result.data[0];
			auto &supp_key = result.data[1];
			auto &extenedprice = result.data[2];
			auto &discount = result.data[3];
			auto order_key_data = FlatVector::GetData<int64_t>(order_key);
			auto supp_key_data = FlatVector::GetData<int64_t>(supp_key);
			auto extenedprice_data = FlatVector::GetData<int64_t>(extenedprice);
			auto discount_data = FlatVector::GetData<int64_t>(discount);

			auto st_compute = std::chrono::high_resolution_clock::now();
			for(int i = 0; i < result.size(); i++) {
				if(order_nation_map[order_key_data[i]] == supp_nation_map[supp_key_data[i]]) {
					q5_ans[supp_nation_map[supp_key_data[i]]] += extenedprice_data[i] * (100 - discount_data[i]);
				}
			}
			auto et_compute = std::chrono::high_resolution_clock::now();
			time_compute += std::chrono::duration_cast<std::chrono::nanoseconds>(et_compute - st_compute).count();
		}

		std::cout << "bitmap or time : "<< time_bitmap_or/1000000 << "ms" << std::endl;
		std::cout << "ids time : "<< time_ids/1000000 << "ms" << std::endl;
		std::cout << "fetch time : "<< time_fetch/1000000 << "ms" << std::endl;
		std::cout << "compute time : "<< time_compute/1000000 << "ms" << std::endl;
	}

	auto s3 = std::chrono::high_resolution_clock::now();
	half_time = std::chrono::duration_cast<std::chrono::milliseconds>(s2 - s1).count();
	whole_time = half_time + std::chrono::duration_cast<std::chrono::milliseconds>(s3 - s2).count();
	std::cout << "half time : "<< half_time << "ms" << std::endl;
	std::cout << "q5 time : "<< whole_time << "ms" << std::endl;

	std::vector<std::pair<uint64_t, uint32_t> > output;
	for(auto &it : q5_ans) {
		output.push_back({it.second, it.first});
	}
	std::sort(output.begin(), output.end(), [](std::pair<uint64_t, uint32_t> &a, std::pair<uint64_t, uint32_t> &b) {return a.first > b.first;});
	for(auto &p : output) {
		std::cout << p.second << " : " << std::fixed << std::setprecision(4) << ((double)p.first) / 10000 << std::endl;
	}

	return;
}
}