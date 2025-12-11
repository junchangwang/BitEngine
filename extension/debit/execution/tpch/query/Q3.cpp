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

struct q3_topk {
	bool operator ()(std::pair<int64_t, int64_t> &a, std::pair<int64_t, int64_t> &b) {
		return a.second > b.second;
	}
};

void BMTableScan::BMTPCH_Q3(ExecutionContext &context, const PhysicalTableScan &op)
{
	auto &customer_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "customer");
	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	auto &orders_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "orders");
	
	long long time_fetch = 0;
	long long time_ids = 0;
	long long time_join = 0;

	char *msg = "BUILDING";
	int32_t filter_days = 9204; // 1995-03-15

	auto s0 = std::chrono::high_resolution_clock::now();

	std::bitset<1500001> o_custkey_b;
	{
		auto &customer_transaction = DuckTransaction::Get(context.client, customer_table.catalog);
		TableScanState customer_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(6));
		customer_table.GetStorage().InitializeScan(context.client, customer_transaction, customer_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(customer_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(customer_table.GetColumns().GetColumnTypes()[6]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			customer_table.GetStorage().Scan(customer_transaction, result, customer_scan_state);
			if(result.size() == 0)
				break;

			auto &c_custkey = result.data[0];
			auto &mktsegment = result.data[1];
			auto c_custkey_data = FlatVector::GetData<int64_t>(c_custkey);

			if(result.data[1].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &mktsegment_sel_vector = DictionaryVector::SelVector(result.data[1]);
				auto &mktsegment_child = DictionaryVector::Child(result.data[1]);

				for(int i = 0; i < result.size(); i++) {
					if(!strcmp(reinterpret_cast<string_t *>(mktsegment_child.GetData())[mktsegment_sel_vector.get_index(i)].GetData(), msg))
						o_custkey_b[c_custkey_data[i]] = 1;
				}
			}
			else {
				for(int i = 0; i < result.size(); i++) {
					if(!strcmp(reinterpret_cast<string_t *>(mktsegment.GetData())[i].GetData(), msg))
						o_custkey_b[c_custkey_data[i]] = 1;
				}
			}
		}
	}

	

	std::unordered_map<int64_t, std::pair<int64_t, std::pair<int32_t, int32_t>>> l_orderkey_map;

	auto rabit_l_orderkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderkey);
	ibis::bitvector btv_res;
	btv_res.adjustSize(0, rabit_l_orderkey->config->n_rows);
	btv_res.decompress();
	{
		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
		TableScanState orders_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(1));
		storage_column_ids.push_back(StorageIndex(4));
		storage_column_ids.push_back(StorageIndex(7));
		orders_table.GetStorage().InitializeScan(context.client, orders_transaction, orders_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[1]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[4]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[7]);

		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			orders_table.GetStorage().Scan(orders_transaction, result, orders_scan_state);
			if(result.size() == 0)
				break;

			auto &o_orderkey = result.data[0];
			auto &o_custkey = result.data[1];
			auto &o_orderdate = result.data[2];
			auto &o_shippriority = result.data[3];
			auto o_orderkey_data = FlatVector::GetData<int64_t>(o_orderkey);
			auto o_custkey_data = FlatVector::GetData<int64_t>(o_custkey);
			auto o_orderdate_data = FlatVector::GetData<int32_t>(o_orderdate);
			auto o_shippriority_data = FlatVector::GetData<int32_t>(o_shippriority);

			auto st_join = std::chrono::high_resolution_clock::now();

			for(int i = 0; i < result.size(); i++) {
				if(o_orderdate_data[i] < filter_days && o_custkey_b[o_custkey_data[i]]) {
					if (rabit_l_orderkey->Btvs[o_orderkey_data[i]]->btv->cnt() > 0) {
						l_orderkey_map.emplace(o_orderkey_data[i],std::make_pair(0LL,
											   std::make_pair(o_orderdate_data[i], o_shippriority_data[0])));
					}
					btv_res |= *rabit_l_orderkey->Btvs[o_orderkey_data[i]]->btv;
				}
			}

			auto et_join = std::chrono::high_resolution_clock::now();
			time_join += std::chrono::duration_cast<std::chrono::nanoseconds>(et_join - st_join).count();
		}
	}
	auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);
	ibis::bitvector btv_shipdate;
	btv_shipdate.copy(*rabit_shipdate->Btvs[filter_days + 1]->btv);
	btv_shipdate.decompress();

	for(uint32_t i = filter_days + 2; i < rabit_shipdate->config->g_cardinality; i++) {
		btv_shipdate |= *rabit_shipdate->Btvs[i]->btv;
	}
	btv_res &= btv_shipdate;

	{
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(5));
		storage_column_ids.push_back(StorageIndex(6));
		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);

		vector<row_t> *ids = new vector<row_t>;
		size_t cursor = 0;

		auto st_ids = std::chrono::high_resolution_clock::now();

		for (ibis::bitvector::indexSet index_set = btv_res.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
			const ibis::bitvector::word_t *indices = index_set.indices();
			if (index_set.isRange()) {
				for (ibis::bitvector::word_t j = *indices; j < indices[1]; ++j) {
					ids->push_back((uint64_t)j);
				}
			} else {
				for (unsigned j = 0; j < index_set.nIndices(); ++j) {
					ids->push_back((uint64_t)indices[j]);
				}
			}
		}

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

			auto &l_order_key = result.data[0];
			auto &l_extendedprice = result.data[1];
			auto &l_discount = result.data[2];
			auto l_order_key_data = FlatVector::GetData<int64_t>(l_order_key);
			auto l_extendedprice_data = FlatVector::GetData<int64_t>(l_extendedprice);
			auto l_discount_data = FlatVector::GetData<int64_t>(l_discount);

			for(int i = 0; i < result.size(); i++) {
				auto it = l_orderkey_map.find(l_order_key_data[i]);
				it->second.first += l_extendedprice_data[i] * (100 - l_discount_data[i]);
			}
		}
	}

    auto s1 = std::chrono::high_resolution_clock::now();

	std::cout << "ids time : " << time_ids / 1000000 << "ms" << std::endl;
	std::cout << "fetch time : " << time_fetch / 1000000 << "ms" << std::endl;
	std::cout << "join time : " << time_join / 1000000 << "ms" << std::endl;
	std::cout << "q3 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;

	std::priority_queue<std::pair<int64_t, int64_t>, vector<std::pair<int64_t, int64_t>>, q3_topk> minHeap;
	int heapc = 10;

	for (auto &it : l_orderkey_map) {
		if (minHeap.size() < heapc)
			minHeap.push(std::make_pair(it.first, it.second.first));
		else {
			if (it.second.first <= minHeap.top().second)
				continue;
			minHeap.pop();
			minHeap.push(std::make_pair(it.first, it.second.first));
		}
	}

	std::vector<std::pair<int64_t, int64_t>> results;
	while (!minHeap.empty()) {
		results.push_back(minHeap.top());
		minHeap.pop();
	}

	for (auto it = results.rbegin(); it != results.rend(); ++it) {
		int32_t date_int = l_orderkey_map[it->first].second.first;
		duckdb::date_t date = duckdb::Date::EpochDaysToDate(date_int);
		std::string date_str = duckdb::Date::ToString(date);
		int32_t shippriority = l_orderkey_map[it->first].second.second;
		std::cout << it->first << " : " << std::fixed << std::setprecision(4)
				<< ((double)it->second) / 10000 << " : "
				<< date_str << " : "
				<< shippriority << std::endl;
	}

}

}