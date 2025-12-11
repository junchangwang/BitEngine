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

void BMTableScan::BMTPCH_Q17(ExecutionContext &context, const PhysicalTableScan &op)
{
    auto s0 = std::chrono::high_resolution_clock::now();
	auto &part_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "part");
	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");

	const char *brand = "Brand#23";
	const char *container = "MED BOX";

	ibis::bitvector btv_res;
	auto rabit_partkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_partkey);
	btv_res.adjustSize(0, rabit_partkey->config->n_rows);
	btv_res.decompress();
	{
		auto &part_transaction = DuckTransaction::Get(context.client, part_table.catalog);
		TableScanState part_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(3));
		storage_column_ids.push_back(StorageIndex(6));
		part_table.GetStorage().InitializeScan(context.client, part_transaction, part_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(part_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(part_table.GetColumns().GetColumnTypes()[3]);
		types.push_back(part_table.GetColumns().GetColumnTypes()[6]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			part_table.GetStorage().Scan(part_transaction, result, part_scan_state);
			if(result.size() == 0)
				break;

			auto &part_key = result.data[0];
			auto &part_brand = result.data[1];
			auto &part_container = result.data[2];
			auto part_key_data = FlatVector::GetData<int64_t>(part_key);

			if(part_brand.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &brand_sel_vector = DictionaryVector::SelVector(part_brand);
				auto &brand_child = DictionaryVector::Child(part_brand);
				auto &container_sel_vector = DictionaryVector::SelVector(part_container);
				auto &container_child = DictionaryVector::Child(part_container);

				for(int i = 0; i < result.size(); i++) {
					if(!strcmp(reinterpret_cast<string_t *>(brand_child.GetData())[brand_sel_vector.get_index(i)].GetData(), brand) && \
						!strcmp(reinterpret_cast<string_t *>(container_child.GetData())[container_sel_vector.get_index(i)].GetData(), container))
						btv_res |= *rabit_partkey->Btvs[part_key_data[i]]->btv;
				}
			}
			else {
				for(int i = 0; i < result.size(); i++) {
					if(!strcmp(reinterpret_cast<string_t *>(part_brand.GetData())[i].GetData(), brand) && \
						!strcmp(reinterpret_cast<string_t *>(part_container.GetData())[i].GetData(), container))
						btv_res |= *rabit_partkey->Btvs[part_key_data[i]]->btv;
				}
			}
		}
	}

	vector<row_t> *row_ids = new vector<row_t>;

	GetRowids(btv_res, row_ids);

	std::unordered_map<int64_t, std::pair<int64_t, int32_t> > partkey_quantity_map;
	std::unordered_map<int64_t, double> avg_quantity_map;
	{
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(1));
		storage_column_ids.push_back(StorageIndex(4));
		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[1]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);

		size_t cursor = 0;

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
				break;
			}

			auto &part_key = result.data[0];
			auto &quantity = result.data[1];
			auto part_key_data = FlatVector::GetData<int64_t>(part_key);
			auto quantity_data = FlatVector::GetData<int64_t>(quantity);

			for(int i = 0; i < result.size(); i++) {
				auto &it = partkey_quantity_map[part_key_data[i]];
				it.first += quantity_data[i];
				it.second++;
			}
		}

		for(auto it = partkey_quantity_map.begin(); it != partkey_quantity_map.end(); it++) {
			avg_quantity_map[it->first] = 0.2 * ((double)it->second.first / it->second.second);
		}

	}

	double sum_extendedprice = 0;
	{
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(1));
		storage_column_ids.push_back(StorageIndex(4));
		storage_column_ids.push_back(StorageIndex(5));
		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[1]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);

		size_t cursor = 0;

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
				break;
			}

			auto &part_key = result.data[0];
			auto &quantity = result.data[1];
			auto &extendedprice = result.data[2];
			auto part_key_data = FlatVector::GetData<int64_t>(part_key);
			auto quantity_data = FlatVector::GetData<int64_t>(quantity);
			auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);

			for(int i = 0; i < result.size(); i++) {
				if((double)quantity_data[i] < avg_quantity_map[part_key_data[i]])
					sum_extendedprice += extendedprice_data[i];
			}
		}
	}

	delete row_ids;
	auto e0 = std::chrono::high_resolution_clock::now();

    sum_extendedprice /= 100.0;
	std::cout << "sum : " <<std::fixed << std::setprecision(2)<< sum_extendedprice << std::endl;
	double q17_ans = sum_extendedprice / 7.0;
	std::cout << "avg : " << std::fixed << std::setprecision(9) << q17_ans << std::endl;
	std::cout << "q17 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(e0 - s0).count() << "ms" << std::endl;

    return;
}




}