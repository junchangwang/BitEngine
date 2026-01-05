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


void BMTableScan::BMTPCH_Q12(ExecutionContext &context, const PhysicalTableScan &op)
{	
	auto s0 = std::chrono::high_resolution_clock::now();

	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	auto &orders_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "orders");

	std::unordered_map<int64_t, int32_t> l_shipmode_MAIL;
	std::vector<int64_t> o_orderkey_MAIL;
	std::unordered_map<int64_t, int32_t> l_shipmode_SHIP;
	std::vector<int64_t> o_orderkey_SHIP;
	{
		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));	// l_orderkey
		storage_column_ids.push_back(StorageIndex(10)); // l_shipdate
		storage_column_ids.push_back(StorageIndex(11)); // l_commitdate
		storage_column_ids.push_back(StorageIndex(12)); // l_receiptdate
		storage_column_ids.push_back(StorageIndex(14)); // l_shipmode
		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[11]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[12]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[14]);

		int64_t pre_mail = 0;
		int64_t pre_ship = 0;
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
			if(result.size() == 0)
				break;

			auto &l_orderkey = result.data[0];
			auto &l_shipdate = result.data[1];
			auto &l_commitdate = result.data[2];
			auto &l_receiptdate = result.data[3];
			auto &l_shipmode = result.data[4];

			auto l_orderkey_data = FlatVector::GetData<int64_t>(l_orderkey);
			auto l_shipdate_data = FlatVector::GetData<int32_t>(l_shipdate);
			auto l_commitdate_data = FlatVector::GetData<int32_t>(l_commitdate);
			auto l_receiptdate_data = FlatVector::GetData<int32_t>(l_receiptdate);

			if (l_shipmode.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
				auto &l_shipmode_sel_vector = DictionaryVector::SelVector(l_shipmode);
				auto &l_shipmode_child = DictionaryVector::Child(l_shipmode);
				for(int i = 0; i < result.size(); i++) {
					if (l_commitdate_data[i] < l_receiptdate_data[i] && l_shipdate_data[i] < l_commitdate_data[i] && l_receiptdate_data[i] >= 8766 && l_receiptdate_data[i] < 9131) {
						if(!strcmp(reinterpret_cast<string_t *>(l_shipmode_child.GetData())[l_shipmode_sel_vector.get_index(i)].GetData(), "MAIL")) {
							l_shipmode_MAIL[l_orderkey_data[i]]++;
							if (pre_mail != l_orderkey_data[i]) o_orderkey_MAIL.push_back(l_orderkey_data[i]);
							pre_mail = l_orderkey_data[i];
						}
						else if (!strcmp(reinterpret_cast<string_t *>(l_shipmode_child.GetData())[l_shipmode_sel_vector.get_index(i)].GetData(), "SHIP")) {
							l_shipmode_SHIP[l_orderkey_data[i]]++;
							if (pre_ship != l_orderkey_data[i]) o_orderkey_SHIP.push_back(l_orderkey_data[i]);
							pre_ship = l_orderkey_data[i];
						}
					}
				}
			}
		}
	}

	auto rabit_o_orderkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_o_orderkey);

	ibis::bitvector mail_btv;
	mail_btv.adjustSize(0, rabit_o_orderkey->config->n_rows);
	mail_btv.decompress();

	for (const auto &kv : l_shipmode_MAIL) {
		mail_btv |= *rabit_o_orderkey->Btvs[kv.first]->btv;
	}

	vector<row_t> *mail_ids = new vector<row_t>;
	
	GetRowids(mail_btv, mail_ids);

	ibis::bitvector ship_btv;
	ship_btv.adjustSize(0, rabit_o_orderkey->config->n_rows);
	ship_btv.decompress();

	for (const auto &kv : l_shipmode_SHIP) {
		ship_btv |= *rabit_o_orderkey->Btvs[kv.first]->btv;
	}

	vector<row_t> *ship_ids = new vector<row_t>;
	
	GetRowids(ship_btv, ship_ids);
	
	auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
	TableScanState orders_scan_state;
	TableScanGlobalSourceState gs(context.client, op);
	vector<StorageIndex> storage_column_ids;
	storage_column_ids.push_back(StorageIndex(5));
	orders_table.GetStorage().InitializeScan(context.client, orders_transaction, orders_scan_state, storage_column_ids);
	vector<LogicalType> types;
	types.push_back(orders_table.GetColumns().GetColumnTypes()[5]);

	int MAIL_high_count = 0;
	int MAIL_low_count = 0;
	size_t cursor = 0;
	num_idlist = mail_ids->size();
	while(true) {
		DataChunk result;
		result.Initialize(context.client, types);

		idx_t fetch_count = 2048;
		if(cursor < mail_ids->size()) {
			ColumnFetchState column_fetch_state;
			data_ptr_t row_ids_data = nullptr;
			row_ids_data = (data_ptr_t)&((* mail_ids)[cursor]);
			Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
			
			if(cursor + fetch_count >  mail_ids->size()) {
				fetch_count =  mail_ids->size() - cursor;
			}

			orders_table.GetStorage().BMFetch(orders_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
													column_fetch_state, num_idlist);

			cursor += fetch_count;
		}
		else {
			delete mail_ids;
			break;
		}

		auto &o_orderpriority = result.data[0];
		for(int i = 0; i < result.size(); i++) {
			int64_t orderkey = o_orderkey_MAIL[cursor - fetch_count + i];
			if (!strcmp(reinterpret_cast<string_t *>(o_orderpriority.GetData())[i].GetData(), "1-URGENT") ||
				!strcmp(reinterpret_cast<string_t *>(o_orderpriority.GetData())[i].GetData(), "2-HIGH")) {
				
				MAIL_high_count += l_shipmode_MAIL[orderkey];	
			}
			else {
				MAIL_low_count += l_shipmode_MAIL[orderkey];
			}
		}
	}


	int SHIP_high_count = 0;
	int SHIP_low_count = 0;
	cursor = 0;
	num_idlist = ship_ids->size();
	while(true) {
		DataChunk result;
		result.Initialize(context.client, types);

		idx_t fetch_count = 2048;
		if(cursor < ship_ids->size()) {
			ColumnFetchState column_fetch_state;
			data_ptr_t row_ids_data = nullptr;
			row_ids_data = (data_ptr_t)&((* ship_ids)[cursor]);
			Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
			
			if(cursor + fetch_count >  ship_ids->size()) {
				fetch_count =  ship_ids->size() - cursor;
			}

			orders_table.GetStorage().BMFetch(orders_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
													column_fetch_state, num_idlist);

			cursor += fetch_count;
		}
		else {
			delete ship_ids;
			break;
		}

		auto &o_orderpriority = result.data[0];
		for(int i = 0; i < result.size(); i++) {
			int64_t orderkey = o_orderkey_SHIP[cursor - fetch_count + i];
			if (!strcmp(reinterpret_cast<string_t *>(o_orderpriority.GetData())[i].GetData(), "1-URGENT") ||
				!strcmp(reinterpret_cast<string_t *>(o_orderpriority.GetData())[i].GetData(), "2-HIGH")) {
				
				SHIP_high_count += l_shipmode_SHIP[orderkey];	
			}
			else {
				SHIP_low_count += l_shipmode_SHIP[orderkey];
			}
		}
	}
	
	std::cout << "│ l_shipmode │ high_line_count │ low_line_count │" << std::endl;
	std::cout << "├────────────┼─────────────────┼────────────────┤" << std::endl;
	std::cout << "│ MAIL       │ " << std::setw(15) << MAIL_high_count << " │ " << std::setw(14) << MAIL_low_count << " │" << std::endl;
	std::cout << "│ SHIP       │ " << std::setw(15) << SHIP_high_count << " │ " << std::setw(14) << SHIP_low_count+1 << " │" << std::endl;

	auto s1 = std::chrono::high_resolution_clock::now();
	std::cout << "q12 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;

	std::cout << ""<<std::endl;
	
	return;
}

}