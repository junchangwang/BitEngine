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

struct result_data {
int64_t sum_totalprice = 0;
int64_t count_order;
friend std::ostream& operator<<( std::ostream &output, const result_data& D);
};
std::ostream& operator<<( std::ostream &output, const result_data& D)
{
	output << std::fixed << std::setprecision(2) << (double)D.sum_totalprice / 100 << "  ";

	return output;
}

inline void reduce_zero(uint32_t *dst, uint32_t *src) {
#if defined(__AVX512F__)
	__m512i mask = _mm512_set_epi8(
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3,
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3,
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3,
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3
	);

	_mm512_storeu_epi32(dst,\
	_mm512_shuffle_epi8( \
	_mm512_or_si512( \
		_mm512_sllv_epi32(_mm512_loadu_epi32(src), _mm512_set_epi32(16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1)),\
		_mm512_srlv_epi32(_mm512_loadu_epi32(src + 1), _mm512_set_epi32(15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30))), mask));

	_mm512_storeu_epi32(dst + 16,\
	_mm512_shuffle_epi8( \
	_mm512_or_si512( \
		_mm512_sllv_epi32(_mm512_loadu_epi32(src + 16), _mm512_set_epi32(0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17)),\
		_mm512_srlv_epi32(_mm512_loadu_epi32(src + 17), _mm512_set_epi32(0,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14))), mask));
#else
	const uint8_t shuffle_table[64] = {
        3,2,1,0,7,6,5,4,11,10,9,8,15,14,13,12,
        3,2,1,0,7,6,5,4,11,10,9,8,15,14,13,12,
        3,2,1,0,7,6,5,4,11,10,9,8,15,14,13,12,
        3,2,1,0,7,6,5,4,11,10,9,8,15,14,13,12
    };

    const int shift_left[32] = {
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,
        17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,0,
    };

    const int shift_right[32] = {
        30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,
        14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,0
    };

    for (int i = 0; i < 32; i++) {
        uint32_t val1 = src[i] << shift_left[i];
        uint32_t val2 = src[i+1] >> shift_right[i];
        uint32_t combined = val1 | val2;

        uint32_t shuffled = 0;
        for (int j = 0; j < 4; j++) {
            uint8_t byte = (combined >> (j*8)) & 0xFF;
            uint8_t new_pos = shuffle_table[(i%16)*4 + j];
            shuffled |= (uint32_t(byte) << (new_pos*8));
        }
        dst[i] = shuffled;
    }
#endif
}

inline void join_aggregation(int64_t *price_ptr, int64_t *discount_ptr, uint16_t base, uint8_t bits, int64_t &revenue) 
{
#if defined(__AVX512F__)
	if(!bits)
		return;

	__m512i indexes0 = _mm512_maskz_compress_epi64(bits, _mm512_loadu_epi64(price_ptr + base));

	__m512i discount = _mm512_loadu_epi64(discount_ptr + base);
	__m512i indexes1 = _mm512_maskz_compress_epi64(bits, _mm512_sub_epi64(\
							_mm512_set_epi64(100, 100, 100, 100, 100, 100, 100, 100),\
								discount));
	indexes0 = _mm512_mullo_epi64(indexes0, indexes1);
	revenue += _mm512_reduce_add_epi64(indexes0);

#else
	if (!bits)
        return;

    for (int i = 0; i < 8; i++) {
        if (bits & (1 << i)) {
            int idx = base + i;
            
            int64_t price = price_ptr[idx] * (100 - discount_ptr[idx]);
            revenue += price;

        }
    }		
#endif
}

inline void agg(int64_t* extendedprice_ptr, uint16_t base, int64_t &sum_price) {
#if defined(__AVX512F__)
    __m512i data_vec = _mm512_loadu_si512((__m512i const*)(extendedprice_ptr + base));
    sum_price += _mm512_reduce_add_epi64(data_vec);
#else
    for (int i = 0; i < 8; i++) {
        sum_price += extendedprice_ptr[base + i];
    }
#endif
}

inline void get_row_id(ibis::bitvector btv, vector<row_t> *ids) {
	for (ibis::bitvector::indexSet index_set = btv.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
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
}

// void BMTableScan::HashJoin(ExecutionContext &context, const PhysicalTableScan &op)
// {
// 	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");

// 	double whole_time = 0;
// 	double join_time = 0;

//     result_data agg_data;

// 	auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
// 	TableScanState lineitem_scan_state;
// 	TableScanGlobalSourceState gs(context.client, op);
// 	vector<StorageIndex> storage_column_ids;
// 	storage_column_ids.push_back(StorageIndex(5));
// 	lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
// 	vector<LogicalType> types;
// 	types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);

// 	auto rabit_l_orderkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderkey);

// 	auto t0 = std::chrono::high_resolution_clock::now();

// 	bool first = true;
// 	ibis::bitvector ttt_res;
// 	for (int i = 0; i < 60000001; i += 10) {
// 		int remain = 0;
// 		if(i == 60000000) break;
// 		while (remain < 1){
// 			if(first) {
// 				ttt_res.copy(*rabit_l_orderkey->Btvs[i + remain]->btv);
// 				ttt_res.decompress();
// 				first = false;
// 			}
// 			else {
// 				ttt_res |= *rabit_l_orderkey->Btvs[i + remain]->btv;
// 			}
// 			remain++;
// 		}
// 	}
// 	ttt_res |= *rabit_l_orderkey->Btvs[60000000]->btv;
	
// 	auto t1 = std::chrono::high_resolution_clock::now();

// 	join_time += std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

// 	vector<uint32_t> *btv_res = new vector<uint32_t>(ttt_res.size() / 32 + (ttt_res.size() % 32?1:0) + 4);

// 	uint32_t *dst_data = (uint32_t *)&(*btv_res)[0];
// 	uint32_t *src_data = ttt_res.m_vec.begin();

// 	// load 31 dst once
// 	while(src_data + 31 < ttt_res.m_vec.end()) {
// 		reduce_zero(dst_data, src_data);
// 		dst_data += 31;
// 		src_data += 32;
// 	}

// 	// 1 dst once
// 	int need_bits = 31;
// 	while(src_data + 1 < ttt_res.m_vec.end()) {
// 		dst_data[0] = ((src_data[0] << (32 - need_bits)) | (src_data[1] >> (need_bits - 1)));
// 		dst_data[0] = htonl(dst_data[0]);
// 		need_bits--;
// 		src_data++;
// 		dst_data++;
// 	}

// 	// active word
// 	dst_data[0] = src_data[0] << (32 - need_bits);
// 	uint32_t last_word = ttt_res.active.val << (32 - ttt_res.active.nbits);
// 	if(ttt_res.active.nbits + need_bits > 32) {
// 		dst_data[0] |= (last_word >> need_bits);
// 		dst_data[0] = htonl(dst_data[0]);
// 		last_word <<= (32 - need_bits);
// 		dst_data[1] = last_word;
// 		dst_data[1] = htonl(dst_data[1]);
// 	}
// 	else {
// 		dst_data[0] |= (last_word >> need_bits);
// 		dst_data[0] = htonl(dst_data[0]);
// 	}
	
// 	uint8_t* btv_res_ptr = (uint8_t *)&((*btv_res)[0]);

// 	int64_t cursor = 0;
// 	int64_t offset = 0;
// 	while(true) {

// 		DataChunk result;
// 		result.Initialize(context.client, types);
// 		lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);

// 		if(result.size() == 0) break;

// 		offset = cursor;
// 		cursor += result.size();

// 		auto &totalprice = result.data[0];

// 		auto totalprice_data = FlatVector::GetData<int64_t>(totalprice);

// 		uint16_t base = 0;
// 		while(base + 7 < result.size()) {
// 			join_aggregation(totalprice_data, base, reverse_table_join[*btv_res_ptr],agg_data);
// 			btv_res_ptr++;
// 			base += 8;
// 		}
// 		if(base < result.size()) {
// 			uint8_t bits = *btv_res_ptr;
// 			while(base < result.size()) {
// 				bool has_bit = bits & 0x80;
// 				agg_data.sum_totalprice += has_bit * totalprice_data[base];
// 				base++;
// 				bits <<= 1;
// 			}
// 		}
//     }

//     std::cout<<"revenue: "<<agg_data<<std::endl;

// 	auto t2 = std::chrono::high_resolution_clock::now();
// 	whole_time += join_time + std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();

// 	std::cout << "join time : "<< join_time/1000000 << "ms" << std::endl;
// 	std::cout << "whole time : "<< whole_time/1000000 << "ms" << std::endl;

// 	return;
// }


// void BMTableScan::HashJoin(ExecutionContext &context, const PhysicalTableScan &op)
// {
// 	double whole_time = 0;
// 	double join_time = 0;
// 	double fetch_time = 0;
	
// 	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
// 	auto &orders_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "orders");

// 	auto rabit_l_orderkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderkey);
// 	ibis::bitvector btv_res;
// 	btv_res.adjustSize(0, rabit_l_orderkey->config->n_rows);
// 	btv_res.decompress();

// 	std::unordered_set<int64_t> o_orderkey_set;
// 	std::unordered_map<int64_t, std::pair<int64_t,int32_t>> output;
// 	{
// 		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
// 		TableScanState orders_scan_state;
// 		TableScanGlobalSourceState gs(context.client, op);
// 		vector<StorageIndex> storage_column_ids;
// 		storage_column_ids.push_back(StorageIndex(0));
// 		storage_column_ids.push_back(StorageIndex(4));
// 		orders_table.GetStorage().InitializeScan(context.client, orders_transaction, orders_scan_state, storage_column_ids);
// 		vector<LogicalType> types;
// 		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);
// 		types.push_back(orders_table.GetColumns().GetColumnTypes()[4]);

// 		while(true) {
// 			DataChunk result;
// 			result.Initialize(context.client, types);
// 			orders_table.GetStorage().Scan(orders_transaction, result, orders_scan_state);
// 			if(result.size() == 0)
// 				break;

// 			auto &o_orderkey = result.data[0];
// 			auto &o_orderdate = result.data[1];

// 			auto o_orderkey_data = FlatVector::GetData<int64_t>(o_orderkey);
// 			auto o_orderdate_data = FlatVector::GetData<int32_t>(o_orderdate);

			

// 			for(int i = 0; i < result.size(); i++) {
// 				if(o_orderkey_data[i] % 10  < 8) {
// 					o_orderkey_set.insert(o_orderkey_data[i]);
// 					output.emplace(o_orderkey_data[i], std::make_pair(0LL, o_orderdate_data[i]));
// 				}
// 			}
			
// 		}

// 	}

	
// 	auto t0 = std::chrono::high_resolution_clock::now();
// 	for(int64_t v : o_orderkey_set) {
// 		btv_res |= *rabit_l_orderkey->Btvs[v]->btv;
// 	}
// 	auto t1 = std::chrono::high_resolution_clock::now();
// 	join_time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
// 	{
// 		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
// 		TableScanState lineitem_scan_state;
// 		TableScanGlobalSourceState gs(context.client, op);
// 		vector<StorageIndex> storage_column_ids;
// 		storage_column_ids.push_back(StorageIndex(0));
// 		storage_column_ids.push_back(StorageIndex(5));
// 		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
// 		vector<LogicalType> types;
// 		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[0]);
// 		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);

// 		vector<row_t> *ids = new vector<row_t>;
// 		size_t cursor = 0;

// 		for (ibis::bitvector::indexSet index_set = btv_res.firstIndexSet(); index_set.nIndices() > 0; ++index_set) {
// 			const ibis::bitvector::word_t *indices = index_set.indices();
// 			if (index_set.isRange()) {
// 				for (ibis::bitvector::word_t j = *indices; j < indices[1]; ++j) {
// 					ids->push_back((uint64_t)j);
// 				}
// 			} else {
// 				for (unsigned j = 0; j < index_set.nIndices(); ++j) {
// 					ids->push_back((uint64_t)indices[j]);
// 				}
// 			}
// 		}
// 		num_idlist = ids->size();
// 		while(true) {
// 			DataChunk result;
// 			result.Initialize(context.client, types);
			
// 			auto s_fetch = std::chrono::high_resolution_clock::now();

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

// 				auto e_fetch = std::chrono::high_resolution_clock::now();
// 				fetch_time += std::chrono::duration_cast<std::chrono::nanoseconds>(e_fetch - s_fetch).count();
// 			}
// 			else {
// 				delete ids;
// 				break;
// 			}

// 			auto &l_order_key = result.data[0];
// 			auto &l_extendedprice = result.data[1];

// 			auto l_order_key_data = FlatVector::GetData<int64_t>(l_order_key);
// 			auto l_extendedprice_data = FlatVector::GetData<int64_t>(l_extendedprice);

// 			for(int i = 0; i < result.size(); i++) {
// 				auto it = output.find(l_order_key_data[i]);
// 				it->second.first += l_extendedprice_data[i];
// 			}
// 		}

// 	}

// 	auto t2 = std::chrono::high_resolution_clock::now();
// 	whole_time = join_time + std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();


// 	std::cout << "join time : "<< join_time << "ms" << std::endl;
// 	std::cout << "fetch time : "<< fetch_time / 1000000 << "ms" << std::endl;
// 	std::cout << "whole time : "<< whole_time << "ms" << std::endl;

// 	std::vector<std::pair<int64_t, std::pair<int64_t, int32_t>>> sorted_output(output.begin(), output.end());
// 	std::sort(sorted_output.begin(), sorted_output.end(), [](const auto &a, const auto &b) {
// 		return a.first < b.first;
// 	});

// 	std::cout << "l_orderkey  revenue   o_orderdate" << std::endl;
// 	int cnt = 0;
// 	for (const auto &kv : sorted_output) {
// 		int32_t date_int = kv.second.second;
// 		duckdb::date_t date = duckdb::Date::EpochDaysToDate(date_int);
// 		std::string date_str = duckdb::Date::ToString(date);
// 		std::cout << kv.first
// 				<< " : " << std::fixed << std::setprecision(2)<< ((double)kv.second.first) / 100
// 				<< " : " << date_str << std::endl;
// 		cnt++;
// 		if (cnt >= 10) break;
// 	}
	
// 	return;
// }

// void BMTableScan::HashJoin(ExecutionContext &context, const PhysicalTableScan &op)
// {
// 	double time1 = 0;
// 	double time2 = 0;

// 	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
// 	auto &orders_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "orders");

// 	auto rabit_l_orderkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderkey);
// 	auto rabit_orderdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderdate);

// 	vector<std::pair<std::pair<int32_t, int64_t>, ibis::bitvector>> output;
// 	auto t0 = std::chrono::high_resolution_clock::now();
// 	{
// 		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
// 		TableScanState orders_scan_state;
// 		TableScanGlobalSourceState gs(context.client, op);
// 		vector<StorageIndex> storage_column_ids;
// 		storage_column_ids.push_back(StorageIndex(0));
// 		orders_table.GetStorage().InitializeScan(context.client, orders_transaction, orders_scan_state, storage_column_ids);
// 		vector<LogicalType> types;
// 		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);

// 		for (int i = 8034; i <= 10439; i++) {

// 			ibis::bitvector btv_okey;
// 			btv_okey.adjustSize(0, rabit_l_orderkey->config->n_rows);
// 			btv_okey.decompress();

// 			vector<row_t> *ids = new vector<row_t>;
// 			size_t cursor = 0;

// 			get_row_id(*(rabit_orderdate->Btvs[i]->btv), ids);

// 			num_idlist = ids->size();
// 			while(true) {
// 				DataChunk result;
// 				result.Initialize(context.client, types);

// 				if(cursor < ids->size()) {
// 					ColumnFetchState column_fetch_state;
// 					data_ptr_t row_ids_data = nullptr;
// 					row_ids_data = (data_ptr_t)&((*ids)[cursor]);
// 					Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
// 					idx_t fetch_count = 2048;
// 					if(cursor + fetch_count > ids->size()) {
// 						fetch_count = ids->size() - cursor;
// 					}

// 					orders_table.GetStorage().BMFetch(orders_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
// 															column_fetch_state, num_idlist);

// 					cursor += fetch_count;

// 				}
// 				else {
// 					delete ids;
// 					break;
// 				}
// 				auto &o_orderkey = result.data[0];
// 				auto o_orderkey_data = FlatVector::GetData<int64_t>(o_orderkey);
				
// 				for(int j = 0; j < result.size(); j++) {
// 					if (o_orderkey_data[j] % 10 < 1) {
// 						btv_okey |= *rabit_l_orderkey->Btvs[o_orderkey_data[j]]->btv;
// 					}
// 				}
// 			}
// 			output.emplace_back(std::make_pair(i, 0LL), std::move(btv_okey));

// 		}
// 	}
// 	auto t1 = std::chrono::high_resolution_clock::now();
// 	time1 = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
// 	{
// 		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
// 		TableScanState lineitem_scan_state;
// 		TableScanGlobalSourceState gs(context.client, op);
// 		vector<StorageIndex> storage_column_ids;
// 		storage_column_ids.push_back(StorageIndex(5));
// 		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
// 		vector<LogicalType> types;
// 		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);

// 		for (auto &kv : output) {
// 			vector<row_t> *ids = new vector<row_t>;
// 			size_t cursor = 0;

// 			get_row_id(kv.second, ids);

// 			num_idlist = ids->size();
// 			while(true) {
// 				DataChunk result;
// 				result.Initialize(context.client, types);

// 				if(cursor < ids->size()) {
// 					ColumnFetchState column_fetch_state;
// 					data_ptr_t row_ids_data = nullptr;
// 					row_ids_data = (data_ptr_t)&((*ids)[cursor]);
// 					Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
// 					idx_t fetch_count = 2048;
// 					if(cursor + fetch_count > ids->size()) {
// 						fetch_count = ids->size() - cursor;
// 					}

// 					lineitem_table.GetStorage().BMFetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
// 															column_fetch_state, num_idlist);

// 					cursor += fetch_count;

// 				}
// 				else {
// 					delete ids;
// 					break;
// 				}
// 				auto &l_extendedprice = result.data[0];
// 				auto l_extendedprice_data = FlatVector::GetData<int64_t>(l_extendedprice);

// 				uint16_t base = 0;
// 				while(base + 7 < result.size()) {
// 					agg(l_extendedprice_data, base, kv.first.second);
// 					base += 8;
// 				}
// 				if(base < result.size()) {
// 					while(base < result.size()) {
// 						kv.first.second += l_extendedprice_data[base];
// 						base++;
// 					}
// 				}
// 			}
// 		}
		
// 	}

// 	auto t2 = std::chrono::high_resolution_clock::now();
// 	time2 = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
// 	std::cout << "time1: " <<time1<< "ms" << std::endl;
// 	std::cout << "time2: " <<time2<< "ms" << std::endl;
// 	std::cout << "whole_time: " <<time1 + time2<< "ms" << std::endl;

// 	std::cout <<"o_orderdate  revenue " << std::endl;
// 	int cnt = 0;
// 	for (const auto &kv : output) {
// 		int32_t date_int = kv.first.first + 1;
// 		duckdb::date_t date = duckdb::Date::EpochDaysToDate(date_int);
// 		std::string date_str = duckdb::Date::ToString(date);
// 		std::cout<<date_str<<"  :  "
// 				 <<std::fixed << std::setprecision(2)<< ((double)kv.first.second) / 100 << std::endl;
// 		cnt++;
// 		if (cnt >= 10) break;
// 	}
	
// 	return;
// }

void BMTableScan::HashJoin(ExecutionContext &context, const PhysicalTableScan &op)
{	
	double whole_time = 0;
	double join_orderkey = 0;
	double join_custkey = 0;

	auto t0 = std::chrono::high_resolution_clock::now();

	auto &nation_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "nation");
	auto &customer_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "customer");
	auto &orders_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "orders");
	auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");

	std::unordered_set<int32_t> r_regionkey_set = {0, 1, 2, 3, 4};

	//get <n_nationkey,r_name>
	vector<int32_t> n_nationkey_vec;
	{
		auto &nation_transaction = DuckTransaction::Get(context.client, nation_table.catalog);
		TableScanState nation_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(2));
		nation_table.GetStorage().InitializeScan(context.client, nation_transaction, nation_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(nation_table.GetColumns().GetColumnTypes()[2]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			nation_table.GetStorage().Scan(nation_transaction, result, nation_scan_state);
			if(result.size() == 0)
				break;

			auto &n_regionkey = result.data[0];
			auto n_regionkey_data = FlatVector::GetData<int32_t>(n_regionkey);

			for(int i = 0; i < result.size(); i++) {
				if(r_regionkey_set.count(n_regionkey_data[i]))
					n_nationkey_vec.emplace_back(n_regionkey_data[i]);
			}
		}
	}

	//get <c_custkey,r_name>
	vector<int32_t> c_custkey_vec;
	{
		auto &customer_transaction = DuckTransaction::Get(context.client, customer_table.catalog);
		TableScanState customer_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(3));
		customer_table.GetStorage().InitializeScan(context.client, customer_transaction, customer_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(customer_table.GetColumns().GetColumnTypes()[3]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			customer_table.GetStorage().Scan(customer_transaction, result, customer_scan_state);
			if(result.size() == 0)
				break;

			auto &c_nationkey = result.data[0];
			auto c_nationkey_data = FlatVector::GetData<int32_t>(c_nationkey);

			for(int i = 0; i < result.size(); i++) {
				c_custkey_vec.emplace_back(n_nationkey_vec[c_nationkey_data[i]]);
			}
		}
		c_custkey_vec.pop_back();
	}

	//for each r_name,get o_orderkey value
	std::unordered_map<int32_t,vector<int64_t>> region_orders_map;
	{
		auto &orders_transaction = DuckTransaction::Get(context.client, orders_table.catalog);
		TableScanState orders_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(0));
		storage_column_ids.push_back(StorageIndex(1));
		orders_table.GetStorage().InitializeScan(context.client, orders_transaction, orders_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(orders_table.GetColumns().GetColumnTypes()[0]);
		types.push_back(orders_table.GetColumns().GetColumnTypes()[1]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			orders_table.GetStorage().Scan(orders_transaction, result, orders_scan_state);
			if(result.size() == 0)
				break;

			auto &o_orderkey = result.data[0];
			auto &o_custkey = result.data[1];
			auto o_orderkey_data = FlatVector::GetData<int64_t>(o_orderkey);
			auto o_custkey_data = FlatVector::GetData<int64_t>(o_custkey);

			auto s_ckey = std::chrono::high_resolution_clock::now();

			for(int i = 0; i < result.size(); i++) {
				if (o_orderkey_data[i] % 10 < 1) {
					region_orders_map[c_custkey_vec[o_custkey_data[i] - 1]].push_back(o_orderkey_data[i]);
				}
			}

			auto e_ckey = std::chrono::high_resolution_clock::now();
			join_custkey += std::chrono::duration_cast<std::chrono::nanoseconds>(e_ckey - s_ckey).count();
		}
	}

	//for each r_name,OR l_orderkey bitvector 
	std::unordered_map<int32_t, std::shared_ptr<std::vector<uint32_t>>> region_btv_map;
	auto rabit_l_orderkey = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderkey);
	for (auto &kv : region_orders_map) {
		int32_t region_id = kv.first;
		const auto &orderkey_vec = kv.second;

		auto s_okey = std::chrono::high_resolution_clock::now();

		ibis::bitvector ttt_res;
		ttt_res.adjustSize(0, rabit_l_orderkey->config->n_rows);
		ttt_res.decompress();

		for (auto orderkey : orderkey_vec) {
			ttt_res |= *rabit_l_orderkey->Btvs[orderkey]->btv;
		}

		auto e_okey = std::chrono::high_resolution_clock::now();
		join_orderkey += std::chrono::duration_cast<std::chrono::milliseconds>(e_okey - s_okey).count();

		auto btv_res = std::make_shared<std::vector<uint32_t>>(ttt_res.size() / 32 + (ttt_res.size() % 32 ? 1 : 0) + 4);

		uint32_t *dst_data = (uint32_t *)&(*btv_res)[0];
		uint32_t *src_data = ttt_res.m_vec.begin();

		// load 31 dst once
		while(src_data + 31 < ttt_res.m_vec.end()) {
			reduce_zero(dst_data, src_data);
			dst_data += 31;
			src_data += 32;
		}

		// 1 dst once
		int need_bits = 31;
		while(src_data + 1 < ttt_res.m_vec.end()) {
			dst_data[0] = ((src_data[0] << (32 - need_bits)) | (src_data[1] >> (need_bits - 1)));
			dst_data[0] = htonl(dst_data[0]);
			need_bits--;
			src_data++;
			dst_data++;
		}

		// active word
		dst_data[0] = src_data[0] << (32 - need_bits);
		uint32_t last_word = ttt_res.active.val << (32 - ttt_res.active.nbits);
		if(ttt_res.active.nbits + need_bits > 32) {
			dst_data[0] |= (last_word >> need_bits);
			dst_data[0] = htonl(dst_data[0]);
			last_word <<= (32 - need_bits);
			dst_data[1] = last_word;
			dst_data[1] = htonl(dst_data[1]);
		}
		else {
			dst_data[0] |= (last_word >> need_bits);
			dst_data[0] = htonl(dst_data[0]);
		}
		
		region_btv_map[region_id] = btv_res;
	}

	//for each result bitmap,aggregate the answer by avx512
	std::unordered_map<int32_t,int64_t> output;
	for (auto &kv : region_btv_map) {
		int64_t cursor = 0;
		int64_t offset = 0;
		uint8_t* btv_res_ptr = reinterpret_cast<uint8_t*>(kv.second->data());

		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(5));
		storage_column_ids.push_back(StorageIndex(6));
		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
			if(result.size() == 0)
				break;
			
			offset = cursor;
			cursor += result.size();

			auto &l_extendedprice = result.data[0];
			auto &l_discount = result.data[1];
			auto l_extendedprice_data = FlatVector::GetData<int64_t>(l_extendedprice);
			auto l_discount_data = FlatVector::GetData<int64_t>(l_discount);

			int32_t region_id = kv.first;

			uint16_t base = 0;
			while(base + 7 < result.size()) {
				join_aggregation(l_extendedprice_data, l_discount_data, base, reverse_table[*btv_res_ptr], output[region_id]);
				btv_res_ptr++;
				base += 8;
			}
			if(base < result.size()) {
				uint8_t bits = *btv_res_ptr;
				while(base < result.size()) {
					bool has_bit = bits & 0x80;
					output[region_id] += has_bit * l_extendedprice_data[base] * (100 - l_discount_data[base]);
					base++;
					bits <<= 1;
				}
			}
		}
	}

	auto t1 = std::chrono::high_resolution_clock::now();
	whole_time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

	std::cout << "orderkey join: " <<join_orderkey<< "ms" << std::endl;
	std::cout << "custkey join: " <<join_custkey / 1000000<< "ms" << std::endl;
	std::cout << "whole time: " <<whole_time<< "ms" << std::endl;
	std::cout << "\n";

	//order by revenue DESC
	std::string region_names[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
	std::cout << "r_name  revenue" << std::endl;

	std::vector<std::pair<int32_t, int64_t>> sorted_output(output.begin(), output.end());
	std::sort(sorted_output.begin(), sorted_output.end(), [](const auto &a, const auto &b) {
		return a.second > b.second; 
	});

	for (const auto &kv : sorted_output) {
		std::cout << region_names[kv.first] << " : " << std::fixed << std::setprecision(4)
				<< ((double)kv.second) / 10000 << std::endl;
	}
	std::cout << "\n\n";

	return;
}

}