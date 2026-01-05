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


struct agg_data {
int64_t sum_qty;
int64_t count_order;
friend std::ostream& operator<<( std::ostream &output, const agg_data& D);
};
std::ostream& operator<<( std::ostream &output, const agg_data& D)
{
	output << std::fixed << std::setprecision(2) << (double)D.sum_qty / 100 << "  ";

	return output;
}


inline void flip_bitvector(ibis::bitvector *btv)
{
#if defined(__AVX512F__)
	ibis::bitvector::word_t *it = btv->m_vec.begin();

	while(it + 15 < btv->m_vec.end()) {
		_mm512_storeu_epi32(it, _mm512_andnot_epi32(_mm512_loadu_epi32(it), \
													_mm512_set1_epi32(0x7fffffff)));
		it += 16;
	}

	for(; it < btv->m_vec.end(); it++)
	 *it ^= ibis::bitvector::ALLONES;

	if (btv->active.nbits > 0) { // also need to toggle active_word
		btv->active.val ^= ((1 << btv->active.nbits) - 1);
	}
#else
	ibis::bitvector::word_t *it = btv->m_vec.begin();
    
    while (it + 15 < btv->m_vec.end()) {
        for (int i = 0; i < 16; ++i) {
            it[i] = (~it[i]) & 0x7fffffff;
        }
        it += 16;
    }

    for (; it < btv->m_vec.end(); ++it) {
        *it ^= ibis::bitvector::ALLONES;
    }

    if (btv->active.nbits > 0) {
        btv->active.val ^= ((1 << btv->active.nbits) - 1);
    }
#endif
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

inline void gather_sum_i64_from_i64_idx_chunk(
    const int64_t* A, const row_t* B, size_t total, int64_t* sum, int64_t offset, int64_t chunk_size, size_t &offset_idx)
{
#if defined(__AVX512F__)
    while (offset_idx + 8 < total && B[offset_idx + 8] < offset + chunk_size) {
        __m512i idx = _mm512_loadu_si512((const void*)(B + offset_idx));
		__m512i voffset = _mm512_set1_epi64(offset);
        __m512i rel_idx = _mm512_sub_epi64(idx, voffset);
        __m512i gathered = _mm512_i64gather_epi64(rel_idx, (const void*)A, 8);
		*sum += _mm512_reduce_add_epi64(gathered);
        offset_idx += 8;
    }
	while (offset_idx < total && B[offset_idx] < offset + chunk_size) {
		*sum += A[B[offset_idx] - offset];
		offset_idx++;
	}
#else
    for (; offset_idx < total; ++offset_idx) {
        if (B[offset_idx] >= offset && B[offset_idx] < offset + chunk_size) {
            *sum += A[B[offset_idx] - offset];
        }
    }
#endif
}

inline void groupby_aggregation(int64_t *quantity_ptr, uint16_t base, uint8_t bits, agg_data &sum) 
{
#if defined(__AVX512F__)
	if(!bits)
		return;

	__m512i indexes0 = _mm512_maskz_compress_epi64(bits, _mm512_loadu_epi64(quantity_ptr + base));
	sum.sum_qty += _mm512_reduce_add_epi64(indexes0);
#else
	if (!bits)
        return;

    for (int i = 0; i < 8; i++) {
        if (bits & (1 << i)) {
            int idx = base + i;
            
            sum.sum_qty += quantity_ptr[idx];
        }
    }		
#endif
}

// void BMTableScan::Groupby_Test(ExecutionContext &context, const PhysicalTableScan &op)
// {
//     auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	
// 	std::map<std::pair<char,char>, agg_data> q1_ans;

// 	long long groupby_time = 0;
// 	long long compute_time = 0;
// 	long long scan_time = 0;
// 	long long timer = 0;
// 	auto rabit_linestatus = dynamic_cast<rabit::Rabit *>(context.client.bitmap_linestatus);
// 	auto rabit_returnflag = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
// 	auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);

// 	auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
// 	TableScanState lineitem_scan_state;
// 	TableScanGlobalSourceState gs(context.client, op);
// 	vector<StorageIndex> storage_column_ids;
// 	storage_column_ids.push_back(StorageIndex(4));

// 	lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
// 	vector<LogicalType> types;
// 	types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);

// 	std::map<std::pair<char, char>, std::pair<vector<row_t>, int64_t> > ids_map;
// 	std::map<std::pair<char, char>, std::pair<vector<uint32_t>*, uint8_t *> > rabit_res_map;

// 	auto s0 = std::chrono::high_resolution_clock::now();
	
//     int c_year1 = 1993;
//     int c_year2 = 1997;
//     int start_year = 1970;
//     SegBtv *shipdate_res = rabit_shipdate->range_or_GE(0, c_year1 - start_year, c_year2 - c_year1 + 1);

// 	for(int i = 0; i < rabit_linestatus->config->g_cardinality; i++) {
// 		for(int r = 0; r < rabit_returnflag->config->g_cardinality; r++) {
// 			SegBtv *btv_shipdate = new SegBtv(*shipdate_res);
// 			btv_shipdate->deepCopy(*shipdate_res);
// 			btv_shipdate->decompress();

// 			SegBtv *btv_linestatus = new SegBtv(*rabit_linestatus->Btvs[i]->seg_btv);
// 			btv_linestatus->deepCopy(*rabit_linestatus->Btvs[i]->seg_btv);
// 			btv_linestatus->decompress();

// 			SegBtv *btv_returnflag = new SegBtv(*rabit_returnflag->Btvs[r]->seg_btv);
// 			btv_returnflag->deepCopy(*rabit_returnflag->Btvs[r]->seg_btv);
// 			btv_returnflag->decompress();
			
// 			auto &ttt_res = *btv_linestatus;
// 			ttt_res._and_in_thread(btv_returnflag, 0, ttt_res.seg_table.size());
// 			ttt_res._and_in_thread(btv_shipdate, 0, ttt_res.seg_table.size());
// 			uint64_t count = 0;
// 			for (const auto & [id_t, seg_t] : ttt_res.seg_table)
// 				count += seg_t->btv->count();

// 			if(!count) continue;
		
// 			vector<uint32_t> *btv_res = reduce_leadingbits_seg(ttt_res);

// 			rabit_res_map[{r, i}].first = btv_res;
// 			rabit_res_map[{r, i}].second = (uint8_t *)&((*btv_res)[0]);
// 			q1_ans[{r, i}].count_order = count;
// 		}
// 	}
// 	auto s1 = std::chrono::high_resolution_clock::now();
// 	groupby_time += std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count();

// 	int64_t cursor = 0;
// 	int64_t offset = 0;
// 	while(true) {
// 		auto xt0 = std::chrono::high_resolution_clock::now();
// 		DataChunk result;
// 		result.Initialize(context.client, types);
// 		lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
// 		if(result.size() == 0)
// 			break;
		
// 		offset = cursor;
// 		cursor += result.size();

// 		auto &quantity = result.data[0];

// 		auto quantity_data = FlatVector::GetData<int64_t>(quantity);

// 		auto xt1 = std::chrono::high_resolution_clock::now();
// 		scan_time += std::chrono::duration_cast<std::chrono::nanoseconds>(xt1 - xt0).count();

// 		auto st1 = std::chrono::high_resolution_clock::now();

// 		for(auto &ids_it : rabit_res_map) {
// 			auto &btv_it = ids_it.second.second;
// 			auto &ans_it = q1_ans[ids_it.first];
// 			uint16_t base = 0;
// 			while(base + 7 < result.size() ) {

// 				groupby_aggregation(quantity_data, base, reverse_table[*btv_it], q1_ans[ids_it.first]);
// 				btv_it++;
// 				base += 8;
// 			}
// 			if(base < result.size()) {
// 				uint8_t bits = *btv_it;
// 				while(base < result.size()) {
// 					if( bits & 0x80 ) {
// 						ans_it.sum_qty += quantity_data[base];
// 					}
// 					base++;
// 					bits <<= 1;
// 				}
// 			}
// 		}
// 		auto et1 = std::chrono::high_resolution_clock::now();
// 		compute_time += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
// 	}
// 	std::cout << "scan time : "<< scan_time/1000000 << "ms" << std::endl;
// 	std::cout << "groupby time : "<< groupby_time/1000000 << "ms" << std::endl;
// 	std::cout << "compute time : "<< compute_time/1000000 << "ms" << std::endl;
	
// 	for(auto &it : q1_ans) {
// 		std::cout << it.first.first << it.first.second << " : " << it.second << std::endl;
// 	}
// 	return;
// }

// void BMTableScan::Groupby_Test(ExecutionContext &context, const PhysicalTableScan &op)
// {
//     auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	
// 	std::map<std::pair<char,char>, agg_data> q1_ans;

// 	long long groupby_time = 0;
// 	long long compute_time = 0;
// 	long long scan_time = 0;
// 	long long timer = 0;
// 	auto rabit_linestatus = dynamic_cast<rabit::Rabit *>(context.client.bitmap_linestatus);
// 	auto rabit_returnflag = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
// 	auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);
// 	auto rabit_group1 = dynamic_cast<rabit::Rabit *>(context.client.bitmap_group1);
// 	auto rabit_group2 = dynamic_cast<rabit::Rabit *>(context.client.bitmap_group2);

// 	auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
// 	TableScanState lineitem_scan_state;
// 	TableScanGlobalSourceState gs(context.client, op);
// 	vector<StorageIndex> storage_column_ids;
// 	storage_column_ids.push_back(StorageIndex(4));

// 	lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
// 	vector<LogicalType> types;
// 	types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);

// 	std::map<std::pair<char, char>, std::pair<vector<row_t>, int64_t> > ids_map;
// 	std::map<std::pair<char, char>, std::pair<vector<uint32_t>*, uint8_t *> > rabit_res_map;

// 	auto s0 = std::chrono::high_resolution_clock::now();
	
//     int c_year = 1993;
//     int start_year = 1970;
// 	ibis::bitvector shipdate_res;
// 	shipdate_res.copy(*rabit_shipdate->Btvs_GE[c_year - start_year]->btv);
// 	shipdate_res.decompress();

// 	for(int i = c_year - start_year + 1; i <= c_year - start_year + 4; i++)
// 	shipdate_res |= *rabit_shipdate->Btvs_GE[i]->btv;

// 	for(int i = 0; i < rabit_group1->config->g_cardinality; i++) {
// 		for(int r = 0; r < rabit_group2->config->g_cardinality; r++) {
// 			auto t0 = std::chrono::high_resolution_clock::now();

// 			ibis::bitvector ttt_res, g_res;
// 			ttt_res.copy(*rabit_group1->Btvs[i]->btv);
// 			g_res.copy(*rabit_group2->Btvs[r]->btv);
// 			ttt_res.decompress();
// 			g_res.decompress();

// 			auto t1 = std::chrono::high_resolution_clock::now();
// 			timer += std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

// 			ibis::bitvector btv_res;
// 			btv_res.copy(ttt_res);
// 			btv_res &= g_res;
// 			btv_res &= shipdate_res;

// 			uint64_t count = btv_res.count();
// 			if (count == 0) continue;

// 			vector<uint32_t> *res = reduce_leadingbits(btv_res);

// 			rabit_res_map[{r, i}].first = res;
// 			rabit_res_map[{r, i}].second = (uint8_t *)&((*res)[0]);
// 			q1_ans[{r, i}].count_order = count;
// 		}
// 	}
// 	auto s1 = std::chrono::high_resolution_clock::now();
// 	groupby_time += std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count();

// 	int64_t cursor = 0;
// 	int64_t offset = 0;
// 	while(true) {
// 		auto xt0 = std::chrono::high_resolution_clock::now();
// 		DataChunk result;
// 		result.Initialize(context.client, types);
// 		lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
// 		if(result.size() == 0)
// 			break;
		
// 		offset = cursor;
// 		cursor += result.size();

// 		auto &quantity = result.data[0];

// 		auto quantity_data = FlatVector::GetData<int64_t>(quantity);

// 		auto xt1 = std::chrono::high_resolution_clock::now();
// 		scan_time += std::chrono::duration_cast<std::chrono::nanoseconds>(xt1 - xt0).count();

// 		auto st1 = std::chrono::high_resolution_clock::now();

// 		for(auto &ids_it : rabit_res_map) {
// 			auto &btv_it = ids_it.second.second;
// 			auto &ans_it = q1_ans[ids_it.first];
// 			uint16_t base = 0;
// 			while(base + 7 < result.size() ) {

// 				groupby_aggregation(quantity_data, base, reverse_table[*btv_it], q1_ans[ids_it.first]);
// 				btv_it++;
// 				base += 8;
// 			}
// 			if(base < result.size()) {
// 				uint8_t bits = *btv_it;
// 				while(base < result.size()) {
// 					if( bits & 0x80 ) {
// 						ans_it.sum_qty += quantity_data[base];
// 					}
// 					base++;
// 					bits <<= 1;
// 				}
// 			}
// 		}
// 		auto et1 = std::chrono::high_resolution_clock::now();
// 		compute_time += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
// 	}
// 	std::cout << "scan time : "<< scan_time/1000000 << "ms" << std::endl;
// 	std::cout << "groupby time : "<< groupby_time/1000000 << "ms" << std::endl;
// 	std::cout << "compute time : "<< compute_time/1000000 << "ms" << std::endl;
// 	std::cout << "timetest : "<< timer/1000000 << "ms" << std::endl;
// 	for(auto &it : q1_ans) {
// 		std::cout << it.first.first << it.first.second << " : " << it.second << std::endl;
// 	}
// 	return;
// }


void BMTableScan::Groupby_Test(ExecutionContext &context, const PhysicalTableScan &op)
{
    auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	
	std::map<std::pair<char,char>, agg_data> q1_ans;

	long long groupby_time = 0;
	long long compute_time = 0;
	long long scan_time = 0;
	long long timer = 0;
	auto rabit_linestatus = dynamic_cast<rabit::Rabit *>(context.client.bitmap_linestatus);
	auto rabit_returnflag = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
	auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);
	auto rabit_group1 = dynamic_cast<rabit::Rabit *>(context.client.bitmap_group1);
	auto rabit_group2 = dynamic_cast<rabit::Rabit *>(context.client.bitmap_group2);

	auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
	TableScanState lineitem_scan_state;
	TableScanGlobalSourceState gs(context.client, op);
	vector<StorageIndex> storage_column_ids;
	storage_column_ids.push_back(StorageIndex(4));

	lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
	vector<LogicalType> types;
	types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);

	std::map<std::pair<char, char>, std::vector<row_t>*> rabit_res_map;

	auto s0 = std::chrono::high_resolution_clock::now();
	
    int c_year = 1993;
    int start_year = 1970;
	ibis::bitvector shipdate_res;
	shipdate_res.copy(*rabit_shipdate->Btvs_GE[c_year - start_year]->btv);
	shipdate_res.decompress();

	for(int i = c_year - start_year + 1; i <= c_year - start_year + 4; i++)
	shipdate_res |= *rabit_shipdate->Btvs_GE[i]->btv;

	std::map<std::pair<char, char>, size_t> group_offset;
	for(int i = 0; i < rabit_group1->config->g_cardinality; i++) {
		for(int r = 0; r < rabit_group2->config->g_cardinality; r++) {

			auto t0 = std::chrono::high_resolution_clock::now();

			ibis::bitvector ttt_res, g_res;
			ttt_res.copy(*rabit_group1->Btvs[i]->btv);
			g_res.copy(*rabit_group2->Btvs[r]->btv);
			ttt_res.decompress();
			g_res.decompress();

			auto t1 = std::chrono::high_resolution_clock::now();
			timer += std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

			ibis::bitvector btv_res;
			btv_res.copy(ttt_res);
			btv_res &= g_res;
			btv_res &= shipdate_res;

			uint64_t count = btv_res.count();
			if (count == 0) continue;

			std::vector<row_t>* ids = new vector<row_t>;
			GetRowids(btv_res, ids);

			rabit_res_map[{r, i}] = ids; 

			group_offset[{r,i}] = 0;

		}
	}

	auto s1 = std::chrono::high_resolution_clock::now();
	groupby_time += std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count();
	
	int64_t cursor = 0;
	int64_t offset = 0;
	while(true) {
		auto xt0 = std::chrono::high_resolution_clock::now();
		DataChunk result;
		result.Initialize(context.client, types);
		lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
		if(result.size() == 0)
			break;
		
		offset = cursor;
		cursor += result.size();

		auto &quantity = result.data[0];

		auto quantity_data = FlatVector::GetData<int64_t>(quantity);

		auto xt1 = std::chrono::high_resolution_clock::now();
		scan_time += std::chrono::duration_cast<std::chrono::nanoseconds>(xt1 - xt0).count();

		auto st1 = std::chrono::high_resolution_clock::now();

		// for (auto &ids_it : rabit_res_map) {
		// 	auto ids = ids_it.second;
		// 	auto group_id = ids_it.first;
		// 	size_t offset_idx = group_offset[group_id];
		// 	while (offset_idx < ids->size() && (*ids)[offset_idx] < cursor) {
		// 		row_t rowid = (*ids)[offset_idx];
		// 		q1_ans[group_id].sum_qty += quantity_data[rowid - offset];
		// 		offset_idx++;
		// 	}
		// 	group_offset[group_id] = offset_idx;
		// }

		for (auto &ids_it : rabit_res_map) {
			auto ids = ids_it.second;
			auto group_id = ids_it.first;
			size_t &offset_idx = group_offset[group_id];
			gather_sum_i64_from_i64_idx_chunk(
				quantity_data, ids->data(), ids->size(), &q1_ans[group_id].sum_qty, offset, result.size(), offset_idx);
		}

		auto et1 = std::chrono::high_resolution_clock::now();
		compute_time += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
	}
	std::cout << "scan time : "<< scan_time/1000000 << "ms" << std::endl;
	std::cout << "groupby time : "<< groupby_time/1000000 << "ms" << std::endl;
	std::cout << "compute time : "<< compute_time/1000000 << "ms" << std::endl;
	std::cout << "timetest : "<< timer/1000000 << "ms" << std::endl;
	for(auto &it : q1_ans) {
		std::cout << it.first.first << it.first.second << " : " << it.second << std::endl;
	}
	return;
}

}
