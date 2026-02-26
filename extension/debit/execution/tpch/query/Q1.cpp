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


struct q1_data {
int64_t sum_qty;
int64_t sum_base_price;
int64_t sum_disc_price;
int64_t sum_charge;
int64_t count_order;
int64_t sum_discount;
friend std::ostream& operator<<( std::ostream &output, const q1_data& D);
};
std::ostream& operator<<( std::ostream &output, const q1_data& D)
{
	output << std::fixed << std::setprecision(2) << (double)D.sum_qty / 100 << "  " << \
				 std::fixed << std::setprecision(2) << (double)D.sum_base_price / 100  << " " << \
				 std::fixed << std::setprecision(4) << (double)D.sum_disc_price / 10000 << "  " << \
				 std::fixed << std::setprecision(6) << (double)D.sum_charge / 1000000 << "  " <<\
				 (double)D.sum_qty / D.count_order / 100<< "  " << (double)D.sum_base_price / D.count_order / 100 << \
				 "  " << (double)D.sum_discount / D.count_order / 100 << "  " << D.count_order;

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

inline void bit_and(uint32_t* dst, uint32_t* src, size_t n_words) {
#if defined(__AVX512F__)
    size_t i = 0;
    for (; i + 15 < n_words; i += 16) {
        __m512i v_dst = _mm512_loadu_si512(dst + i);
        __m512i v_src = _mm512_loadu_si512(src + i);
        v_dst = _mm512_and_si512(v_dst, v_src);
        _mm512_storeu_si512(dst + i, v_dst);
    }
    for (; i < n_words; i++) {
        dst[i] &= src[i];
    }
#else
    for (size_t i = 0; i < n_words; i++) {
        dst[i] &= src[i];
    }
#endif
}


inline void triple_bit_and(uint32_t* dst, uint32_t* src1, uint32_t* src2, size_t n_words) {
#if defined(__AVX512F__)
    size_t i = 0;
    for (; i + 15 < n_words; i += 16) {
        __m512i v_dst = _mm512_loadu_si512(dst + i);
        __m512i v_src1 = _mm512_loadu_si512(src1 + i);
		__m512i v_src2 = _mm512_loadu_si512(src2 + i);
        v_dst = _mm512_and_si512(v_dst, v_src1);
		v_dst = _mm512_and_si512(v_dst, v_src2);
        _mm512_storeu_si512(dst + i, v_dst);
    }
    for (; i < n_words; i++) {
        dst[i] &= src1[i];
		dst[i] &= src2[i];
    }
#else
#endif
}

inline void q1_exe_aggregation(int64_t *quantity_ptr, int64_t *price_ptr, int64_t *discount_ptr, int64_t *tax_ptr, \
						uint16_t base, uint8_t bits, q1_data &sum) 
{
#if defined(__AVX512F__)
	if(!bits)
		return;

	__m512i indexes0 = _mm512_maskz_compress_epi64(bits, _mm512_loadu_epi64(quantity_ptr + base));
	sum.sum_qty += _mm512_reduce_add_epi64(indexes0);

	__m512i discount = _mm512_loadu_epi64(discount_ptr + base);
	__m512i index_discount = _mm512_maskz_compress_epi64(bits, discount);
	sum.sum_discount += _mm512_reduce_add_epi64(index_discount);
	
	indexes0 = _mm512_maskz_compress_epi64(bits, _mm512_loadu_epi64(price_ptr + base));
	sum.sum_base_price += _mm512_reduce_add_epi64(indexes0);

	__m512i indexes1 = _mm512_maskz_compress_epi64(bits, _mm512_sub_epi64(\
							_mm512_set_epi64(100, 100, 100, 100, 100, 100, 100, 100),\
								discount));
	indexes0 = _mm512_mullo_epi64(indexes0, indexes1);
	sum.sum_disc_price += _mm512_reduce_add_epi64(indexes0);

	indexes1 = _mm512_maskz_compress_epi64(bits, _mm512_add_epi64(\
					_mm512_set_epi64(100, 100, 100, 100, 100, 100, 100, 100),\
						_mm512_loadu_epi64(tax_ptr + base)));
	indexes0 = _mm512_mullo_epi64(indexes0, indexes1);
	sum.sum_charge += _mm512_reduce_add_epi64(indexes0);
#else
	if (!bits)
        return;

    for (int i = 0; i < 8; i++) {
        if (bits & (1 << i)) {
            int idx = base + i;
            
            sum.sum_qty += quantity_ptr[idx];
            
            int64_t discount = discount_ptr[idx];
            sum.sum_discount += discount;
            
            int64_t price = price_ptr[idx];
            sum.sum_base_price += price;
            
            int64_t disc_price = price * (100 - discount);
            sum.sum_disc_price += disc_price;
            
            int64_t tax = tax_ptr[idx];
            sum.sum_charge += disc_price * (100 + tax) / 100;
        }
    }		
#endif
}

inline void q1_exe_aggregation_batch(int64_t *quantity_ptr, int64_t *price_ptr, int64_t *discount_ptr, int64_t *tax_ptr, uint16_t base,
                                     const std::vector<uint8_t> &reverse_table_values, const std::vector<q1_data*> &sums) {
#if defined(__AVX512F__)
    if (reverse_table_values.size() < 4 || sums.size() < 4) {
        return;
    }

    __m512i quantity = _mm512_loadu_epi64(quantity_ptr + base);


    __m512i index_quantity = _mm512_maskz_compress_epi64(reverse_table_values[0], quantity);
    sums[0]->sum_qty += _mm512_reduce_add_epi64(index_quantity);

	index_quantity = _mm512_maskz_compress_epi64(reverse_table_values[1], quantity);
    sums[1]->sum_qty += _mm512_reduce_add_epi64(index_quantity);

	index_quantity = _mm512_maskz_compress_epi64(reverse_table_values[2], quantity);
    sums[2]->sum_qty += _mm512_reduce_add_epi64(index_quantity);

	index_quantity = _mm512_maskz_compress_epi64(reverse_table_values[3], quantity);
    sums[3]->sum_qty += _mm512_reduce_add_epi64(index_quantity);


    __m512i price = _mm512_loadu_epi64(price_ptr + base);
	__m512i discount = _mm512_loadu_epi64(discount_ptr + base);
	__m512i tax = _mm512_loadu_epi64(tax_ptr + base);


    __m512i index_price = _mm512_maskz_compress_epi64(reverse_table_values[0], price);
    sums[0]->sum_base_price += _mm512_reduce_add_epi64(index_price);
	__m512i index_discount = _mm512_maskz_compress_epi64(reverse_table_values[0], discount);
    sums[0]->sum_discount += _mm512_reduce_add_epi64(index_discount);
	__m512i index_disc_price = _mm512_mullo_epi64(index_price, _mm512_sub_epi64(_mm512_set1_epi64(100), index_discount));
    sums[0]->sum_disc_price += _mm512_reduce_add_epi64(index_disc_price);
	__m512i index_charge = _mm512_mullo_epi64(index_disc_price, _mm512_add_epi64(_mm512_set1_epi64(100), tax));
    sums[0]->sum_charge += _mm512_reduce_add_epi64(index_charge);

	index_price = _mm512_maskz_compress_epi64(reverse_table_values[1], price);
    sums[1]->sum_base_price += _mm512_reduce_add_epi64(index_price);
	index_discount = _mm512_maskz_compress_epi64(reverse_table_values[1], discount);
    sums[1]->sum_discount += _mm512_reduce_add_epi64(index_discount);
	index_disc_price = _mm512_mullo_epi64(index_price, _mm512_sub_epi64(_mm512_set1_epi64(100), index_discount));
    sums[1]->sum_disc_price += _mm512_reduce_add_epi64(index_disc_price);
	index_charge = _mm512_mullo_epi64(index_disc_price, _mm512_add_epi64(_mm512_set1_epi64(100), tax));
    sums[1]->sum_charge += _mm512_reduce_add_epi64(index_charge);

    index_price = _mm512_maskz_compress_epi64(reverse_table_values[2], price);
    sums[2]->sum_base_price += _mm512_reduce_add_epi64(index_price);
	index_discount = _mm512_maskz_compress_epi64(reverse_table_values[2], discount);
    sums[2]->sum_discount += _mm512_reduce_add_epi64(index_discount);
	index_disc_price = _mm512_mullo_epi64(index_price, _mm512_sub_epi64(_mm512_set1_epi64(100), index_discount));
    sums[2]->sum_disc_price += _mm512_reduce_add_epi64(index_disc_price);
	index_charge = _mm512_mullo_epi64(index_disc_price, _mm512_add_epi64(_mm512_set1_epi64(100), tax));
    sums[2]->sum_charge += _mm512_reduce_add_epi64(index_charge);

	index_price = _mm512_maskz_compress_epi64(reverse_table_values[3], price);
    sums[3]->sum_base_price += _mm512_reduce_add_epi64(index_price);
    index_discount = _mm512_maskz_compress_epi64(reverse_table_values[3], discount);
    sums[3]->sum_discount += _mm512_reduce_add_epi64(index_discount);
	index_disc_price = _mm512_mullo_epi64(index_price, _mm512_sub_epi64(_mm512_set1_epi64(100), index_discount));
    sums[3]->sum_disc_price += _mm512_reduce_add_epi64(index_disc_price);
    index_charge = _mm512_mullo_epi64(index_disc_price, _mm512_add_epi64(_mm512_set1_epi64(100), tax));
    sums[3]->sum_charge += _mm512_reduce_add_epi64(index_charge);
#else
#endif
}

bool q1_using_bitmap = 1;
bool q1_using_idlist = 0;
bool q1_using_segment = 0;
void BMTableScan::BMTPCH_Q1(ExecutionContext &context, const PhysicalTableScan &op)
{
	auto s0 = std::chrono::high_resolution_clock::now();

    auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	
	// 1998-09-02
	int right_days = 10471;

	std::map<std::pair<char,char>, q1_data> q1_ans;

	if (q1_using_segment) {
		long long time1 = 0;
		long long timeb = 0;
		long long timeids = 0;
		auto rabit_linestatus = dynamic_cast<rabit::Rabit *>(context.client.bitmap_linestatus);
		auto rabit_returnflag = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
		auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);

		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(4));
		storage_column_ids.push_back(StorageIndex(5));
		storage_column_ids.push_back(StorageIndex(6));
		storage_column_ids.push_back(StorageIndex(7));
		storage_column_ids.push_back(StorageIndex(10));
		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);

		std::map<std::pair<char, char>, std::pair<vector<row_t>, int64_t> > ids_map;
		std::map<std::pair<char, char>, std::pair<vector<uint32_t>*, uint8_t *> > rabit_res_map;

		std::map<int, char> returnflag_map;
		std::map<int, char> linestatus_map;
		returnflag_map[0] = 'N';
		returnflag_map[1] = 'R';
		returnflag_map[2] = 'A';
		linestatus_map[0] = 'O';
		linestatus_map[1] = 'F';

		// auto stb = std::chrono::high_resolution_clock::now();
		
		SegBtv* shipdate_res = rabit_shipdate->range_res(0, 0, right_days + 1);
		for(int i = 0; i < rabit_linestatus->config->g_cardinality; i++) {
    		for(int r = 0; r < rabit_returnflag->config->g_cardinality; r++) {

				SegBtv *btv_shipdate = new SegBtv(*shipdate_res);
				btv_shipdate->deepCopy(*shipdate_res);
				btv_shipdate->decompress();

				SegBtv *btv_linestatus = new SegBtv(*rabit_linestatus->Btvs[i]->seg_btv);
				btv_linestatus->deepCopy(*rabit_linestatus->Btvs[i]->seg_btv);
				btv_linestatus->decompress();

				SegBtv *btv_returnflag = new SegBtv(*rabit_returnflag->Btvs[r]->seg_btv);
				btv_returnflag->deepCopy(*rabit_returnflag->Btvs[r]->seg_btv);
				btv_returnflag->decompress();
				
				auto &ttt_res = *btv_shipdate;
				ttt_res._and_in_thread(btv_linestatus, 0, ttt_res.seg_table.size());
				ttt_res._and_in_thread(btv_returnflag, 0, ttt_res.seg_table.size());

				uint64_t count = 0;
				for (const auto & [id_t, seg_t] : ttt_res.seg_table)
        			count += seg_t->btv->count();

				if(!count) continue;
			
                vector<uint32_t> *btv_res = reduce_leadingbits_seg(ttt_res);

				rabit_res_map[{returnflag_map[r], linestatus_map[i]}].first = btv_res;
				rabit_res_map[{returnflag_map[r], linestatus_map[i]}].second = (uint8_t *)&((*btv_res)[0]);
				q1_ans[{returnflag_map[r], linestatus_map[i]}].count_order = count;
				
			}
		}
		// auto etb = std::chrono::high_resolution_clock::now();
		// timeb += std::chrono::duration_cast<std::chrono::nanoseconds>(etb - stb).count();

		int64_t cursor = 0;
		int64_t offset = 0;
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
			if(result.size() == 0)
				break;
			
			offset = cursor;
			cursor += result.size();



			auto &quantity = result.data[0];
			auto &extendedprice = result.data[1];
			auto &discount = result.data[2];
			auto &tax = result.data[3];
			auto &days = result.data[4];

			auto quantity_data = FlatVector::GetData<int64_t>(quantity);
			auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
			auto discount_data = FlatVector::GetData<int64_t>(discount);
			auto tax_data = FlatVector::GetData<int64_t>(tax);
			auto days_data = FlatVector::GetData<int32_t>(days);


			// auto st1 = std::chrono::high_resolution_clock::now();

			for(auto &ids_it : rabit_res_map) {
				auto &btv_it = ids_it.second.second;
				auto &ans_it = q1_ans[ids_it.first];
				uint16_t base = 0;
				while(base + 7 < result.size() ) {
					q1_exe_aggregation(quantity_data, extendedprice_data, discount_data, tax_data,\
										base, reverse_table[*btv_it], q1_ans[ids_it.first]);
					btv_it++;
					base += 8;
				}
				if(base < result.size()) {
					uint8_t bits = *btv_it;
					while(base < result.size()) {
						if( bits & 0x80 ) {
							ans_it.sum_qty += quantity_data[base];
							ans_it.sum_discount += discount_data[base];
							ans_it.sum_base_price += extendedprice_data[base];
							ans_it.sum_disc_price += extendedprice_data[base]*(100 - discount_data[base]);
							ans_it.sum_charge += extendedprice_data[base]*(100 - discount_data[base]) *(100 + tax_data[base]);
						}
						base++;
						bits <<= 1;
					}
				}
			}
			
			// auto et1 = std::chrono::high_resolution_clock::now();
			// time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
		
		}
		// std::cout << "bitmap time : "<< timeb/1000000 << "ms" << std::endl;
		// std::cout << "ids time : "<< timeids/1000000 << "ms" << std::endl;
		// std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;

		 auto s1 = std::chrono::high_resolution_clock::now();

		for(auto &it : q1_ans) {
			std::cout << it.first.first << it.first.second << " : " << it.second << std::endl;
		}

		std::cout << "q1 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;

		return;

	}
	else {
		if(!q1_using_bitmap) {
			long long time1 = 0;
			auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
			TableScanState lineitem_scan_state;
			TableScanGlobalSourceState gs(context.client, op);
			vector<StorageIndex> storage_column_ids;
			storage_column_ids.push_back(StorageIndex(8));
			storage_column_ids.push_back(StorageIndex(9));
			storage_column_ids.push_back(StorageIndex(4));
			storage_column_ids.push_back(StorageIndex(5));
			storage_column_ids.push_back(StorageIndex(6));
			storage_column_ids.push_back(StorageIndex(7));
			storage_column_ids.push_back(StorageIndex(10));
			lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
			vector<LogicalType> types;
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[8]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[9]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);

			while(true) {
				DataChunk result;
				result.Initialize(context.client, types);
				lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
				if(result.size() == 0)
					break;

				auto &returnflag = result.data[0];
				auto &linestatus = result.data[1];
				auto &quantity = result.data[2];
				auto &extendedprice = result.data[3];
				auto &discount = result.data[4];
				auto &tax = result.data[5];
				auto &days = result.data[6];

				auto quantity_data = FlatVector::GetData<int64_t>(quantity);
				auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
				auto discount_data = FlatVector::GetData<int64_t>(discount);
				auto tax_data = FlatVector::GetData<int64_t>(tax);
				auto days_data = FlatVector::GetData<int32_t>(days);

				if(result.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
					auto &returnflag_sel_vector = DictionaryVector::SelVector(result.data[0]);
					auto &returnflag_child = DictionaryVector::Child(result.data[0]);
					auto &linestatus_sel_vector = DictionaryVector::SelVector(result.data[1]);
					auto &linestatus_child = DictionaryVector::Child(result.data[1]);
					
					auto st1 = std::chrono::high_resolution_clock::now();
					
					for(int i = 0; i < result.size(); i++) {

						if(days_data[i] > right_days)
							continue;

						auto &it = q1_ans[{reinterpret_cast<string_t *>(returnflag_child.GetData())[returnflag_sel_vector.get_index(i)].GetData()[0],\
								reinterpret_cast<string_t *>(linestatus_child.GetData())[linestatus_sel_vector.get_index(i)].GetData()[0]}];

						it.sum_qty += quantity_data[i];
						it.sum_discount += discount_data[i];
						it.sum_base_price += extendedprice_data[i];
						it.sum_disc_price += extendedprice_data[i]*(100 - discount_data[i]);
						it.sum_charge += extendedprice_data[i]*(100 - discount_data[i]) *(100 + tax_data[i]);
						it.count_order += 1;
					}
					
					auto et1 = std::chrono::high_resolution_clock::now();
					time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
				}
				else {
					auto st1 = std::chrono::high_resolution_clock::now();
					
					for(int i = 0; i < result.size(); i++) {

						if(days_data[i] > right_days)
							continue;

						auto &it = q1_ans[{reinterpret_cast<string_t *>(returnflag.GetData())[i].GetData()[0], \
							reinterpret_cast<string_t *>(linestatus.GetData())[i].GetData()[0]}];

						it.sum_qty += quantity_data[i];
						it.sum_discount += discount_data[i];
						it.sum_base_price += extendedprice_data[i];
						it.sum_disc_price += extendedprice_data[i]*(100 - discount_data[i]);
						it.sum_charge += extendedprice_data[i]*(100 - discount_data[i]) *(100 + tax_data[i]);
						it.count_order += 1;
					}
					
					auto et1 = std::chrono::high_resolution_clock::now();
					time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
				}
			}
			std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;
		}
		else {
			if(q1_using_idlist) {
				long long time1 = 0;
				long long timeb = 0;
				long long timeids = 0;
				auto rabit_linestatus = dynamic_cast<rabit::Rabit *>(context.client.bitmap_linestatus);
				auto rabit_returnflag = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
				auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
				TableScanState lineitem_scan_state;
				TableScanGlobalSourceState gs(context.client, op);
				vector<StorageIndex> storage_column_ids;
				storage_column_ids.push_back(StorageIndex(4));
				storage_column_ids.push_back(StorageIndex(5));
				storage_column_ids.push_back(StorageIndex(6));
				storage_column_ids.push_back(StorageIndex(7));
				storage_column_ids.push_back(StorageIndex(10));
				lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
				vector<LogicalType> types;
				types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
				types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
				types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
				types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);
				types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);

				std::map<std::pair<char, char>, std::pair<vector<row_t>, int64_t> > ids_map;
				std::map<int, char> returnflag_map;
				std::map<int, char> linestatus_map;
				returnflag_map[0] = 'N';
				returnflag_map[1] = 'R';
				returnflag_map[2] = 'A';
				linestatus_map[0] = 'O';
				linestatus_map[1] = 'F';

				auto stb = std::chrono::high_resolution_clock::now();
				for(int i = 0; i < rabit_linestatus->config->g_cardinality; i++) {
					for(int r = 0; r < rabit_returnflag->config->g_cardinality; r++) {
						ibis::bitvector btv_res;
						btv_res.copy(*rabit_returnflag->Btvs[r]->btv);
						btv_res &= *rabit_linestatus->Btvs[i]->btv;

						if(!btv_res.count())
							continue;

						auto &ids = ids_map[{returnflag_map[r], linestatus_map[i]}].first;

						auto stids = std::chrono::high_resolution_clock::now();

						ids.resize(btv_res.count() + 64);

						auto element_ptr = &ids[0];

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

						ids.resize(btv_res.count());


						auto etids = std::chrono::high_resolution_clock::now();


						timeids += std::chrono::duration_cast<std::chrono::nanoseconds>(etids - stids).count();

					}
				}
				auto etb = std::chrono::high_resolution_clock::now();
				timeb += std::chrono::duration_cast<std::chrono::nanoseconds>(etb - stb).count();


				int64_t cursor = 0;
				int64_t offset = 0;
				while(true) {
					DataChunk result;
					result.Initialize(context.client, types);
					// lineitem_scan_state.table_state.Scan(lineitem_transaction, result);
					lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
					if(result.size() == 0)
						break;
					
					offset = cursor;
					cursor += result.size();

					auto &quantity = result.data[0];
					auto &extendedprice = result.data[1];
					auto &discount = result.data[2];
					auto &tax = result.data[3];
					auto &days = result.data[4];

					auto quantity_data = FlatVector::GetData<int64_t>(quantity);
					auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
					auto discount_data = FlatVector::GetData<int64_t>(discount);
					auto tax_data = FlatVector::GetData<int64_t>(tax);
					auto days_data = FlatVector::GetData<int32_t>(days);


					auto st1 = std::chrono::high_resolution_clock::now();

					for(auto &ids_it : ids_map) {
						auto &it = q1_ans[ids_it.first];
						auto &ids = ids_it.second.first;
						auto &c_row = ids_it.second.second;
						while(c_row < ids.size() && ids[c_row] < cursor) {
							uint64_t i = ids[c_row++] - offset;

							if(days_data[i] > right_days)
								continue;

							it.sum_qty += quantity_data[i];
							it.sum_discount += discount_data[i];
							it.sum_base_price += extendedprice_data[i];
							it.sum_disc_price += extendedprice_data[i]*(100 - discount_data[i]);
							it.sum_charge += extendedprice_data[i]*(100 - discount_data[i]) *(100 + tax_data[i]);
							it.count_order += 1;
						}
					}
					
					auto et1 = std::chrono::high_resolution_clock::now();
					time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
				
				}
				std::cout << "bitmap time : "<< timeb/1000000 << "ms" << std::endl;
				std::cout << "ids time : "<< timeids/1000000 << "ms" << std::endl;
				std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;
			}
			else
			{
				long long time1 = 0;
				long long timeb = 0;
				long long timer = 0;
				auto rabit_linestatus = dynamic_cast<rabit::Rabit *>(context.client.bitmap_linestatus);
				auto rabit_returnflag = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
				auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);

				auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
				TableScanState lineitem_scan_state;
				TableScanGlobalSourceState gs(context.client, op);
				vector<StorageIndex> storage_column_ids;
				storage_column_ids.push_back(StorageIndex(4));
				storage_column_ids.push_back(StorageIndex(5));
				storage_column_ids.push_back(StorageIndex(6));
				storage_column_ids.push_back(StorageIndex(7));
				lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
				vector<LogicalType> types;
				types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
				types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
				types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
				types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);

				std::map<std::pair<char, char>, std::pair<vector<row_t>, int64_t> > ids_map;
				std::map<std::pair<char, char>, std::pair<vector<uint32_t>*, uint8_t *> > rabit_res_map;
				std::map<int, char> returnflag_map;
				std::map<int, char> linestatus_map;
				returnflag_map[0] = 'N';
				returnflag_map[1] = 'R';
				returnflag_map[2] = 'A';
				linestatus_map[0] = 'O';
				linestatus_map[1] = 'F';

				auto stb = std::chrono::high_resolution_clock::now();

				ibis::bitvector shipdate_res;
				shipdate_res.copy(*rabit_shipdate->Btvs[rabit_shipdate->config->g_cardinality - 1]->btv);
				shipdate_res.decompress();
				// compute l_shipdate > CAST('1998-09-02' AS date)
				for(int i = rabit_shipdate->config->g_cardinality - 2; i > right_days; i--)
					shipdate_res |= *rabit_shipdate->Btvs[i]->btv;
					
				// invert the result (get l_shipdate <= CAST('1998-09-02' AS date))
				flip_bitvector(&shipdate_res);

				for(int i = 0; i < rabit_linestatus->config->g_cardinality; i++) {
					for(int r = 0; r < rabit_returnflag->config->g_cardinality; r++) {
						if (i == 0 && r > 0) {
							continue;
						}
						ibis::bitvector ttt_res, g_res;
						ttt_res.copy(*rabit_linestatus->Btvs[i]->btv);
						g_res.copy(*rabit_returnflag->Btvs[r]->btv);
						ttt_res.decompress();
						g_res.decompress();

						ttt_res &= g_res;
						ttt_res &= shipdate_res;

						uint64_t count = ttt_res.count();

						assert(count > 0);

						vector<uint32_t> *btv_res = reduce_leadingbits(ttt_res);

						rabit_res_map[{returnflag_map[r], linestatus_map[i]}].first = btv_res;
						rabit_res_map[{returnflag_map[r], linestatus_map[i]}].second = (uint8_t *)&((*btv_res)[0]);
						q1_ans[{returnflag_map[r], linestatus_map[i]}].count_order = count;

					}
				}

				auto etb = std::chrono::high_resolution_clock::now();
				timeb += std::chrono::duration_cast<std::chrono::nanoseconds>(etb - stb).count();


				int64_t cursor = 0;
				int64_t offset = 0;
				std::vector<uint8_t> reverse_table_values;
				std::vector<q1_data*> q1_ans_pointers;
				while(true) {
					DataChunk result;
					result.Initialize(context.client, types);
					lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
					if(result.size() == 0)
						break;
					
					offset = cursor;
					cursor += result.size();

					auto &quantity = result.data[0];
					auto &extendedprice = result.data[1];
					auto &discount = result.data[2];
					auto &tax = result.data[3];

					auto quantity_data = FlatVector::GetData<int64_t>(quantity);
					auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
					auto discount_data = FlatVector::GetData<int64_t>(discount);
					auto tax_data = FlatVector::GetData<int64_t>(tax);

					auto st1 = std::chrono::high_resolution_clock::now();
					

					for (auto &ids_it : rabit_res_map) {
						auto &ans_it = q1_ans[ids_it.first];
						q1_ans_pointers.push_back(&q1_ans[ids_it.first]);
					}

					uint16_t base = 0;
					while (base + 7 < result.size()) {
						for (auto &ids_it : rabit_res_map) {
							auto &btv_it = ids_it.second.second;
							reverse_table_values.push_back(reverse_table[*btv_it]);
						}

						q1_exe_aggregation_batch(quantity_data, extendedprice_data, discount_data, tax_data, base, reverse_table_values, q1_ans_pointers);

						for (auto &ids_it : rabit_res_map) {
							auto &btv_it = ids_it.second.second;
							btv_it++;
						}

						base += 8;
						reverse_table_values.clear();
					}

					for (auto &ids_it : rabit_res_map) {
						auto &btv_it = ids_it.second.second;
						auto &ans_it = q1_ans[ids_it.first];
						uint16_t remain_base = base;
						if (remain_base < result.size()) {
							uint8_t bits = *btv_it;
							while (remain_base < result.size()) {
								if (bits & 0x80) {
									ans_it.sum_qty += quantity_data[remain_base];
									ans_it.sum_discount += discount_data[remain_base];
									ans_it.sum_base_price += extendedprice_data[remain_base];
									ans_it.sum_disc_price += extendedprice_data[remain_base] * (100 - discount_data[remain_base]);
									ans_it.sum_charge += extendedprice_data[remain_base] * (100 - discount_data[remain_base]) * (100 + tax_data[remain_base]);
								}
								remain_base++;
								bits <<= 1;
							}
						}
					}

					auto et1 = std::chrono::high_resolution_clock::now();
					time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
				
				}
				std::cout << "bitmap time : "<< timeb/1000000 << "ms" << std::endl;
				std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;
			}
		}
		
		auto s1 = std::chrono::high_resolution_clock::now();

		for(auto &it : q1_ans) {
			std::cout << it.first.first << it.first.second << " : " << it.second << std::endl;
		}

		std::cout << "q1 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;

		return;
	}
	

}

}
