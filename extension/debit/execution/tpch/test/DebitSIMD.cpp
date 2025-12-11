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


struct bmsimd_data {
int64_t sum_price_discount = 0;
int64_t count_order;

friend std::ostream& operator<<( std::ostream &output, const bmsimd_data& D);
};
std::ostream& operator<<( std::ostream &output, const bmsimd_data& D)
{
	output << std::fixed << std::setprecision(4) << (double)D.sum_price_discount / 10000;

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

inline void bmsimd_aggregation_64(int64_t *price_ptr, int64_t *discount_ptr, uint16_t base, uint8_t bits, bmsimd_data &sum) 
{
#if defined(__AVX512F__)
	if(!bits)
		return;

	__m512i discount = _mm512_loadu_epi64(discount_ptr + base);
	__m512i price = _mm512_loadu_epi64(price_ptr + base);
	__m512i index_discount = _mm512_maskz_compress_epi64(bits, discount);
	__m512i index_price = _mm512_maskz_compress_epi64(bits, price);
	__m512i price_times_discount = _mm512_mullo_epi64(index_price, index_discount);
	sum.sum_price_discount += _mm512_reduce_add_epi64(price_times_discount);
#else
	if (!bits)
        return;

    for (int i = 0; i < 8; i++) {
        if (bits & (1 << i)) {
            int idx = base + i;
            
            int64_t discount = discount_ptr[idx];
            int64_t price = price_ptr[idx];
            
            int64_t price_discount = price * discount;
            sum.sum_price_discount += price_discount;

        }
    }		
#endif
}

inline void bmsimd_aggregation_32(int32_t *price_ptr, int32_t *discount_ptr, uint16_t base, uint16_t bits, bmsimd_data &sum) 
{
#if defined(__AVX512F__)
	if(!bits)
		return;

	// __m512i discount = _mm512_loadu_epi32(discount_ptr + base);
	// __m512i price = _mm512_loadu_epi32(price_ptr + base);
	// __m512i index_discount = _mm512_maskz_compress_epi32(bits, discount);
	// __m512i index_price = _mm512_maskz_compress_epi32(bits, price);
	// __m512i price_times_discount = _mm512_mullo_epi32(index_price, index_discount);
	// sum.sum_price_discount += _mm512_reduce_add_epi32(price_times_discount);

	 __m512i discount = _mm512_loadu_epi32(discount_ptr + base);
	__m512i price = _mm512_loadu_epi32(price_ptr + base);
	__m512i src = _mm512_setzero_epi32();
	__m512i price_times_discount = _mm512_mask_mullo_epi32(src, bits, discount, price);
	sum.sum_price_discount += _mm512_reduce_add_epi32(price_times_discount);
#else
	if (!bits)
        return;

    for (int i = 0; i < 16; i++) {
        if (bits & (1 << i)) {
            int idx = base + i;
            
            int32_t discount = discount_ptr[idx];
            int32_t price = price_ptr[idx];
            
            int32_t price_discount = price * discount;
            sum.sum_price_discount += price_discount;
        }
    }		
#endif
}

void BMTableScan::Debit_SIMD(ExecutionContext &context, const PhysicalTableScan &op)
{
    auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	
	auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);
	auto rabit_discount = dynamic_cast<rabit::Rabit *>(context.client.bitmap_discount);
	auto rabit_quantity = dynamic_cast<rabit::Rabit *>(context.client.bitmap_quantity);

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

	int c_year1 = 1994;
	int c_year2 = 1994;
	int start_year = 1970;

	int lower_discount = 5;
	int upper_discount = 7;

	int upper_quantity = 23;

	SegBtv *btv_shipdate = rabit_shipdate->range_or_GE(0, c_year1 - start_year, c_year2 - c_year1 + 1);

	SegBtv *btv_discount = rabit_discount->range_res(0, lower_discount, upper_discount - lower_discount + 1);
	SegBtv *btv_quantity = rabit_quantity->range_res(0, 0, upper_quantity + 1);
	
	auto &ttt_res = *btv_shipdate;
	ttt_res._and_in_thread(btv_discount, 0, ttt_res.seg_table.size());
	ttt_res._and_in_thread(btv_quantity, 0, ttt_res.seg_table.size());

	uint64_t count = 0;
	for (const auto & [id_t, seg_t] : ttt_res.seg_table)
		count += seg_t->btv->count();

	assert(count);
	double time_count = 0;
	double scan_count = 0;
	auto s0 = std::chrono::high_resolution_clock::now();
	size_t total_bits = ttt_res.do_cnt();
	vector<uint32_t> *btv_res = new vector<uint32_t>(total_bits / 32 + (total_bits % 32 ? 1 : 0) + 32, 0);

	uint32_t *dst_data = (uint32_t *)&(*btv_res)[0];
	size_t seg_idx = 0;
	size_t seg_cnt = ttt_res.seg_table.size();
	std::vector<uint32_t> remain_words;

	for (const auto& seg_pair : ttt_res.seg_table) {
		ibis::bitvector* seg_btv = seg_pair.second->btv;
		uint32_t *src_data = seg_btv->m_vec.begin();
		uint32_t* src_end = seg_btv->m_vec.end();

		while(remain_words.size() < 32 && src_data < src_end) {
			remain_words.push_back(*src_data);
			src_data++;
		}

		if(remain_words.size() == 32) {
			remain_words.push_back(0);
			reduce_zero(dst_data, remain_words.data());
			dst_data += 31;
			remain_words.clear();
		}

		while(src_data + 32 < src_end) {
			reduce_zero(dst_data, src_data);
			dst_data += 31;
			src_data += 32;
		}

		while(src_data < src_end) {
			remain_words.push_back(*src_data);
			src_data++;
		}
	}

	int need_bits = 31;
	uint32_t *src_data2 = remain_words.data();
	uint32_t *src_end2 = remain_words.data() + remain_words.size();
	while(src_data2 + 1 < src_end2) {
		dst_data[0] = ((src_data2[0] << (32 - need_bits)) | (src_data2[1] >> (need_bits - 1)));
		dst_data[0] = htonl(dst_data[0]);
		need_bits--;
		src_data2++;
		dst_data++;
	}

	auto &last_seg = ttt_res.seg_table.rbegin()->second->btv;
	dst_data[0] = src_data2[0] << (32 - need_bits);
	uint32_t last_word = last_seg->active.val << (32 - last_seg->active.nbits);
	if(last_seg->active.nbits + need_bits > 32) {
		dst_data[0] |= (last_word >> need_bits);
		dst_data[0] = htonl(dst_data[0]);
		last_word <<= (32 - need_bits);
		dst_data[1] = last_word;
		dst_data[1] = htonl(dst_data[1]);
	} else {
		dst_data[0] |= (last_word >> need_bits);
		dst_data[0] = htonl(dst_data[0]);
	}
	auto s1 = std::chrono::high_resolution_clock::now();
	time_count += std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count();
	uint16_t* btv_res_ptr = (uint16_t *)&((*btv_res)[0]);
	bmsimd_data agg_ans;
	agg_ans.count_order = count;	

	int64_t cursor = 0;
	int64_t offset = 0;
	while(true) {
		auto s2 = std::chrono::high_resolution_clock::now();

		DataChunk result;
		result.Initialize(context.client, types);
		lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);

		if(result.size() == 0) break;
		
		offset = cursor;
		cursor += result.size();

		auto &extendedprice = result.data[0];
		auto &discount = result.data[1];

		auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
		auto discount_data = FlatVector::GetData<int64_t>(discount);

		auto s3 = std::chrono::high_resolution_clock::now();

		int32_t* extendedprice_data_32 = new int32_t[result.size()];
		int32_t* discount_data_32 = new int32_t[result.size()];
		for (int i = 0; i < result.size(); ++i) {
			extendedprice_data_32[i] = static_cast<int32_t>(extendedprice_data[i]);
			discount_data_32[i] = static_cast<int32_t>(discount_data[i]);
		}
		auto s4 = std::chrono::high_resolution_clock::now();
		uint16_t base = 0;
		while(base + 15 < result.size()) {
			uint16_t val = *btv_res_ptr;
			uint8_t low = val & 0xFF;
			uint8_t high = (val >> 8) & 0xFF;
			uint16_t reversed = (reverse_table[high] << 8) | reverse_table[low];
			bmsimd_aggregation_32(extendedprice_data_32, discount_data_32, base, reversed, agg_ans);
			btv_res_ptr++;
			base += 16;
		}
		if(base < result.size()) {
			uint16_t bits = *btv_res_ptr;
			while(base < result.size()) {
				bool has_bit = bits & 0x8000;
				agg_ans.sum_price_discount += has_bit * extendedprice_data_32[base] * discount_data_32[base];
				base++;
				bits <<= 1;
			}
		}
		auto s5 = std::chrono::high_resolution_clock::now();
		scan_count += std::chrono::duration_cast<std::chrono::nanoseconds>(s3 - s2).count();
		time_count += std::chrono::duration_cast<std::chrono::nanoseconds>(s3 - s2).count() + std::chrono::duration_cast<std::chrono::nanoseconds>(s5 - s4).count();
		delete[] extendedprice_data_32;
		delete[] discount_data_32;
	}

	// auto s1 = std::chrono::high_resolution_clock::now();

	std::cout <<"revenue:"<< agg_ans << std::endl;
	std::cout << "scan time : "<< scan_count/1000000 << "ms" << std::endl;
	std::cout << "whole time : "<< time_count/1000000 << "ms" << std::endl;

	// std::cout << "q6 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;

	return;
	
}

}
