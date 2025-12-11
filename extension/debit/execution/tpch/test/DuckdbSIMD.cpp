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
#include <vector>
#include <cstdint>
#include <cstring>
#include <bitset>

#if defined(__AVX512F__) && defined(__AVX512VL__)
  #include <immintrin.h>
  #define HAVE_AVX512 1
#else
  #define HAVE_AVX512 0
#endif

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


struct q_data {
int64_t sum_price_discount = 0;
int64_t count_order;

friend std::ostream& operator<<( std::ostream &output, const q_data& D);
};
std::ostream& operator<<( std::ostream &output, const q_data& D)
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

inline void duckdbsimd_aggregation(int64_t *price_ptr, int64_t *discount_ptr, uint16_t base, uint8_t bits, q_data &sum) 
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

std::vector<uint32_t> convertToBitvector_scalar(const uint32_t* input, size_t input_size, size_t bitvectorSizeWords) {
    std::vector<uint32_t> bv(bitvectorSizeWords, 0u);
    if (!input || input_size == 0 || bitvectorSizeWords == 0) return bv;

    const uint64_t bit_limit = bitvectorSizeWords * 32ull;
    uint32_t cur_idx = UINT32_MAX;
    uint32_t acc = 0u;

    for (size_t i = 0; i < input_size; ++i) {
        uint32_t x = input[i];
        if (x >= bit_limit) break; 
        uint32_t idx = x >> 5; 
        uint32_t mask = 1u << (x & 31);
        if (idx != cur_idx) {
            if (cur_idx != UINT32_MAX) bv[cur_idx] |= acc;
            cur_idx = idx;
            acc = mask;
        } else {
            acc |= mask;
        }
    }
    if (cur_idx != UINT32_MAX) bv[cur_idx] |= acc;
    return bv;
}

#if HAVE_AVX512

std::vector<uint32_t> convertToBitvector_avx512(const uint32_t* input, size_t input_size, size_t bitvectorSizeWords) {
    std::vector<uint32_t> bv(bitvectorSizeWords, 0u);
    if (!input || input_size == 0 || bitvectorSizeWords == 0) return bv;

    const uint64_t bit_limit = bitvectorSizeWords * 32ull;
    size_t i = 0;
    uint32_t cur_idx = UINT32_MAX;
    uint32_t acc = 0u;

    for (; i + 16 <= input_size; i += 16) {
        __m512i v = _mm512_loadu_si512((const void*)(input + i));
        __m512i limit = _mm512_set1_epi32((int)bit_limit);
        __mmask16 in_range = _mm512_cmplt_epu32_mask(v, limit);
        if (in_range == 0) break;

        __m512i idx = _mm512_srli_epi32(v, 5);
        __m512i off = _mm512_and_si512(v, _mm512_set1_epi32(31));
        __m512i masks = _mm512_sllv_epi32(_mm512_set1_epi32(1), off);

        alignas(64) uint32_t idx_arr[16];
        alignas(64) uint32_t msk_arr[16];
        _mm512_store_si512((void*)idx_arr, idx);
        _mm512_store_si512((void*)msk_arr, masks);

        for (int lane = 0; lane < 16; ++lane) {
            if (((in_range >> lane) & 1) == 0) msk_arr[lane] = 0u;
        }
        // 合并同idx
        for (int a = 0; a < 16; ++a) {
            if (msk_arr[a] == 0u) continue;
            for (int b = a + 1; b < 16 && idx_arr[b] == idx_arr[a]; ++b) {
                msk_arr[a] |= msk_arr[b];
                msk_arr[b] = 0u;
            }
        }
        for (int lane = 0; lane < 16; ++lane) {
            uint32_t m = msk_arr[lane];
            if (!m) continue;
            uint32_t widx = idx_arr[lane];
            if (cur_idx == UINT32_MAX) {
                cur_idx = widx;
                acc = m;
            } else if (widx == cur_idx) {
                acc |= m;
            } else {
                if (cur_idx < bv.size()) bv[cur_idx] |= acc;
                cur_idx = widx;
                acc = m;
            }
        }
    }

    for (; i < input_size; ++i) {
        uint32_t x = input[i];
        if (x >= bit_limit) break;
        uint32_t widx = x >> 5;
        uint32_t m = 1u << (x & 31);
        if (cur_idx == UINT32_MAX) {
            cur_idx = widx;
            acc = m;
        } else if (widx == cur_idx) {
            acc |= m;
        } else {
            if (cur_idx < bv.size()) bv[cur_idx] |= acc;
            cur_idx = widx;
            acc = m;
        }
    }
    if (cur_idx != UINT32_MAX && cur_idx < bv.size()) bv[cur_idx] |= acc;
    return bv;
}
#endif

std::vector<uint32_t> convertToBitvector(const uint32_t* input, size_t input_size, size_t bitvectorSizeWords) {
#if HAVE_AVX512
    std::cout << "AVX512 version:"<<std::endl;
    return convertToBitvector_avx512(input, input_size, bitvectorSizeWords);
#else
    std::cout << " Scalar version:"<<std::endl;
    return convertToBitvector_scalar(input, input_size, bitvectorSizeWords);
#endif
}



void BMTableScan::DuckDB_SIMD(ExecutionContext &context, const PhysicalTableScan &op, std::vector<uint32_t>* idlist)
{
	auto s0 = std::chrono::high_resolution_clock::now();

    auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	
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

    size_t bitvectorSizeWords = 59986023 / 32 + (59986023 % 32 ? 1 : 0) + 32;
    auto s2 = std::chrono::high_resolution_clock::now();
    std::vector<uint32_t> btv_res = convertToBitvector(idlist->data(), idlist->size(), bitvectorSizeWords);
    auto s3 = std::chrono::high_resolution_clock::now();
    uint8_t* btv_res_ptr = (uint8_t *)&((btv_res)[0]);
    q_data agg_ans;
    double scan_count = 0;
    int64_t cursor = 0;
    int64_t offset = 0;
    while(true) {
        auto t0 = std::chrono::high_resolution_clock::now();
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
        auto t1 = std::chrono::high_resolution_clock::now();
        scan_count += std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

        uint16_t base = 0;
        while(base + 7 < result.size() ) {
            duckdbsimd_aggregation(extendedprice_data, discount_data, base, *btv_res_ptr, agg_ans);
            btv_res_ptr++;
            base += 8;
        }
        if(base < result.size()) {
            uint8_t bits = *btv_res_ptr;
            int bit_idx = 0;
            while(base < result.size()) {
                if(bits & (1 << bit_idx)) {
                    agg_ans.sum_price_discount += extendedprice_data[base] * discount_data[base];
                }
                base++;
                bit_idx++;
            }
        }
        
    }
    
    auto s1 = std::chrono::high_resolution_clock::now();

    std::cout <<"revenue:"<< agg_ans << std::endl;

    std::cout << "q6 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;
    std::cout << "scan time : "<< scan_count/1000000 << "ms" << std::endl;
    std::cout << "idlist to btv time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s3 - s2).count() << "ms" << std::endl;
    return;	
}

}